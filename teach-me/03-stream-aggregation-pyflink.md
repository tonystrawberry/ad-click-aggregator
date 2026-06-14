# Chapter 3: Stream Aggregation with PyFlink

The click processor dropped individual clicks onto Kinesis. Now we turn that
firehose of single events into the thing advertisers actually query: a count of
clicks per campaign per minute, updated in near-real-time. That's the job of
[stream/flink-aggregator/main.py](stream/flink-aggregator/main.py) — a PyFlink app
running on Amazon Managed Service for Apache Flink. This chapter is about three
ideas: **event-time windows**, **watermarks**, and **idempotent sinking into
Redshift**.

## Why PyFlink and not Java

Flink is a JVM engine; the obvious choice is Java. This repo started there and
[migrated to PyFlink](specs/001-ad-click-aggregator/research.md) on purpose. The
key realization:

> Our aggregation is pure Table API / SQL. Flink **plans and executes SQL in the
> JVM regardless of which language submitted it.** Python only pays a penalty for
> Python *UDFs* — user functions that run in a separate Python process. We have
> none.

So PyFlink costs us nothing at runtime here, and it deletes the entire Java/Maven
toolchain — the codebase becomes Ruby + Python only.

| | Java Flink | PyFlink (chosen) |
|--|------------|------------------|
| Windowing perf | JVM | JVM (identical — it's the same SQL planner) |
| Toolchain | Maven, `pom.xml`, a JDK | `pip install apache-flink` |
| Python-UDF penalty | n/a | none (we use zero UDFs) |
| Custom sink ergonomics | easy (`RichSinkFunction`) | a bit more constrained (see MERGE below) |

The one place Java would have been easier is the custom Redshift sink — which is
why the Python version solves it with SQL instead of code.

## The source: a Kinesis table with a watermark

Everything starts with a `CREATE TABLE` over the Kinesis stream, built in
`source_ddl`:

```python
def source_ddl(cfg):
    return f"""
        CREATE TABLE click_events (
          impression_id STRING, ad_id STRING, campaign_id STRING,
          advertiser_id STRING, click_ts STRING,
          click_time AS TO_TIMESTAMP(REPLACE(REPLACE(click_ts, 'T', ' '), 'Z', '')),
          WATERMARK FOR click_time AS click_time - INTERVAL '30' SECOND
        ) WITH (
          'connector' = 'kinesis',
          'stream' = '{cfg.get("stream.name")}',
          'scan.stream.initpos' = '{cfg.get("scan.initpos", "LATEST")}',
          'format' = 'json', 'json.ignore-parse-errors' = 'true'
        )
    """
```

Two columns matter. `click_time` is a *computed* column: it parses the
`click_ts` string the click processor wrote in [Chapter 2](02-click-capture-path.md)
into a real timestamp. The `WATERMARK` line is the heart of the whole job, so let's
slow down.

## Event-time vs processing-time, and why you must care

There are two clocks in a streaming system:

- **Processing time**: the wall clock when Flink sees the event.
- **Event time**: when the click actually happened (`click_time`).

A click that happened at 14:07:50 might arrive at Flink at 14:08:05 — network lag,
a Kinesis retry, a Lambda cold start. If you bucket by *processing* time, that
click lands in the 14:08 minute and your numbers are wrong. The reference (and this
job) bucket by **event time**, so a click always lands in the minute it happened,
regardless of when it shows up.

But event time creates a problem: how does Flink know it has seen *all* the clicks
for the 14:07 minute, so it can finalize that count? It can't wait forever. That's
what the **watermark** answers: `click_time - INTERVAL '30' SECOND` tells Flink "I
promise events are at most 30s late; once I've seen event-time 14:08:30, the 14:07
window is closed." Anything later than that is dropped by the stream path — and
this is precisely the drift that [reconciliation](05-reconciliation.md) exists to
correct.

## The window: one line of SQL

```python
windowed = t_env.sql_query("""
    SELECT campaign_id,
           window_start AS minute_bucket,
           COUNT(*)     AS click_count
    FROM TABLE(TUMBLE(TABLE click_events, DESCRIPTOR(click_time), INTERVAL '1' MINUTE))
    GROUP BY campaign_id, window_start, window_end
""")
```

`TUMBLE` is a *tumbling* (non-overlapping, fixed-size) window. One minute, keyed by
`campaign_id`. Here's a concrete run — five clicks, one arriving out of order, with
the 30s watermark:

| arrival | event `click_time` | campaign | window | note |
|---------|--------------------|----------|--------|------|
| 1 | 14:07:10 | camp_1 | 14:07 | |
| 2 | 14:07:50 | camp_1 | 14:07 | |
| 3 | 14:07:05 | camp_1 | 14:07 | arrived 3rd, **event-time earlier** — still 14:07 |
| 4 | 14:08:01 | camp_1 | 14:08 | passing 14:08:30 watermark later closes 14:07 |
| 5 | 14:07:30 | camp_2 | 14:07 | different campaign, own bucket |

Emitted rows when the windows close:

```
(camp_1, 14:07, 3)   <- clicks 1,2,3 (the out-of-order one counted correctly)
(camp_1, 14:08, 1)   <- click 4
(camp_2, 14:07, 1)   <- click 5
```

This exact scenario is the smoke test in
[stream/flink-aggregator/tests/test_windowing.py](stream/flink-aggregator/tests/test_windowing.py),
which asserts `sorted(per_campaign["camp_1"]) == [1, 3]`. That's the load-bearing
behaviour: out-of-order clicks land in the right minute.

## The sink: a single-statement Redshift MERGE

Each closed window emits one row per `(campaign, minute)`. We need to get it into
Redshift. The catch: **Redshift has no `INSERT … ON CONFLICT`**, so Flink's
built-in JDBC upsert (which assumes Postgres-style conflict handling) doesn't work.
The fix is a single `MERGE` statement:

```python
MERGE_SQL = """
MERGE INTO click_aggregates t
USING (SELECT CAST(? AS VARCHAR(64)) AS campaign_id,
              CAST(? AS TIMESTAMP)   AS minute_bucket,
              CAST(? AS BIGINT)      AS click_count) s
ON t.campaign_id = s.campaign_id AND t.minute_bucket = s.minute_bucket
WHEN MATCHED THEN UPDATE
  SET click_count = s.click_count, source = 'stream', updated_at = GETDATE()
WHEN NOT MATCHED THEN INSERT (...) VALUES (...)
"""
```

Notice the semantics: `SET click_count = s.click_count` — a **replace**, not an
add. Because a tumbling window emits each `(campaign, minute)` exactly once with its
final count, replace is correct *and* idempotent: if Flink restarts from a
checkpoint and re-emits a window, it re-sets the same value rather than
double-counting. The earlier Java version accumulated (`+=`), which was subtler to
get right on replay. Migrating to SQL made the cleaner semantics the natural one.

It's wired up with `JdbcSink` over the converted stream:

```python
ds = t_env.to_data_stream(windowed)
ds.add_sink(JdbcSink.sink(MERGE_SQL, row_type, conn_opts, exec_opts)).name("redshift-merge")
env.execute("ad-click-minute-aggregator")
```

Every row written carries `source = 'stream'`. Keep that column in mind — it is the
seam between this chapter and [Chapter 5](05-reconciliation.md), where the batch job
overwrites these same rows with `source = 'batch'`.

## Config and checkpoints

`main()` reads runtime properties Managed Flink injects as a JSON file
(`get_application_properties` → the `FlinkAppProperties` group), so the stream name,
region, and Redshift JDBC URL/creds are not hardcoded. And `env.enable_checkpointing(60_000)`
is what makes "resume without loss" true — on failure the job restarts from the last
checkpoint, and the replace-MERGE keeps that safe.

## Try it out

Try each step yourself first — expand the solution only when stuck.

Setup: the windowing test runs a local Flink MiniCluster bundled inside the
`apache-flink` wheel. It needs **Python 3.11** (apache-flink doesn't support 3.12+)
and a JRE on PATH.

```bash
cd stream/flink-aggregator
python3.11 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt pytest
```

1. Run the windowing test and confirm the out-of-order assertion holds.

   <details>
   <summary><b>Solution</b></summary>

   ```bash
   cd stream/flink-aggregator && python -m pytest -q
   ```
   `1 passed`. The test feeds the five-event scenario from the table above and
   asserts `camp_1` produced windows of `[1, 3]` — the out-of-order click landed in
   14:07.
   </details>

2. Tighten the watermark to `INTERVAL '5' SECOND` in the *test's* schema and predict
   whether the out-of-order click (15s late relative to the latest event) still
   counts.

   <details>
   <summary><b>Solution</b></summary>

   In `tests/test_windowing.py` change the watermark to 5 seconds. The 14:07:05
   event arrives after 14:07:50, which is already 45s ahead — well past a 5s
   allowance — so depending on emit timing it can be dropped, and `camp_1` may
   become `[1, 2]`. This demonstrates exactly what the watermark trades off:
   tighter = fresher but drops more late data (which reconciliation then recovers).
   </details>

3. Add a `day` rollup query alongside the minute window (read-only experiment).

   <details>
   <summary><b>Solution</b></summary>

   You don't — and that's the lesson. Day rollups are computed at *query time* in
   [Chapter 4](04-query-service.md) via `date_trunc`, not pre-aggregated here. Flink
   only ever produces minute buckets; adding a second window would duplicate counts
   into Redshift. Confirm by grepping: `grep -n "INTERVAL" main.py` shows a single
   1-minute window.
   </details>

4. Explain why `to_data_stream(windowed)` is needed before `add_sink` instead of an
   `INSERT INTO` sink table.

   <details>
   <summary><b>Solution</b></summary>

   A SQL JDBC sink table would use Flink's dialect-based upsert, which emits
   Postgres `ON CONFLICT` — invalid on Redshift. Dropping to the DataStream lets us
   attach a `JdbcSink` running our own `MERGE_SQL`. Grep `grep -n "MERGE\|JdbcSink"
   main.py` to see the two pieces that replace the built-in upsert.
   </details>

Next: [Chapter 4](04-query-service.md) is the other half of the read path — the Ruby
query service that turns these minute rows into the zero-filled, any-granularity
answer an advertiser's dashboard renders.
