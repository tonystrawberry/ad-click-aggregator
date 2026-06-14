# stream/flink-aggregator — Flink stream aggregator (PyFlink)

PyFlink job for Amazon Managed Service for Apache Flink (REFERENCE: Kinesis → Flink
near-real-time aggregation). A documented non-Ruby exception (Constitution
Principle III) — no Ruby Flink runtime exists; PyFlink keeps the codebase Ruby +
Python and removes the Java/Maven toolchain.

- `main.py` — Kinesis source (event-time, 30s watermark) → 1-minute tumbling window
  count by `campaign_id` (Table API SQL) → MERGE-upsert into Redshift
  `click_aggregates` with `source='stream'` via a `JdbcSink`.

The windowing is pure Table API/SQL, planned and executed in the JVM, so there is
**no Python-process penalty** (no Python UDFs). Event-time + watermark attribute
late/out-of-order clicks to the correct minute (FR-016).

**Sink semantics**: each closed 1-minute window emits its final count once, so the
Redshift `MERGE` REPLACES the `(campaign, minute)` count — idempotent under
checkpoint replay. Reconciliation later overwrites the period with `source='batch'`.

## Test

```bash
pip install -r requirements.txt      # apache-flink bundles the Flink runtime
python -m pytest -q                   # MiniCluster windowing test (no external jars)
```

## Build / package (deploy)

`make build-flink` zips `main.py` plus the connector + Redshift driver jars under
`lib/` and uploads to the artifacts bucket. Managed Flink runs it as a Python app;
the `kinesis.analytics.flink.run.options` property group points `python` at
`main.py` and supplies the connector jar. Runtime config (stream, region, Redshift
JDBC URL + creds) comes from the `FlinkAppProperties` group, set by Terraform.

> The connector/driver jar bundling is the deploy-time detail to verify against the
> Managed Flink runtime (see `quickstart.md`).
