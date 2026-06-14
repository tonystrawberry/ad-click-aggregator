# Phase 1 Data Model: Ad Click Aggregator

Maps the spec's Key Entities onto concrete stores. Reference-data entities (Advertiser,
Campaign, Ad) live in DynamoDB; the event of record (Click Event) flows through Kinesis to
S3; the read-optimized rollup (Aggregated Metric) lives in Redshift. Impression dedup state
lives in Redis.

---

## Entity overview

| Entity (spec) | Store | Role |
|---------------|-------|------|
| Advertiser | DynamoDB (`ads` GSI) | Owner principal; scopes queries |
| Campaign | DynamoDB (`ads` attrs + GSI) | Query unit (counts grouped by campaign) |
| Ad | DynamoDB `ads` | Hot-path lookup: destination + campaign/advertiser |
| Ad Impression | Redis key | Dedup unit; one count per impression |
| Click Event | Kinesis → S3 (Parquet) | Raw unit of truth for reconciliation |
| Aggregated Metric | Redshift `click_aggregates` | Fast advertiser query surface |

---

## DynamoDB — `ads` (ads catalog + ownership)

- **PK**: `ad_id` (string, e.g. `ad_8f3...`)
- **Attributes**:
  - `campaign_id` (string)
  - `advertiser_id` (string)
  - `destination_url` (string, absolute https URL)
  - `active` (bool)
  - `created_at` (ISO8601)
- **GSI `advertiser-campaign-index`**: PK `advertiser_id`, SK `campaign_id` — lists an
  advertiser's campaigns for ownership checks (FR-009) and dashboards.

**Validation / invariants**:
- `destination_url` MUST be an absolute `https://` URL (open-redirect guard, FR-005 path).
- A click for an `ad_id` not present, or with `active = false`, is rejected (FR-005).
- `campaign_id` and `advertiser_id` are required and immutable for an ad.

Capacity: PAY_PER_REQUEST (on-demand) for the educational build.

---

## Redis (ElastiCache) — impression dedup

- **Key**: `imp:<impression_id>` → value `1`
- **Write**: `SET imp:<impression_id> 1 NX EX 172800` (48h TTL).
- **Semantics**: `NX` success ⇒ first time ⇒ emit click event. `NX` failure ⇒ duplicate ⇒
  redirect only, no event (FR-004 / SC-005).

Optional (not in v1): `qcache:<campaign>:<from>:<to>:<gran>` → JSON, short TTL, for hot
repeated queries.

---

## Click Event (Kinesis record → S3 Parquet)

Canonical JSON put on Kinesis by the click processor; archived to S3 by Firehose. See
`contracts/click-event.schema.json` for the authoritative schema.

| Field | Type | Notes |
|-------|------|-------|
| `event_id` | string (uuid) | Per-event id (debug/trace) |
| `impression_id` | string | Dedup key; reconciliation de-dups on this |
| `ad_id` | string | FK → DynamoDB `ads` |
| `campaign_id` | string | Denormalized at capture for windowing without a join |
| `advertiser_id` | string | Denormalized for convenience |
| `click_ts` | string (ISO8601 UTC) | Event time; basis for `minute_bucket` |
| `minute_bucket` | string (`YYYY-MM-DDTHH:MM:00Z`) | UTC minute floor of `click_ts` |
| `user_agent` | string | Optional, for future fraud analysis (not used in counts) |
| `ingest_ts` | string (ISO8601 UTC) | Set by processor; latency/debug |

**Partition key (Kinesis)**: `"<ad_id>:<salt>"`, salt ∈ [0, N) — hot-shard mitigation (D3).
Denormalizing `campaign_id` at capture lets Flink and Spark aggregate without a DynamoDB
join.

**S3 layout**: `raw/dt=YYYY-MM-DD/hr=HH/*.parquet` (event-time derived partitions).

---

## Redshift — `click_aggregates` (OLAP rollup)

```
campaign_id    VARCHAR(64)   NOT NULL
minute_bucket  TIMESTAMP     NOT NULL   -- UTC minute floor
click_count    BIGINT        NOT NULL
source         VARCHAR(8)    NOT NULL   -- 'stream' (Flink) | 'batch' (reconciled, authoritative)
updated_at     TIMESTAMP     NOT NULL
PRIMARY KEY (campaign_id, minute_bucket)
```

- **DISTKEY** `campaign_id`, **SORTKEY** `(campaign_id, minute_bucket)` — range scans for a
  campaign over a window are contiguous (SC-002).
- **Upsert (Flink)**: insert-or-add on `(campaign_id, minute_bucket)` with `source='stream'`.
- **Reconcile (Spark)**: per period, delete rows then insert authoritative counts with
  `source='batch'`; queries thereafter return exact values (FR-013/FR-014/SC-004).
- **State transition** of a `(campaign, minute)` row: `absent → stream (approx) → batch
  (exact)`. Once `batch`, it is the source of truth and only later reconciliations may
  replace it.

**Query-time rollups**: hour/day via `date_trunc('hour'|'day', minute_bucket)` (FR-008); no
separate rollup tables (YAGNI).

---

## Relationships

```
Advertiser 1───* Campaign 1───* Ad 1───* (Ad Impression 1───0..1 Click Event)
                     │                                          │
                     └──────────── aggregated by ──────────────┘
                                 (campaign_id, minute_bucket)
                                          │
                                   Redshift click_aggregates
```

- An impression yields at most one counted click (Redis NX).
- A click event is attributed to exactly one `(campaign_id, minute_bucket)`.
- An aggregate row = SUM of distinct-impression clicks for a campaign in a minute.
