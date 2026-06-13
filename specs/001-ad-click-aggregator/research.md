# Phase 0 Research: Ad Click Aggregator

All technology unknowns are resolved here as senior-engineer decisions. No
`NEEDS CLARIFICATION` markers remain. Each decision lists what was chosen, why, and the
alternatives considered. The constitution (`/.specify/memory/constitution.md`) pins the
overall stack; this document settles the *how*.

---

## D1. Redirect strategy: server-side redirect through the click processor

**Decision**: The click is logged **before** the redirect. API Gateway routes the click to
the click-processor Lambda, which records the click (dedup + Kinesis put) and then returns
an HTTP **302** to the ad's destination URL.

**Rationale**: `REFERENCE.md` calls this the "better" option — it ensures the click is
captured before the user leaves, supporting the high-integrity NFR. Ad-blocker evasion of a
parallel beacon is avoided because logging is on the redirect path itself.

**Alternatives considered**:
- *Immediate redirect + async beacon*: lower latency but ad blockers can drop the beacon →
  lost clicks, violating FR-002/SC-001. Rejected.
- *Client-side JS counting*: trivially spoofable; rejected for integrity.

**Trade-off accepted**: One synchronous dedup + stream put on the hot path. Both are
single-digit-ms; well within an acceptable redirect latency for an educational build.

---

## D2. Idempotency: Redis `SET NX` on impression ID

**Decision**: Each impression carries a unique `impression_id` (generated at ad-display
time, assumed per spec). The click processor runs `SET impression_id 1 NX EX <ttl>` against
ElastiCache Redis. First writer wins and proceeds to count; a failed `NX` means duplicate →
redirect still happens, but **no** click event is emitted.

**Rationale**: Matches the reference's "Redis stores seen impression IDs" approach. `SET NX`
is atomic, so concurrent duplicate clicks cannot both count (SC-005). TTL bounds memory and
matches the reconciliation/replay window.

**TTL choice**: 48 hours — comfortably longer than the (≤24h) reconciliation cycle so the
fast path and batch path agree on dedup within a period. Documented as a tunable.

**Alternatives considered**:
- *DynamoDB conditional put for dedup*: works but adds latency/cost to the hot path and
  duplicates Redis's stated role in the reference. Redis chosen.
- *Dedup only in Flink (stream-side)*: would still admit duplicate raw events to S3,
  complicating reconciliation. Front-door dedup is cleaner.

**Note on reconciliation dedup**: The Spark job also de-duplicates by `impression_id` over
raw S3 records, so exactness (SC-004) does not depend on Redis TTL behavior.

---

## D3. Stream transport: Kinesis Data Streams with salted partition key

**Decision**: One Kinesis Data Stream `click-events`. Partition key =
`"<ad_id>:<salt>"` where `salt` is `rand(0..N-1)` (N = configured salt factor, default 8).

**Rationale**: Directly addresses the reference's **hot-shard problem** (SC-006). A viral ad
keyed solely on `ad_id` would pin one shard; salting fans its traffic across up to N shards.
Flink re-aggregates across shards by `campaign_id`, so salting does not affect correctness.

**Capacity**: On-demand mode for the educational build (auto-scales, no shard math, easy
destroy). Provisioned mode noted as the cost-optimized production alternative (10K/s ÷ 1
MB/s per shard, with salting to spread hot keys).

**Alternatives considered**:
- *Self-hosted Kafka*: violates Principle V (managed-first). Rejected.
- *Partition by `campaign_id`*: a viral campaign still creates a hot shard; `ad_id`+salt is
  finer-grained. Chosen.

---

## D4. Stream aggregation: Managed Service for Apache Flink (Table API, Java)

**Decision**: A Flink application on Amazon Managed Service for Apache Flink consumes
`click-events`, applies a **1-minute tumbling window keyed by `campaign_id`** using
**event-time** with a bounded watermark, counts clicks per window, and **upserts** each
`(campaign_id, minute_bucket, count)` row into Redshift with `source = 'stream'`.

**Rationale**: Faithful to the reference (Kinesis → Flink → aggregate store). Event-time +
watermark attributes delayed/out-of-order clicks to the correct minute (FR-016). Tumbling
1-minute windows match the minimum granularity (FR-003/SC-007).

**Language**: Java 11 + Flink 1.20 Table API — documented Principle III exception (no Ruby
Flink runtime). Table API keeps the windowing logic close to declarative SQL for clarity
(Principle VI).

**Redshift write path**: Flink JDBC sink in upsert mode (batched). Rationale: keeps a single
authoritative aggregate store and lets the query service read one place. Aggregate
cardinality is campaigns × minutes (low), so Redshift handles the write rate comfortably.

**Alternatives considered**:
- *Flink → Redshift streaming ingestion (materialized view from Kinesis)*: would bypass
  Flink's windowing and undercut Principle I. Rejected; we want Flink to own aggregation.
- *Flink SQL via Studio notebook*: great for exploration but harder to manage in Terraform
  and ship reproducibly. JAR app chosen; Studio noted as a learning aid.
- *Flink → DynamoDB for real-time aggregates, Redshift only for batch*: splits the read path
  across two stores and conflicts with the constitution's "Redshift for OLAP". Rejected to
  keep one query surface.

---

## D5. Raw archive: Kinesis Data Firehose → S3 (Parquet, date/hour partitioned)

**Decision**: A second consumer, **Kinesis Data Firehose**, reads `click-events` and
delivers raw records to `s3://<bucket>/raw/dt=YYYY-MM-DD/hr=HH/` converting JSON → **Parquet**
via the Firehose record-format conversion.

**Rationale**: The reference enables "Kinesis to dump raw events to S3" for reconciliation.
Firehose is the managed, zero-code way to do this (Principle V). Parquet + Hive-style
partitions make the Spark/Glue reconciliation efficient and cheap.

**Buffering**: 60 s / 64 MB (whichever first) — keeps S3 archive within ~1 min of real time
while batching writes.

**Alternatives considered**:
- *Flink second sink to S3*: more code, more to break; Firehose is purpose-built. Rejected.
- *Kinesis → Lambda → S3*: re-implements Firehose. Rejected.

---

## D6. Aggregate store: Amazon Redshift Serverless

**Decision**: Redshift **Serverless** holds the single `click_aggregates` table
`(campaign_id, minute_bucket, click_count, source, updated_at)`. Both Flink (real-time,
`source='stream'`) and the Spark reconciliation job (`source='batch'`, authoritative) write
it. The query service reads it.

**Rationale**: Constitution pins Redshift for OLAP. Serverless removes cluster sizing/babysit
and bills per use — ideal for an educational, destroy-when-idle stack (Principle VI,
cost-awareness). Aggregated table is tiny relative to raw clicks, so sub-second range scans
over `(campaign_id, minute_bucket)` are easily achievable (SC-002), aided by a sort key on
`(campaign_id, minute_bucket)`.

**Reconciliation overwrite semantics**: Spark writes batch results to a staging table, then
a transaction deletes the period's rows and inserts the recomputed authoritative rows
(`source='batch'`). After that, queries return exact counts (FR-014/SC-004).

**Alternatives considered**:
- *Provisioned Redshift cluster*: always-on cost; overkill for the demo. Serverless chosen.
- *DynamoDB or Postgres aggregate store* (reference says these are acceptable for the simpler
  query shape): would contradict the constitution's explicit Redshift-for-OLAP choice and
  reduce the learning value of Redshift. Rejected.

---

## D7. Ads catalog & ownership: DynamoDB

**Decision**: DynamoDB table `ads` keyed by `ad_id` (PK) holding `campaign_id`,
`advertiser_id`, `destination_url`, `active`. A GSI `advertiser-campaign-index`
(`advertiser_id` PK, `campaign_id` SK) backs the query service's ownership check.

**Rationale**: Constitution pins DynamoDB for the ads catalog; click processing needs a
single-digit-ms point lookup by `ad_id` on the hot path — DynamoDB's core strength.

**Alternatives considered**: caching the catalog in Redis to avoid the DynamoDB read — noted
as an optional optimization, not needed for correctness; kept out of v1 for clarity (YAGNI).

---

## D8. Reconciliation engine & schedule: PySpark on AWS Glue, hourly via EventBridge

**Decision**: An **AWS Glue** PySpark job runs **hourly** (EventBridge schedule). It reads
the previous closed hour(s) of raw clicks from S3, **de-duplicates by `impression_id`**,
counts per `(campaign_id, minute_bucket)`, and overwrites the corresponding Redshift
aggregate rows as authoritative.

**Rationale**: Glue is serverless managed Spark (Principle V) — no EMR cluster to manage or
leave running. Hourly cadence corrects the fast path quickly while keeping cost low; the
reference allows hourly/daily. Reprocessing whole closed hours makes the job idempotent and
handles late arrivals within the window (FR-016).

**Language**: PySpark — documented Principle III exception.

**Alternatives considered**:
- *EMR Spark*: more control, but a standing/again-provisioned cluster adds cost/ops. Glue
  chosen for the educational build; EMR noted as the scale-up path.
- *Daily only*: simpler but leaves the fast path uncorrected longer. Hourly chosen as the
  balance; cadence is a single Terraform variable.

---

## D9. Services runtime: Ruby 3.3 on AWS Lambda behind API Gateway HTTP API

**Decision**: Click processor and query service are Ruby 3.3 Lambdas fronted by **API
Gateway HTTP API** (cheaper/simpler than REST API). Shared logic (entities, AWS client
wrappers, minute-bucket helpers) lives in a `shared/` Ruby gem layered into both.

**Rationale**: Principle III (Ruby-first) + Principle V (Lambda over standing servers). HTTP
API is the lean choice for two simple routes and supports the 302 redirect response.

**Demo principal model (resolves analysis finding U1)**: the spec assumes an identity
provider exists and out-of-scopes building one. For the educational build, the query
service treats the **bearer token value as the `advertiser_id`** (e.g. `Authorization:
Bearer adv_demo`). `advertiser_id` is therefore always derived from the token, never from
the query string, so ownership scoping (FR-009) is still exercised end to end. A production
build would attach a JWT authorizer to API Gateway and read the `advertiser_id` claim — a
drop-in change that does not affect the query logic.

**Alternatives considered**:
- *Containers on ECS/Fargate*: standing infra, against Lambda-first. Rejected.
- *REST API Gateway*: more features (API keys, request validation) than needed; HTTP API
  suffices and is cheaper. Rejected for v1.

---

## D10. Networking & dependencies

**Decision**: A small VPC (2 private subnets across AZs) hosts ElastiCache Redis and the
Redshift Serverless workgroup; Lambdas that touch Redis/Redshift run in-VPC with a NAT-free
design using **VPC endpoints** (DynamoDB gateway endpoint, Kinesis/Firehose/S3 interface
endpoints) to avoid NAT Gateway cost.

**Rationale**: ElastiCache and Redshift are VPC-bound. VPC endpoints keep Lambda→AWS traffic
private and dodge NAT charges (cost-awareness, Principle VI). The click processor only needs
DynamoDB + Kinesis + Redis; the query service needs Redshift (+ optional Redis).

**Alternatives considered**: public Redshift Serverless endpoint + Lambdas outside VPC —
simpler but less realistic and exposes Redis problems (Redis can't be public). In-VPC with
endpoints chosen as the faithful, still-cheap option.

---

## D11. Local development & testing: LocalStack + RSpec; local PySpark; Flink MiniCluster

**Decision**:
- Ruby Lambdas: RSpec unit tests + integration tests against **LocalStack** (DynamoDB,
  Kinesis, S3) and a local Redis container.
- Flink: Table API logic tested with the Flink MiniCluster harness (smoke level).
- Spark: `job.py` factored so the transform is a pure function tested with local PySpark over
  sample raw JSON/Parquet fixtures.
- Terraform: `fmt`/`validate`/`plan` gates; `apply` only against a real dev account when the
  user provides credentials.

**Rationale**: Lets every user story's Independent Test (from the spec) run without a live
AWS bill where possible (Principle VI). Flink/Redshift end-to-end is validated on real AWS
in `quickstart.md`.

**Alternatives considered**: full AWS-only testing (slow, costly) — rejected for the inner
loop; reserved for the end-to-end demo.

---

## Cross-cutting resolutions

- **Time semantics**: all buckets are UTC minute floors (`minute_bucket` = epoch minute or
  ISO `YYYY-MM-DDTHH:MM:00Z`). Event-time everywhere; client-supplied click timestamp is
  trusted but bounded by Flink watermark and re-derived exactly in reconciliation.
- **Error handling (Principle IV)**: a failed Kinesis put returns 5xx so the edge/client
  retries; the redirect still occurs only after a durable enqueue. Nothing is swallowed.
  Flink failures restart from checkpoint; Firehose retries to an S3 error prefix.
- **Granularity rollups**: Redshift stores **minute** buckets; hour/day are computed at query
  time with `date_trunc` (FR-008), avoiding redundant pre-rollup tables (YAGNI).
- **Security scoping (FR-009)**: query service derives `advertiser_id` from the
  authenticated principal (assumed present per spec) and validates campaign ownership via the
  DynamoDB GSI before querying Redshift.
