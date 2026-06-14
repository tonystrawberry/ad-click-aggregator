# Architecture — Ad Click Aggregator

Educational implementation of the Hello Interview "Ad Click Aggregator" design
(`REFERENCE.md`). Every component below maps to a section of that reference.

## Pipeline

```
                         ┌──────────────┐
 Browser click ──GET────▶│ API Gateway  │  (REFERENCE: System interface & data flow)
                         │  /click      │
                         └──────┬───────┘
                                ▼
                       ┌──────────────────┐   GetItem      ┌──────────────┐
                       │ Click Processor  │───────────────▶│  DynamoDB    │  ads catalog
                       │  Lambda (Ruby)   │   active ad?    │  (ads)       │  (REFERENCE: validate)
                       │                  │                └──────────────┘
                       │  SET NX impr ────┼───────────────▶  ElastiCache Redis  (REFERENCE: idempotency)
                       │                  │   dedup
                       │  302 redirect ◀──┼── to advertiser (REFERENCE: redirect flow, server-side)
                       └────────┬─────────┘
                                │ PutRecord (key = ad_id:salt — REFERENCE: hot shard)
                                ▼
                        ┌───────────────┐
                        │ Kinesis Data  │  click-events  (REFERENCE: Kinesis stream)
                        │ Stream        │
                        └───┬───────┬───┘
              consumer 1    │       │   consumer 2
                            ▼       ▼
        ┌────────────────────┐   ┌────────────────────────┐
        │ Managed Flink      │   │ Kinesis Data Firehose   │
        │ 1-min tumbling     │   │ → S3 raw/dt=/hr= Parquet│  (REFERENCE: dump raw to S3)
        │ count by campaign  │   └────────────┬────────────┘
        │ (REFERENCE: Flink) │                │
        └─────────┬──────────┘                │ hourly
       upsert     │ source='stream'           ▼
                  ▼                  ┌────────────────────────┐
        ┌────────────────────┐      │ Glue PySpark            │
        │ Redshift Serverless│◀─────│ reconciliation          │  (REFERENCE: Spark batch,
        │ click_aggregates   │ swap │ recompute from raw,     │   Lambda/Kappa reconciliation)
        │ (REFERENCE: OLAP)  │ batch│ source='batch' (exact)  │
        └─────────┬──────────┘      └─────────────────────────┘
                  │ date_trunc range scan
                  ▼
        ┌────────────────────┐   GET /metrics   ┌──────────────┐
        │ Query Service      │◀─────────────────│ API Gateway  │◀── Advertiser dashboard
        │ Lambda (Ruby)      │   ownership check │  /metrics    │
        └────────────────────┘   (DynamoDB GSI)  └──────────────┘
```

## Reference mapping

| REFERENCE.md concept | This implementation |
|----------------------|---------------------|
| Click processor + validation | `services/click_processor` (Ruby Lambda) + DynamoDB lookup |
| Idempotency via impression IDs + Redis | `Shared::Aws::RedisDeduper` (`SET NX EX`) |
| Log raw click data | Firehose → S3 `raw/` Parquet |
| Kinesis click event stream | `infra/.../ingestion/kinesis.tf` (on-demand) |
| Hot shard mitigation | Salted partition key `ad_id:salt` (`Shared::Aws::Kinesis`) |
| Flink stream aggregator (near real-time) | `stream/flink-aggregator` → Redshift `source='stream'` |
| Pre-aggregation OLAP store | Redshift Serverless `click_aggregates` |
| Spark batch reconciliation | `batch/reconciliation/job.py` (Glue) → `source='batch'` |
| Periodic reconciliation (Lambda/Kappa) | EventBridge hourly schedule → Glue job |
| Query service (<1s) | `services/query_service` reading Redshift with `date_trunc` |

## Why two write paths into one store

The near-real-time path (Flink) optimizes for **freshness** (SC-003) but is
approximate under lateness/failure. The batch path (Spark) optimizes for
**exactness** (SC-004) and overwrites each closed period authoritatively. Both
write the single Redshift `click_aggregates` table, distinguished by a `source`
column, so the query service has one read surface. This is the reference's
"reconciliation" design — neither pure Kappa nor pure Lambda.

## Data integrity (Constitution Principle IV)

- Impression dedup (Redis `SET NX`) → at most one count per impression (SC-005).
- Failed Kinesis put → HTTP 502, never a silent drop.
- Every accepted click is archived raw to S3 → fully replayable.
- Reconciliation recomputes from raw and de-dups again → exact billing counts.

See `specs/001-ad-click-aggregator/` for the spec, plan, research decisions, and
contracts.
