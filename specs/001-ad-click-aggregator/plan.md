# Implementation Plan: Ad Click Aggregator

**Branch**: `001-ad-click-aggregator` | **Date**: 2026-06-13 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-ad-click-aggregator/spec.md`

## Summary

Build the educational ad click aggregator end to end on AWS, faithful to `REFERENCE.md`.
A user click hits **API Gateway → a Ruby click-processor Lambda** that validates the ad
(DynamoDB), de-duplicates the impression (Redis), emits a click event to **Kinesis Data
Streams**, and 302-redirects the user to the advertiser. Two consumers read the stream:
**Managed Service for Apache Flink** maintains 1-minute tumbling-window counts per campaign
and upserts them into **Redshift** (the OLAP aggregate store), while **Kinesis Data
Firehose** archives every raw click to **S3**. A Ruby **query-service Lambda** behind API
Gateway serves advertiser metric queries from Redshift (scoped to the caller's campaigns).
A scheduled **Spark (AWS Glue) reconciliation** job re-derives exact counts from the S3
raw archive and overwrites the Redshift aggregates for each period, guaranteeing
billing-grade integrity. Everything is provisioned with **Terraform**.

## Technical Context

**Language/Version**:
- Ruby 3.3 (AWS Lambda `ruby3.3` runtime) — click processor, query service, shared lib.
- Python 3.11 + PyFlink (Apache Flink 1.20 Table API) — stream aggregator on Managed
  Service for Apache Flink. *(Non-Ruby; documented exception under Constitution
  Principle III. Migrated from Java to PyFlink to drop the Java/Maven toolchain.)*
- Python 3.10 + PySpark (AWS Glue 4.0 / Spark 3.3) — reconciliation batch job.
  *(Non-Ruby; documented exception under Constitution Principle III.)*

**Primary Dependencies**:
- Ruby: `aws-sdk-dynamodb`, `aws-sdk-kinesis`, `redis`, `rack` (HTTP API event shaping),
  `rspec`, `standard` (lint/format).
- PyFlink: `apache-flink` (Table API) + bundled `flink-sql-connector-kinesis` and
  Redshift JDBC driver jars for the `JdbcSink`.
- Spark/Glue: bundled PySpark, `boto3` for catalog access.
- IaC: Terraform ≥ 1.7, `hashicorp/aws` provider ≥ 5.x.

**Storage**:
- **DynamoDB** — ads catalog (ad → campaign, advertiser, destination) + advertiser→campaign
  ownership.
- **Amazon Redshift Serverless** — `click_aggregates` OLAP table (campaign × minute bucket).
- **Amazon S3** — raw click archive (partitioned `dt=/hr=`) feeding reconciliation.
- **Amazon ElastiCache for Redis** — impression-ID dedup set (and optional query cache).

**Testing**:
- RSpec unit + integration for Ruby Lambdas; **LocalStack** for local DynamoDB/Kinesis/S3
  integration tests.
- PyFlink Table API windowing test via the Flink MiniCluster harness bundled with
  `apache-flink` (smoke-level for education).
- PySpark reconciliation tested locally against sample Parquet/JSON raw clicks.
- `terraform validate` + `terraform plan` in CI; targeted `terraform apply` against a dev
  account when the user supplies credentials.

**Target Platform**: AWS (serverless + managed services), region `us-east-1` (dev). Local
dev via LocalStack + Docker.

**Project Type**: Cloud data-pipeline (infrastructure design) — multiple small services +
stream/batch jobs + IaC, not a single monolith.

**Performance Goals** (from spec Success Criteria):
- Sustain 10,000 clicks/sec peak with no accepted-click loss (SC-001).
- 95% of advertiser queries < 1 s (SC-002).
- New click visible in queries within 1 minute (SC-003).
- 0% discrepancy after reconciliation (SC-004).

**Constraints**:
- Idempotency: at most one count per impression (SC-005).
- Hot-shard tolerance: a single ad up to 50% of traffic must not drop clicks (SC-006).
- Educational/cost-aware: smallest capacity that demonstrates each behavior; full stack
  must be `terraform destroy`-able.

**Scale/Scope**: ~10M ads, far fewer campaigns (aggregate key is campaign × minute → low
cardinality, Redshift-friendly). 3 user stories, ~5 Terraform modules, 2 Ruby Lambdas,
1 Flink app, 1 Glue job.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| # | Principle | Status | Notes |
|---|-----------|--------|-------|
| I | Reference-Architecture Fidelity | ✅ PASS | All named components present: API Gateway, Kinesis Data Streams, Flink (Managed Service for Apache Flink), Spark (Glue) reconciliation, Redshift OLAP, DynamoDB ads, Redis dedup, S3 raw archive. Pipeline matches the reference end to end. |
| II | Infrastructure as Code (Terraform) | ✅ PASS | 100% of AWS resources in `infra/terraform/` modules; no console-only resources planned. |
| III | Ruby-First Implementation | ✅ PASS (with documented exceptions) | Both Lambdas + shared lib in Ruby 3.3. Flink (PyFlink) and Spark (PySpark) are the explicitly-permitted "no practical Ruby option" exceptions named in Principle III. Recorded in Complexity Tracking. |
| IV | Data Integrity & Idempotency | ✅ PASS | Impression-ID dedup in Redis before counting; raw clicks durably archived to S3; Spark reconciliation recomputes authoritative counts; no swallowed errors (failed Kinesis put → 5xx, client/edge retries; nothing silently dropped). |
| V | Managed AWS Services First | ✅ PASS | Managed Service for Apache Flink (not self-run Flink), Kinesis (not self-hosted Kafka), Redshift Serverless, ElastiCache, Glue, Lambda — all managed. |
| VI | Educational Clarity | ✅ PASS | Per-component READMEs mapping each module to the reference; explicit over clever; no abstractions beyond the reference design (YAGNI). |

**Gate result: PASS.** The only deviations from "pure Ruby" are the Flink and Spark jobs,
which Principle III explicitly sanctions. See Complexity Tracking.

## Project Structure

### Documentation (this feature)

```text
specs/001-ad-click-aggregator/
├── plan.md              # This file
├── research.md          # Phase 0 — technology decisions & rationale
├── data-model.md        # Phase 1 — entities, keys, schemas
├── quickstart.md        # Phase 1 — provision, seed, demo, destroy
├── contracts/           # Phase 1 — interface contracts
│   ├── click-api.yaml         # OpenAPI: GET /click (302 redirect)
│   ├── query-api.yaml         # OpenAPI: GET /metrics (JSON)
│   ├── click-event.schema.json# Kinesis click-event record schema
│   ├── redshift-schema.sql    # click_aggregates + external/raw schema
│   └── dynamodb-ads.md        # Ads/ownership table key design
└── tasks.md             # Phase 2 output (/speckit-tasks — NOT created here)
```

### Source Code (repository root)

```text
infra/
└── terraform/
    ├── modules/
    │   ├── ingestion/        # API Gateway (click) + click-processor Lambda + Kinesis stream
    │   ├── streaming/        # Managed Service for Apache Flink app + Firehose→S3
    │   ├── storage/          # DynamoDB, Redshift Serverless, S3 archive, ElastiCache Redis, VPC
    │   ├── query/            # API Gateway (metrics) + query-service Lambda
    │   └── reconciliation/   # Glue Spark job + EventBridge schedule
    └── envs/
        └── dev/              # root module wiring modules together; backend + tfvars

services/                     # Ruby (Lambda)
├── click_processor/
│   ├── lib/                  # handler + ad lookup + dedup + kinesis emit + redirect
│   └── spec/
├── query_service/
│   ├── lib/                  # handler + ownership check + redshift query + bucketing
│   └── spec/
└── shared/                   # shared Ruby gem: models, AWS client wrappers, time bucketing
    ├── lib/
    └── spec/

stream/
└── flink-aggregator/         # PyFlink Table API app (1-min tumbling windows → Redshift)
    ├── main.py
    └── tests/

batch/
└── reconciliation/           # PySpark Glue job: S3 raw → recompute → overwrite Redshift
    ├── job.py
    └── tests/

seeds/                        # sample advertisers/campaigns/ads + load script + click simulator
docs/                         # architecture diagram + per-component notes
```

**Structure Decision**: This is an infrastructure/data-pipeline feature, so the layout is
organized by **runtime role** (`services/` Ruby Lambdas, `stream/` Flink, `batch/` Spark)
with all provisioning isolated in `infra/terraform/` modules that map 1:1 to the pipeline
stages. A `shared/` Ruby gem holds the entities and AWS client wrappers used by both
Lambdas. `seeds/` provides demo data and a click simulator so each user story is
independently demonstrable per the spec's Independent Test notes.

## Complexity Tracking

> Only deviations from the constitution that need justification.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|--------------------------------------|
| Flink job in **PyFlink (Python)**, not Ruby (Principle III) | Managed Service for Apache Flink runs Flink apps; there is no Ruby Flink runtime. PyFlink keeps the codebase Ruby + Python and drops the Java/Maven toolchain. Stream windowing is core to the reference design. | A Ruby Kinesis consumer doing windowing would re-implement Flink and violate Principle I (Reference-Architecture Fidelity) and V (managed-first). |
| Reconciliation job in **PySpark**, not Ruby (Principle III) | Spark/Glue is the reference's batch engine; Glue's first-class languages are Python/Scala, not Ruby. | A Ruby batch job over S3 would not be Spark and would violate Principle I. |

Both exceptions are explicitly pre-authorized by Principle III ("Flink jobs in
PyFlink/Java/Scala/SQL, Spark jobs in PySpark/Scala"). All service-tier code remains Ruby.
