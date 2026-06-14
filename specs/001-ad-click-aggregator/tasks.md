---
description: "Task list for the Ad Click Aggregator feature"
---

# Tasks: Ad Click Aggregator

**Input**: Design documents from `specs/001-ad-click-aggregator/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Targeted tests are included where they directly verify a Success Criterion
(idempotency SC-005, reconciliation exactness SC-004, query bucketing/zero-fill, hot-shard
no-loss SC-006). This is intentional given Constitution Principle IV (Data Integrity); it is
not full TDD across every file.

**Organization**: Phase 1 Setup → Phase 2 Foundational (shared infra) → Phase 3 US1 (P1,
MVP) → Phase 4 US2 (P2) → Phase 5 US3 (P3) → Phase 6 Polish. Each user story is an
independently demonstrable increment.

## Path Conventions

Per plan.md structure: `infra/terraform/` (IaC modules + `envs/dev`), `services/` (Ruby
Lambdas + `shared/` gem), `stream/flink-aggregator/` (PyFlink), `batch/reconciliation/`
(PySpark Glue), `seeds/`, `docs/`.

---

## Phase 1: Setup (Shared Project Scaffolding)

**Purpose**: Repo skeleton, toolchains, and IaC backbone so every later phase has a home.

- [X] T001 Create the repository directory skeleton per plan.md (`infra/terraform/modules/{ingestion,streaming,storage,query,reconciliation}`, `infra/terraform/envs/dev`, `services/{click_processor,query_service,shared}`, `stream/flink-aggregator`, `batch/reconciliation`, `seeds`, `docs`) with a placeholder `.keep` in each empty dir
- [X] T002 [P] Initialize the shared Ruby gem in `services/shared/` (`shared.gemspec`, `Gemfile` with `aws-sdk-dynamodb`, `aws-sdk-kinesis`, `redis`, `rspec`, `standard`; `lib/shared.rb` entrypoint)
- [X] T003 [P] Initialize `services/click_processor/` Bundler project (`Gemfile` referencing the `shared` gem via path, `rspec`, `standard`) and `services/query_service/` likewise (add `pg` for Redshift access in query_service)
- [X] T004 [P] Initialize the PyFlink project in `stream/flink-aggregator/` (`requirements.txt` with `apache-flink==1.20.*`, `tests/`; connector + Redshift JDBC jars bundled at build time)
- [X] T005 [P] Initialize the PySpark Glue project in `batch/reconciliation/` (`job.py` stub, `requirements-dev.txt` with `pyspark==3.3.*` and `pytest`, `tests/` dir)
- [X] T006 [P] Configure linting/formatting: `.standard.yml` for Ruby, `.editorconfig`, and a root `Makefile` with targets `test`, `build-lambdas`, `build-flink`, `build-glue`, `fmt`
- [X] T007 [P] Add `docker-compose.test.yml` at repo root (LocalStack with dynamodb/kinesis/s3 + a Redis container) for local integration tests
- [X] T008 Scaffold Terraform root in `infra/terraform/envs/dev/` (`main.tf` module wiring stubs, `variables.tf`, `outputs.tf`, `providers.tf` pinning aws ~>5.x + terraform >=1.7, `backend.tf`, `dev.tfvars.example`) and run `terraform init`

**Checkpoint**: `make fmt` and `terraform init` succeed; empty module directories exist.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Shared infrastructure and shared-code primitives that EVERY user story depends
on. No user story can be implemented until this phase is complete.

**⚠️ CRITICAL**: Storage/network must exist before any service can read/write it.

### Shared Ruby primitives (used by both Lambdas)

- [X] T009 [P] Implement UTC minute-bucket helpers in `services/shared/lib/shared/time_bucket.rb` (`minute_floor(ts) → "YYYY-MM-DDTHH:MM:00Z"`, parse/format) per data-model.md
- [X] T010 [P] Implement the `ClickEvent` value object + JSON serialization in `services/shared/lib/shared/click_event.rb`, validating against `contracts/click-event.schema.json` fields
- [X] T011 [P] Implement AWS client wrappers in `services/shared/lib/shared/aws/` (`dynamodb.rb` get-ad, `kinesis.rb` put-record with salted partition key `"<ad_id>:<salt>"`, `redis.rb` `SET NX EX` dedup) per research D2/D3/D7
- [X] T012 [P] Unit-test shared helpers in `services/shared/spec/` (time bucketing edge cases, ClickEvent schema validation, salted partition-key format)

### Networking & storage (Terraform `storage` module — VPC lives here per plan D10)

- [X] T013 Implement VPC + 2 private subnets (multi-AZ) + security groups + VPC endpoints (DynamoDB gateway; Kinesis/Firehose/S3/Secrets interface endpoints) in `infra/terraform/modules/storage/vpc.tf` (no NAT, per research D10)
- [X] T014 [P] Implement DynamoDB `ads` table + `advertiser-campaign-index` GSI (PAY_PER_REQUEST, PITR) in `infra/terraform/modules/storage/dynamodb.tf` per `contracts/dynamodb-ads.md`
- [X] T015 [P] Implement the S3 raw-archive bucket (versioning off, lifecycle to expire old raw partitions, `raw/` prefix) + an artifacts bucket for Flink JAR / Glue script in `infra/terraform/modules/storage/s3.tf`
- [X] T016 [P] Implement ElastiCache Redis (single-node, in-VPC, SG allowing Lambda) in `infra/terraform/modules/storage/redis.tf` per research D2/D10
- [X] T017 [P] Implement Redshift Serverless namespace + workgroup (in-VPC, admin secret in Secrets Manager) in `infra/terraform/modules/storage/redshift.tf` per research D6
- [X] T018 Define `storage` module outputs (vpc/subnet/sg ids, table name, GSI name, bucket names, redis endpoint, redshift workgroup/endpoint/secret arn) in `infra/terraform/modules/storage/outputs.tf` and wire the module into `envs/dev/main.tf`
- [X] T019 Author the Redshift DDL bootstrap from `contracts/redshift-schema.sql` as `seeds/create_redshift_schema.rb` (creates `click_aggregates` + `click_aggregates_stage`, idempotent)

**Checkpoint**: `terraform apply` provisions network + all data stores; shared gem tests pass.
User-story phases can now begin (and can proceed in parallel if staffed).

---

## Phase 3: User Story 1 — Capture a click and redirect (Priority: P1) 🎯 MVP

**Goal**: A click hits the system, is de-duplicated, durably enqueued, archived to S3, and
the user is 302-redirected to the advertiser.

**Independent Test**: `GET /click?ad_id=ad_demo_1&impression_id=imp_001` returns 302 to the
correct destination; replaying the same impression keeps the counted total at one; unknown
ad → 404 with no record; exactly one raw record per impression lands in S3.

### Implementation — click processor (Ruby Lambda)

- [X] T020 [P] [US1] Implement ad lookup + validation in `services/click_processor/lib/ad_repository.rb` (GetItem by `ad_id`; reject missing/`active=false`; return destination/campaign/advertiser) per FR-005
- [X] T021 [P] [US1] Implement impression dedup in `services/click_processor/lib/deduper.rb` using Redis `SET imp:<id> 1 NX EX 172800` (first-writer-wins) per FR-004/SC-005
- [X] T022 [US1] Implement the Lambda handler in `services/click_processor/lib/handler.rb`: parse `ad_id`/`impression_id`/`click_ts` → validate ad → dedup → build `ClickEvent` (minute_bucket via shared helper) → on first-time, `kinesis.put` with salted key → return 302 `Location: destination_url`; on duplicate, 302 without emit; on unknown ad, 404; on Kinesis failure, 502 (no silent drop) per FR-001/FR-002/Principle IV
- [X] T023 [US1] Add request validation + error JSON shapes in `services/click_processor/lib/handler.rb` for 400 (missing/malformed params) per `contracts/click-api.yaml`

### Infrastructure — ingestion (Terraform `ingestion` module)

- [X] T024 [P] [US1] Implement Kinesis Data Stream `click-events` (on-demand mode) in `infra/terraform/modules/ingestion/kinesis.tf` per research D3
- [X] T025 [US1] Implement the click-processor Lambda (ruby3.3, in-VPC, env: table name, stream name, redis endpoint, salt factor) + IAM role (DynamoDB GetItem, Kinesis PutRecord, Redis SG) + the `shared` gem as a layer/bundled dep in `infra/terraform/modules/ingestion/lambda.tf`
- [X] T026 [US1] Implement API Gateway HTTP API with `GET /click` integrated to the Lambda (302 passthrough) in `infra/terraform/modules/ingestion/apigw.tf`; export `click_api_url`; wire module into `envs/dev/main.tf`

### Verification

- [X] T027 [P] [US1] RSpec unit tests in `services/click_processor/spec/handler_spec.rb` covering: first click emits + 302, duplicate impression emits nothing (SC-005), unknown ad → 404 no emit (FR-005), Kinesis failure → 502 (Principle IV)
- [X] T028 [US1] LocalStack integration test in `services/click_processor/spec/integration/click_flow_spec.rb` (tagged `:integration`): real DynamoDB+Kinesis+Redis via docker-compose; asserts exactly one Kinesis record per impression across replays

**Checkpoint**: US1 is independently demonstrable — clicks redirect and land durably,
de-duplicated. This is the MVP.

---

## Phase 4: User Story 2 — Advertiser queries metrics (Priority: P2)

**Goal**: Captured clicks become near-real-time minute aggregates in Redshift, queryable by
the owning advertiser at minute/hour/day granularity in <1s.

**Independent Test**: With known recorded clicks, `GET /metrics?campaign_id=...&from=...&to=...&granularity=minute|hour`
returns correct per-bucket counts; empty buckets return 0; a non-owned campaign → 403; a
click is reflected within ~1 minute.

### Stream aggregation (Flink — PyFlink; depends on Kinesis from US1)

- [X] T029 [P] [US2] Implement the PyFlink Table API job in `stream/flink-aggregator/main.py`: Kinesis source on `click-events`, event-time + bounded watermark, 1-minute tumbling window keyed by `campaign_id`, COUNT → rows `(campaign_id, minute_bucket, count)` per research D4/FR-003/FR-016
- [X] T030 [US2] Implement the Redshift `JdbcSink` (single-statement MERGE, replace on `(campaign_id, minute_bucket)`, `source='stream'`) in `stream/flink-aggregator/main.py` per `contracts/redshift-schema.sql`
- [X] T031 [P] [US2] PyFlink MiniCluster smoke test in `stream/flink-aggregator/tests/test_windowing.py`: out-of-order events land in the correct minute bucket; counts per window correct

### Query service (Ruby Lambda)

- [X] T032 [P] [US2] Implement ownership check in `services/query_service/lib/ownership.rb` (Query `advertiser-campaign-index` for `advertiser_id`+`campaign_id`; 403 if absent) per FR-009
- [X] T033 [P] [US2] Implement the Redshift reader in `services/query_service/lib/aggregate_repository.rb` (parameterized `date_trunc(:gran, minute_bucket)` SUM query from `contracts/redshift-schema.sql`)
- [X] T034 [US2] Implement bucket assembly + zero-fill in `services/query_service/lib/bucketizer.rb` (emit every bucket in `[from,to)` at granularity, defaulting missing to 0) per FR-010/SC-007
- [X] T035 [US2] Implement the Lambda handler in `services/query_service/lib/handler.rb`: derive `advertiser_id` from bearer principal → ownership check → validate range/granularity (400 on bad range) → query → zero-fill → JSON per `contracts/query-api.yaml`

### Infrastructure — streaming + query (Terraform)

- [X] T036 [P] [US2] Implement Managed Service for Apache Flink application (reads JAR from artifacts bucket, in-VPC, IAM: Kinesis read + Redshift write via secret) in `infra/terraform/modules/streaming/flink.tf` per research D4
- [X] T037 [US2] Implement the query-service Lambda (ruby3.3, in-VPC, env: redshift workgroup/secret, table/GSI) + IAM (Redshift Data API or JDBC via secret, DynamoDB Query on GSI) in `infra/terraform/modules/query/lambda.tf`
- [X] T038 [US2] Implement API Gateway HTTP API `GET /metrics` with bearer authorizer integrated to the query Lambda in `infra/terraform/modules/query/apigw.tf`; export `query_api_url`; wire `streaming` + `query` modules into `envs/dev/main.tf`

### Verification

- [X] T039 [P] [US2] RSpec tests for `services/query_service/spec/`: bucketizer zero-fill (FR-010), ownership 403 (FR-009), bad range → 400, minute vs hour vs day rollup shapes (FR-008)

**Checkpoint**: US1 + US2 work together — clicks flow in and appear in advertiser queries
within ~1 minute at multiple granularities, scoped to the owner.

---

## Phase 5: User Story 3 — Reconciliation guarantees correct counts (Priority: P3)

**Goal**: A scheduled Spark job re-derives exact counts from the S3 raw archive and
overwrites Redshift aggregates so billed numbers are exact.

**Independent Test**: Inject drift into `click_aggregates` (source='stream'), run the Glue
job, then confirm the reconciled period's rows are `source='batch'` and match counts
derived directly from raw S3 clicks (0% discrepancy).

### Raw archive (Firehose → S3; depends on Kinesis from US1)

- [X] T040 [P] [US3] Implement Kinesis Data Firehose delivery stream consuming `click-events` → S3 `raw/dt=/hr=` with Parquet record-format conversion + a Glue table for the schema, 60s/64MB buffering, error prefix, in `infra/terraform/modules/streaming/firehose.tf` per research D5
- [X] T041 [US3] Verify S3 partition layout + Parquet schema match `contracts/click-event.schema.json` (document the Glue Data Catalog table DDL in `infra/terraform/modules/streaming/glue_catalog.tf`)

### Reconciliation job (PySpark — Glue)

- [X] T042 [P] [US3] Implement the pure transform in `batch/reconciliation/job.py` (`recompute(df) → distinct by impression_id → group by campaign_id, minute_bucket → count`) per research D8/FR-012
- [X] T043 [US3] Implement the Redshift swap in `batch/reconciliation/job.py`: load exact counts into `click_aggregates_stage`, then transactional DELETE period + INSERT `source='batch'` per `contracts/redshift-schema.sql`/FR-013/FR-014
- [X] T044 [P] [US3] PySpark unit tests in `batch/reconciliation/tests/test_recompute.py`: dedup by impression_id, correct minute bucketing of late/out-of-order rows, exact counts over a fixture (SC-004)

### Infrastructure — reconciliation (Terraform)

- [X] T045 [US3] Implement the Glue PySpark job (script from artifacts bucket, Glue 4.0, IAM: S3 read raw, Redshift write via secret, Glue catalog read) + EventBridge hourly schedule in `infra/terraform/modules/reconciliation/glue.tf` per research D8; wire module into `envs/dev/main.tf`

**Checkpoint**: All three stories functional — clicks captured, queried near-real-time, and
made exact by reconciliation.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Demo enablement, integrity validation across the whole pipeline, docs.

- [X] T046 [P] Implement `seeds/load_catalog.rb` (sample advertisers, campaigns, ads → DynamoDB) so all demos have data, per quickstart.md §3
- [X] T047 [P] Implement `seeds/click_simulator.rb` (`--rps`, `--hot-ad`, `--hot-share`, `--duration`) to drive load and exercise the hot-shard path (SC-006), per quickstart.md §4
- [X] T048 End-to-end hot-shard + no-loss check (SC-001/SC-006): run the simulator with `--hot-share 0.5`, then assert `COUNT(DISTINCT impression_id)` in S3 == accepted clicks; record results in `docs/load-test-notes.md`
- [X] T049 End-to-end reconciliation exactness check (SC-004): after a simulated run, compare query results pre/post Glue job; confirm `source='batch'` and 0% discrepancy; note in `docs/reconciliation-notes.md`
- [X] T050 [P] Write `docs/architecture.md` with the pipeline diagram and a per-component map back to REFERENCE.md sections (Constitution Principle VI)
- [X] T051 [P] Add per-module `README.md` to each `infra/terraform/modules/*` and to `services/`, `stream/`, `batch/` explaining what reference component it implements
- [X] T052 CI workflow `.github/workflows/ci.yml`: `make test` (Ruby/PySpark/Flink unit), `terraform fmt -check`, `terraform validate`
- [ ] T053 Validate the full `quickstart.md` walkthrough on a real dev account end to end (provision → seed → US1/US2/US3 demos → destroy) and fix any gaps

---

## Dependencies & Execution Order

### Phase dependencies

- **Setup (P1)**: no dependencies.
- **Foundational (P2)**: depends on Setup. **BLOCKS all user stories.** (Network + stores + shared gem.)
- **US1 (P3)**: depends on Foundational. The MVP. Creates the Kinesis stream that US2/US3 consume.
- **US2 (P4)**: depends on Foundational + the `click-events` Kinesis stream (T024) and Redshift (T017/T019) from earlier phases. Independently testable once aggregates exist.
- **US3 (P5)**: depends on Foundational + the `click-events` stream (T024) and Redshift schema (T019). Firehose archive (T040) can be built right after T024 — does not require US2.
- **Polish (P6)**: depends on the user stories being demoed are complete.

### Cross-story note on the shared Kinesis stream

`click-events` (T024) is created in the US1 phase because US1 is the producer. US2 (Flink)
and US3 (Firehose) are both consumers of it. If staffing in parallel, pull T024 forward to
the end of Foundational so all three streams of work proceed concurrently.

### Within each user story

- Models/repositories before handlers; handlers before the API Gateway wiring.
- Terraform store/stream resources before the Lambda/Flink/Glue that use them.
- Verification tasks after the implementation they cover.

### Parallel opportunities

- Setup: T002–T007 are all `[P]` (different projects/files).
- Foundational: shared-gem tasks T009–T012 `[P]`; storage tasks T014–T017 `[P]` (T013 VPC first, then they attach).
- US1: T020/T021 `[P]`; Terraform T024 `[P]` with the Ruby work; tests T027 `[P]`.
- US2: T029/T031/T032/T033 `[P]`; T036 `[P]`.
- US3: T040/T042/T044 `[P]`.
- Polish: T046/T047/T050/T051 `[P]`.

---

## Parallel Example: User Story 1

```bash
# After Foundational, launch US1 parallelizable work together:
Task: "T020 [US1] Ad lookup in services/click_processor/lib/ad_repository.rb"
Task: "T021 [US1] Impression dedup in services/click_processor/lib/deduper.rb"
Task: "T024 [US1] Kinesis stream in infra/terraform/modules/ingestion/kinesis.tf"
# Then T022 (handler) joins them, T025/T026 wire infra, T027 tests.
```

---

## Implementation Strategy

### MVP first (US1 only)

1. Phase 1 Setup → 2. Phase 2 Foundational → 3. Phase 3 US1 → **STOP & VALIDATE** the click
   capture/redirect/dedup demo (quickstart §4 US1). This alone is a shippable, demonstrable
   slice.

### Incremental delivery

- Add US2 → near-real-time advertiser queries (demo §4 US2).
- Add US3 → reconciliation exactness (demo §4 US3).
- Polish → seeds, load/hot-shard + reconciliation validation, docs, CI.

### Cost discipline (Constitution Principle II/VI)

Run `terraform destroy` between work sessions; Redshift Serverless and ElastiCache bill
while running. Keep dev capacity minimal.

---

## Notes

- `[P]` = different files, no incomplete-task dependency.
- `[USx]` maps each task to its spec user story for traceability.
- Flink (PyFlink, T029–T031, T036) and Spark (PySpark, T042–T045) are the Constitution
  Principle III documented exceptions; all service-tier code is Ruby.
- Commit after each task or logical group.
