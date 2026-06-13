# Quickstart: Ad Click Aggregator

How to provision, seed, demo each user story end to end, and tear everything down. Assumes
AWS credentials for a **dev** account (the user supplies these) and the local toolchain
below. This is an educational stack — destroy it when idle to avoid cost.

## Prerequisites

- Terraform ≥ 1.7, AWS CLI v2 (configured: `aws configure` / SSO)
- Ruby 3.3 + Bundler, Java 11 + Maven (Flink app), Python 3.10 (Glue job packaging)
- Docker (LocalStack + Redis for local tests)
- `jq`, `curl`

## 1. Local inner loop (no AWS bill)

```bash
# Ruby Lambdas
cd services/click_processor && bundle install && bundle exec rspec
cd ../query_service       && bundle install && bundle exec rspec

# Integration tests against LocalStack + Redis
docker compose -f docker-compose.test.yml up -d   # localstack + redis
bundle exec rspec --tag integration
docker compose -f docker-compose.test.yml down

# Spark reconciliation transform (pure-function tests over fixtures)
cd batch/reconciliation && python -m pytest

# Flink Table API smoke tests (MiniCluster)
cd stream/flink-aggregator && mvn test
```

## 2. Provision AWS (Terraform)

```bash
cd infra/terraform/envs/dev
terraform init
terraform plan      # review: VPC, DynamoDB, Redshift Serverless, S3, ElastiCache,
                    # Kinesis, Firehose, Managed Flink, Glue, 2x API GW + Lambdas
terraform apply
```

Outputs include `click_api_url`, `query_api_url`, Redshift workgroup, S3 raw bucket.

Build/upload artifacts (wired as Terraform-invoked build steps or a Make target):

```bash
make build-lambdas      # zip Ruby Lambdas + shared layer
make build-flink        # mvn package → upload JAR to artifacts bucket
make build-glue         # upload job.py to scripts bucket
terraform apply         # picks up new artifact versions
```

## 3. Seed demo data

```bash
cd seeds
ruby load_catalog.rb    # advertisers, campaigns, ads → DynamoDB
# create Redshift table:
ruby create_redshift_schema.rb   # runs contracts/redshift-schema.sql DDL
```

## 4. Demo each user story (maps to spec Independent Tests)

### US1 — Capture a click + redirect (P1, MVP)
```bash
CLICK="$(terraform -chdir=infra/terraform/envs/dev output -raw click_api_url)"
# First click → 302 to destination, one event emitted:
curl -i "$CLICK/click?ad_id=ad_demo_1&impression_id=imp_001"
# Replay same impression → still 302, but NO additional count (idempotency, SC-005):
curl -i "$CLICK/click?ad_id=ad_demo_1&impression_id=imp_001"
# Unknown ad → 404, no count, no redirect (FR-005):
curl -i "$CLICK/click?ad_id=does_not_exist&impression_id=imp_002"
```
Verify exactly one raw record per impression landed in S3 (`raw/dt=.../hr=.../`).

### US2 — Advertiser queries metrics (P2)
```bash
QUERY="$(terraform -chdir=infra/terraform/envs/dev output -raw query_api_url)"
# Demo principal model (research D9): the bearer token value IS the advertiser_id.
TOKEN="adv_demo"
# Within ~1 min of the clicks above, minute-granularity counts appear (SC-003):
curl -s -H "Authorization: Bearer $TOKEN" \
  "$QUERY/metrics?campaign_id=camp_demo&from=2026-06-13T00:00:00Z&to=2026-06-14T00:00:00Z&granularity=minute" | jq
# Hourly rollup of the same window:
curl -s -H "Authorization: Bearer $TOKEN" \
  "$QUERY/metrics?...&granularity=hour" | jq
# Campaign not owned by caller → 403 (FR-009).
```

### US3 — Reconciliation makes counts exact (P3)
```bash
# Optionally inject a deliberate drift into click_aggregates (source='stream'),
# then trigger the Glue job (normally hourly via EventBridge):
aws glue start-job-run --job-name ad-click-reconciliation
# After it completes, re-query: the period's buckets now show source='batch'
# and match counts derived from raw S3 clicks exactly (SC-004 / FR-014).
```

### Load / hot-shard sanity (SC-001 / SC-006)
```bash
cd seeds && ruby click_simulator.rb --rps 500 --hot-ad ad_demo_1 --hot-share 0.5 --duration 60
# Confirm no accepted-click loss: count distinct impression_ids in S3 == accepted clicks.
```

## 5. Tear down

```bash
cd infra/terraform/envs/dev
terraform destroy     # removes ALL resources — required to stop costs
```

## Notes
- Redshift Serverless and ElastiCache bill while running; `destroy` when not demoing.
- Kinesis is on-demand; Glue/Flink bill per run/hour. Keep dev capacity minimal.
- Every behavior above corresponds to a Success Criterion in `spec.md`.
