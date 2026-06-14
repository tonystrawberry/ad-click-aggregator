# services/ — Ruby service tier

Ruby 3.3 Lambdas plus a shared gem. All service-tier code is Ruby per Constitution
Principle III.

- `shared/` — gem with the entities and AWS wrappers used by both Lambdas:
  `TimeBucket` (UTC minute floors), `ClickEvent` (canonical event + JSON),
  `Aws::DynamoDB` (ads lookup + ownership), `Aws::Kinesis` (salted-key put),
  `Aws::RedisDeduper` (`SET NX` impression dedup).
- `click_processor/` — `GET /click`: validate ad → dedup → emit to Kinesis → 302
  redirect (REFERENCE: click processor + validation + redirect flow).
- `query_service/` — `GET /metrics`: ownership check → Redshift range scan →
  zero-filled, granularity-rolled buckets (REFERENCE: query service).

## Test

```bash
cd services/shared          && bundle exec rspec
cd services/click_processor && bundle exec rspec --tag ~integration
cd services/query_service   && bundle exec rspec --tag ~integration
# Integration (LocalStack + Redis): docker compose -f docker-compose.test.yml up -d
RUN_INTEGRATION=1 bundle exec rspec spec/integration
```
