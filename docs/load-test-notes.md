# Load & hot-shard validation (SC-001, SC-006)

Runbook for verifying no accepted-click loss under load and under a viral-ad spike.
Requires a deployed dev stack (`terraform apply`) and seeded catalog.

## Procedure

```bash
CLICK_API=$(terraform -chdir=infra/terraform/envs/dev output -raw click_api_url)

# Hot-shard scenario: one ad takes 50% of traffic (SC-006).
ruby seeds/click_simulator.rb --url "$CLICK_API" \
  --rps 500 --duration 60 --hot-ad ad_viral_1 --hot-share 0.5
```

The simulator prints `accepted(302)=N`. Each accepted click with a unique
impression_id must appear exactly once in the raw archive.

## No-loss check (Athena over the S3 raw archive)

```sql
-- distinct impressions captured during the run
SELECT COUNT(DISTINCT impression_id) AS captured
FROM clicks.click_events
WHERE dt = '<run-date>';
```

Compare `captured` against the number of DISTINCT impression_ids the simulator
sent (it logs the breakdown). Equal ⇒ no accepted-click loss.

## Notes on scale (SC-001)

Per the spec Assumptions, the 10,000 clicks/sec target is a **sizing target**, not
a load test executed at full scale in this educational build. The salted partition
key (`ad_id:salt`, factor 8) is the mechanism that lets a hot ad fan out across
shards; Kinesis on-demand mode auto-scales shard count. Document observed RPS and
any throttling here after a run.

## Results

| Date | RPS | Hot share | Sent | Accepted | Captured (S3) | Loss | Notes |
|------|-----|-----------|------|----------|---------------|------|-------|
| _TBD_ | | | | | | | run after deploy |
