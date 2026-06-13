# Query latency & freshness validation (SC-002, SC-003)

Closes analysis findings G1 (query latency) and G2 (freshness) with explicit
procedures. Requires a deployed stack + seeded data.

## SC-002 — 95% of queries < 1 second (G1)

```bash
QUERY_API=$(terraform -chdir=infra/terraform/envs/dev output -raw query_api_url)
TOKEN=adv_demo   # demo principal == advertiser_id (research D9)

# 100 timed queries over a populated window; report p95.
for i in $(seq 1 100); do
  curl -s -o /dev/null -w "%{time_total}\n" -H "Authorization: Bearer $TOKEN" \
    "$QUERY_API/metrics?campaign_id=camp_demo&from=2026-06-13T00:00:00Z&to=2026-06-14T00:00:00Z&granularity=hour"
done | sort -n | awk '{a[NR]=$1} END{print "p95="a[int(NR*0.95)]"s  max="a[NR]"s"}'
```

Pass: p95 < 1.0s. Redshift `click_aggregates` is keyed/sorted on
`(campaign_id, minute_bucket)` and the aggregate table is small, so range scans
are fast. Record results below.

## SC-003 — new click visible within 1 minute (G2)

```bash
CLICK_API=$(terraform -chdir=infra/terraform/envs/dev output -raw click_api_url)
IMP="freshness-$(date +%s)"
curl -s -o /dev/null "$CLICK_API/click?ad_id=ad_demo_1&impression_id=$IMP"
START=$(date +%s)

# Poll the current-minute bucket until the click appears.
while true; do
  COUNT=$(curl -s -H "Authorization: Bearer adv_demo" \
    "$QUERY_API/metrics?campaign_id=camp_demo&from=$(date -u +%Y-%m-%dT%H:%M:00Z -d '-1 min')&to=$(date -u +%Y-%m-%dT%H:%M:00Z -d '+1 min')&granularity=minute" \
    | grep -o '"click_count":[0-9]*' | head -1)
  [ -n "$COUNT" ] && [ "${COUNT##*:}" -gt 0 ] && break
  sleep 5
done
echo "visible after $(( $(date +%s) - START ))s"
```

Pass: visible in < 60s. Freshness is governed by the Flink window (1 min) +
checkpoint/emit latency.

## Results

| Date | SC-002 p95 | SC-003 latency | Pass | Notes |
|------|-----------|----------------|------|-------|
| _TBD_ | | | | run after deploy |
