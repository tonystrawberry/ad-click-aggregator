# Reconciliation exactness validation (SC-004, FR-014)

Runbook proving reconciliation makes aggregates exact. Requires a deployed stack.

## Procedure

1. Generate clicks for a closed minute/hour (simulator), and let the Flink path
   write approximate `source='stream'` rows to Redshift.

2. Inject deliberate drift to prove the batch path corrects it:
   ```sql
   UPDATE click_aggregates
      SET click_count = click_count + 999, source = 'stream'
    WHERE campaign_id = 'camp_demo' AND minute_bucket = '<closed-minute>';
   ```

3. Run reconciliation for the period (normally hourly via EventBridge):
   ```bash
   aws glue start-job-run --job-name "$(terraform -chdir=infra/terraform/envs/dev output -raw reconciliation_job_name)" \
     --arguments '{"--period_start":"<hour-start>","--period_end":"<hour-end>"}'
   ```

4. Verify the period now matches the raw-derived counts exactly:
   ```sql
   -- Redshift aggregate (should be source='batch')
   SELECT campaign_id, minute_bucket, click_count, source
     FROM click_aggregates
    WHERE minute_bucket >= '<hour-start>' AND minute_bucket < '<hour-end>';
   ```
   ```sql
   -- Ground truth from raw S3 (Athena), dedup by impression_id
   SELECT campaign_id, minute_bucket, COUNT(DISTINCT impression_id) AS exact
     FROM clicks.click_events
    WHERE minute_bucket >= '<hour-start>' AND minute_bucket < '<hour-end>'
    GROUP BY campaign_id, minute_bucket;
   ```

Expected: every reconciled row has `source='batch'` and `click_count == exact`
(0% discrepancy, SC-004). The injected +999 drift is gone.

## Results

| Date | Period | Pre (stream) | Exact (raw) | Post (batch) | Match | Notes |
|------|--------|--------------|-------------|--------------|-------|-------|
| _TBD_ | | | | | | run after deploy |
