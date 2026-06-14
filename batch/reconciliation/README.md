# batch/reconciliation — Spark reconciliation job

PySpark job for AWS Glue (REFERENCE: Spark batch reconciliation; the Lambda/Kappa
reconciliation step). A documented non-Ruby exception (Constitution Principle III).

`job.py`:
- `recompute(df)` — **pure** transform: de-duplicate raw clicks by `impression_id`,
  count per `(campaign_id, minute_bucket)`. Unit-tested without AWS.
- `_swap_into_redshift` — stage exact counts, then transactionally DELETE the
  period and INSERT with `source='batch'` (authoritative, exact — SC-004/FR-014).

Runs hourly via EventBridge, reprocessing the previous closed hour from the S3 raw
archive. Reprocessing whole closed periods makes it idempotent and absorbs late
arrivals (FR-016).

## Test

```bash
pip install -r requirements-dev.txt
python -m pytest -q        # exercises recompute() on local Spark
```
