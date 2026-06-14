# module: reconciliation (User Story 3)

Periodic exactness (REFERENCE: Spark reconciliation, Lambda/Kappa correction).

- AWS Glue PySpark job running `batch/reconciliation/job.py` (script from the
  artifacts bucket) with IAM to read raw S3, the Glue catalog, and write Redshift
  via the Redshift Data API + secret.
- EventBridge Scheduler firing hourly; each run reconciles the previous closed
  hour, overwriting `click_aggregates` for that period with `source='batch'`.

After a run, advertiser queries for the period return exact counts (FR-014/SC-004).
