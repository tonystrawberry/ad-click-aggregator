# module: streaming (User Stories 2 & 3)

The two Kinesis consumers (REFERENCE: Flink aggregation + dump raw to S3).

- Managed Service for Apache Flink application running the aggregator jar; reads
  `click-events`, writes minute aggregates to Redshift (`source='stream'`).
- Kinesis Data Firehose → S3 `raw/dt=/hr=` in Parquet (the reconciliation source).
- Glue Data Catalog database + `click_events` table describing the raw schema,
  used by Firehose format conversion and the Spark job.

Note (educational): the Redshift password is passed to Flink via app properties
for simplicity; harden by reading the secret in-app for production.
