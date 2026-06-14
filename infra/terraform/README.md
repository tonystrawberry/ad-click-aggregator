# infra/terraform — Infrastructure as Code

All AWS resources for the aggregator (Constitution Principle II: Terraform-only).

- `envs/dev/` — root module wiring everything together; local backend, dev tfvars.
- `modules/storage` — VPC (+ VPC endpoints, no NAT), DynamoDB ads catalog,
  Redshift Serverless, S3 raw + artifacts buckets, ElastiCache Redis, secrets.
- `modules/ingestion` — Kinesis stream, click-processor Lambda, click API (US1).
- `modules/streaming` — Managed Flink app, Firehose→S3, Glue catalog (US2/US3).
- `modules/query` — query-service Lambda + metrics API (US2).
- `modules/reconciliation` — Glue PySpark job + hourly EventBridge schedule (US3).

```bash
cd envs/dev
terraform init
terraform plan -var-file=dev.tfvars
terraform apply -var-file=dev.tfvars
terraform destroy -var-file=dev.tfvars   # cost discipline — tear down when idle
```

Lambda zips (`dist/*.zip`) and the Flink jar / Glue script must be built and
uploaded first — see the root `Makefile` and `quickstart.md`.
