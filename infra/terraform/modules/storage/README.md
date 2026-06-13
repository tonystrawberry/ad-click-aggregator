# module: storage

Network + all data stores (REFERENCE: Click DB, OLAP store, Redis cache, raw S3).

- VPC, 2 private subnets, security groups, and VPC endpoints (DynamoDB/S3 gateway;
  Kinesis/Secrets/Redshift-Data interface) — NAT-free for cost (research D10).
- DynamoDB `ads` + `advertiser-campaign-index` GSI (ads catalog, ownership).
- Redshift Serverless namespace/workgroup — OLAP `click_aggregates` store.
- S3 raw-clicks bucket (30-day lifecycle) + artifacts bucket.
- ElastiCache Redis (impression dedup).
- Secrets Manager secret holding Redshift admin credentials.
