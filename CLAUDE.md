<!-- SPECKIT START -->
Active feature: **001-ad-click-aggregator** (educational ad click aggregator per REFERENCE.md).

For technologies, project structure, contracts, and commands, read the current plan and its
design artifacts:
- Plan: `specs/001-ad-click-aggregator/plan.md`
- Research/decisions: `specs/001-ad-click-aggregator/research.md`
- Data model: `specs/001-ad-click-aggregator/data-model.md`
- Contracts: `specs/001-ad-click-aggregator/contracts/`
- Quickstart: `specs/001-ad-click-aggregator/quickstart.md`

Stack at a glance: Ruby 3.3 Lambdas (click processor, query service) behind API Gateway →
Kinesis Data Streams → Managed Service for Apache Flink (1-min aggregates → Redshift) +
Firehose→S3 raw archive; advertiser queries served from Redshift Serverless; ads catalog in
DynamoDB; impression dedup in ElastiCache Redis; hourly Spark (Glue) reconciliation. All
infra in Terraform (`infra/terraform/`). Constitution: `.specify/memory/constitution.md`.
<!-- SPECKIT END -->
