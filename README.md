# Ad Click Aggregator

An **educational** implementation of the Hello Interview "Ad Click Aggregator"
system design (`REFERENCE.md`), built end to end on AWS with Terraform.

Users click ads → get redirected to advertisers → clicks are captured,
de-duplicated, streamed, aggregated, and reconciled → advertisers query
campaign click metrics over time at minute granularity in under a second.

## Stack

| Concern | Technology |
|---------|-----------|
| Ingest + query APIs | API Gateway (HTTP API) |
| Services | AWS Lambda — **Ruby 3.3** (click processor, query service) |
| Idempotency / cache | ElastiCache **Redis** |
| Ads catalog | **DynamoDB** |
| Event stream | **Kinesis Data Streams** (salted key for hot shards) |
| Stream aggregation | Managed Service for **Apache Flink** (Java) |
| Raw archive | Kinesis **Firehose → S3** (Parquet) |
| OLAP aggregates | **Redshift Serverless** |
| Reconciliation | **Spark** on **AWS Glue** (PySpark), hourly |
| Infra | **Terraform** |

See [docs/architecture.md](docs/architecture.md) for the pipeline diagram and the
mapping back to `REFERENCE.md`.

## Layout

```
infra/terraform/   IaC — modules: storage, ingestion, streaming, query, reconciliation
services/          Ruby Lambdas (click_processor, query_service) + shared gem
stream/            Java Flink aggregator
batch/             PySpark Glue reconciliation job
seeds/             sample catalog + click simulator
specs/             Spec Kit spec, plan, research, data-model, contracts, tasks
docs/              architecture + validation notes
```

## Quickstart

Full provision → seed → demo → destroy flow:
[specs/001-ad-click-aggregator/quickstart.md](specs/001-ad-click-aggregator/quickstart.md).

```bash
make test          # Ruby + Spark unit tests (locally runnable)
make tf-validate   # terraform fmt -check + validate
```

## Project governance

This project is developed via Spec Kit; the engineering principles (reference
fidelity, Terraform-only infra, Ruby-first, data integrity, managed-services-first,
educational clarity) are in [.specify/memory/constitution.md](.specify/memory/constitution.md).
