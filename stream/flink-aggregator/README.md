# stream/flink-aggregator — Flink stream aggregator

Java + Apache Flink Table API job for Amazon Managed Service for Apache Flink
(REFERENCE: Kinesis → Flink near-real-time aggregation). A documented non-Ruby
exception (Constitution Principle III) — there is no Ruby Flink runtime.

- `ClickAggregatorJob` — Kinesis source (event-time, 30s watermark) → 1-minute
  tumbling window count by `campaign_id`.
- `RedshiftUpsertSink` — batched UPDATE-then-INSERT upsert into Redshift
  `click_aggregates` with `source='stream'`.

Event-time + watermark attribute late/out-of-order clicks to the correct minute
(FR-016); reconciliation corrects anything beyond the watermark.

## Build / test

```bash
mvn test            # MiniCluster windowing test
mvn clean package   # produces target/flink-aggregator-1.0.0.jar (uploaded by make build-flink)
```

Runtime config is read from the Managed Flink "FlinkAppProperties" group
(stream name, region, Redshift JDBC URL + creds) — set by Terraform.
