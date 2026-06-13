# module: ingestion (User Story 1)

The click capture path (REFERENCE: click processor, Kinesis stream, hot shard).

- Kinesis `click-events` stream (on-demand; salted partition key set by the Lambda).
- Click-processor Lambda (Ruby 3.3, in-VPC) with least-privilege IAM
  (DynamoDB GetItem + Kinesis PutRecord).
- API Gateway HTTP API `GET /click` → Lambda (302 redirect passthrough).

Outputs the Kinesis stream ARN/name consumed by the streaming module.
