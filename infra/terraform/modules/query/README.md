# module: query (User Story 2)

The advertiser-facing read path (REFERENCE: query service, <1s analytics).

- Query-service Lambda (Ruby 3.3, in-VPC to reach Redshift) with least-privilege
  IAM (DynamoDB Query on the ownership GSI + read the Redshift secret).
- API Gateway HTTP API `GET /metrics` → Lambda.

The Lambda derives `advertiser_id` from the bearer token (demo principal model,
research D9) and scopes every query to campaigns the caller owns (FR-009).
