# DynamoDB Contract — `ads` (ads catalog + advertiser ownership)

The hot-path lookup for click processing and the ownership source for the query service.

## Table: `ads`

| Attribute | Type | Key | Notes |
|-----------|------|-----|-------|
| `ad_id` | S | **PK** | Clicked ad identifier. |
| `campaign_id` | S | — | Aggregation grouping key. |
| `advertiser_id` | S | — | Owner principal. |
| `destination_url` | S | — | Absolute `https://` URL; redirect target. |
| `active` | BOOL | — | Inactive/unknown ads are rejected (FR-005). |
| `created_at` | S | — | ISO8601 UTC. |

- **Billing**: `PAY_PER_REQUEST`.
- **PITR**: enabled (cheap insurance for the catalog).

## GSI: `advertiser-campaign-index`

| Attribute | Key | Notes |
|-----------|-----|-------|
| `advertiser_id` | **PK** | The authenticated caller. |
| `campaign_id` | **SK** | Lists owned campaigns; backs ownership check (FR-009). |
| Projection | KEYS_ONLY | Ownership check needs keys only. |

## Access patterns

1. **Click processor** — `GetItem(ad_id)` → `{campaign_id, advertiser_id, destination_url, active}`.
   Single-digit-ms point read on the hot path. Reject if missing or `active=false`.
2. **Query service** — ownership check: `Query(advertiser-campaign-index, advertiser_id, campaign_id)`
   returns ≥1 item iff the caller owns the campaign; otherwise `403`.

## Invariants

- `destination_url` MUST be absolute and `https://` (open-redirect guard).
- `campaign_id`, `advertiser_id` required and treated as immutable per ad.
- Seed data (advertisers, campaigns, ads) is loaded via `seeds/` for demos.
