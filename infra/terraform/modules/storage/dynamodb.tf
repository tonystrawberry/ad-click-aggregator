# Ads catalog + advertiser ownership (contracts/dynamodb-ads.md).
resource "aws_dynamodb_table" "ads" {
  name         = "${var.name_prefix}-ads"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "ad_id"

  attribute {
    name = "ad_id"
    type = "S"
  }
  attribute {
    name = "advertiser_id"
    type = "S"
  }
  attribute {
    name = "campaign_id"
    type = "S"
  }

  global_secondary_index {
    name            = "advertiser-campaign-index"
    hash_key        = "advertiser_id"
    range_key       = "campaign_id"
    projection_type = "KEYS_ONLY"
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = { Name = "${var.name_prefix}-ads" }
}
