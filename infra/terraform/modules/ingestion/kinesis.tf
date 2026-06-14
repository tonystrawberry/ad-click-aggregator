# Click event stream (research D3). On-demand mode auto-scales shards — no shard
# math for the educational build; the salted partition key spreads hot ads.
resource "aws_kinesis_stream" "click_events" {
  name = "${var.name_prefix}-click-events"

  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }

  retention_period = 24 # hours; raw durability lives in S3 via Firehose

  tags = { Name = "${var.name_prefix}-click-events" }
}
