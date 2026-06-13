output "glue_database_name" {
  value = aws_glue_catalog_database.clicks.name
}

output "glue_table_name" {
  value = aws_glue_catalog_table.click_events.name
}

output "flink_application_name" {
  value = aws_kinesisanalyticsv2_application.aggregator.name
}

output "firehose_name" {
  value = aws_kinesis_firehose_delivery_stream.raw_archive.name
}
