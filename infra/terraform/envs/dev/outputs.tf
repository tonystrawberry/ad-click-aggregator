output "click_api_url" {
  description = "Base URL of the click ingestion API (append /click)."
  value       = module.ingestion.click_api_url
}

output "query_api_url" {
  description = "Base URL of the advertiser metrics API (append /metrics)."
  value       = module.query.query_api_url
}

output "kinesis_stream_name" {
  value = module.ingestion.kinesis_stream_name
}

output "raw_bucket_name" {
  value = module.storage.raw_bucket_name
}

output "artifacts_bucket_name" {
  value = module.storage.artifacts_bucket_name
}

output "redshift_workgroup" {
  value = module.storage.redshift_workgroup_name
}

output "redshift_endpoint" {
  value = module.storage.redshift_endpoint
}

output "reconciliation_job_name" {
  value = module.reconciliation.glue_job_name
}
