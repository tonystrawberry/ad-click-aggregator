# Root module for the dev environment. Wires the pipeline modules together:
# storage (network + data stores) → ingestion (US1) → streaming (US2/US3) →
# query (US2) → reconciliation (US3). See specs/001-ad-click-aggregator/plan.md.

locals {
  name = "${var.name_prefix}-${var.environment}"
}

module "storage" {
  source      = "../../modules/storage"
  name_prefix = local.name
}

module "ingestion" {
  source = "../../modules/ingestion"

  name_prefix            = local.name
  ads_table_name         = module.storage.ads_table_name
  ads_table_arn          = module.storage.ads_table_arn
  redis_endpoint         = module.storage.redis_endpoint
  redis_port             = module.storage.redis_port
  vpc_id                 = module.storage.vpc_id
  private_subnet_ids     = module.storage.private_subnet_ids
  lambda_sg_id           = module.storage.lambda_sg_id
  kinesis_salt_factor    = var.kinesis_salt_factor
  impression_ttl_seconds = var.impression_ttl_seconds
}

module "streaming" {
  source = "../../modules/streaming"

  name_prefix         = local.name
  kinesis_stream_arn  = module.ingestion.kinesis_stream_arn
  kinesis_stream_name = module.ingestion.kinesis_stream_name
  raw_bucket_name     = module.storage.raw_bucket_name
  raw_bucket_arn      = module.storage.raw_bucket_arn
  artifacts_bucket    = module.storage.artifacts_bucket_name
  flink_zip_key       = var.flink_zip_key
  redshift_workgroup  = module.storage.redshift_workgroup_name
  redshift_endpoint   = module.storage.redshift_endpoint
  redshift_secret_arn = module.storage.redshift_secret_arn
  vpc_id              = module.storage.vpc_id
  private_subnet_ids  = module.storage.private_subnet_ids
}

module "query" {
  source = "../../modules/query"

  name_prefix         = local.name
  ads_table_name      = module.storage.ads_table_name
  ads_table_arn       = module.storage.ads_table_arn
  ads_gsi_name        = module.storage.ads_gsi_name
  redshift_workgroup  = module.storage.redshift_workgroup_name
  redshift_endpoint   = module.storage.redshift_endpoint
  redshift_secret_arn = module.storage.redshift_secret_arn
  vpc_id              = module.storage.vpc_id
  private_subnet_ids  = module.storage.private_subnet_ids
  lambda_sg_id        = module.storage.lambda_sg_id
}

module "reconciliation" {
  source = "../../modules/reconciliation"

  name_prefix         = local.name
  raw_bucket_name     = module.storage.raw_bucket_name
  raw_bucket_arn      = module.storage.raw_bucket_arn
  artifacts_bucket    = module.storage.artifacts_bucket_name
  glue_script_key     = var.glue_script_key
  glue_database       = module.streaming.glue_database_name
  redshift_workgroup  = module.storage.redshift_workgroup_name
  redshift_endpoint   = module.storage.redshift_endpoint
  redshift_secret_arn = module.storage.redshift_secret_arn
  schedule_expression = var.reconciliation_schedule
}
