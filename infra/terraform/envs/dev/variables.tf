variable "region" {
  description = "AWS region for the dev stack."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name, used in tags and resource names."
  type        = string
  default     = "dev"
}

variable "name_prefix" {
  description = "Prefix applied to all resource names."
  type        = string
  default     = "ad-click"
}

variable "kinesis_salt_factor" {
  description = "Number of salt buckets appended to the Kinesis partition key (hot-shard mitigation, research D3)."
  type        = number
  default     = 8
}

variable "impression_ttl_seconds" {
  description = "Redis dedup TTL for impression IDs (research D2)."
  type        = number
  default     = 172800 # 48h
}

variable "reconciliation_schedule" {
  description = "EventBridge schedule expression for the Glue reconciliation job (research D8)."
  type        = string
  default     = "rate(1 hour)"
}

variable "flink_jar_key" {
  description = "S3 key of the Flink fat jar within the artifacts bucket."
  type        = string
  default     = "flink/flink-aggregator-1.0.0.jar"
}

variable "glue_script_key" {
  description = "S3 key of the Glue PySpark script within the artifacts bucket."
  type        = string
  default     = "glue/job.py"
}
