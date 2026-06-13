variable "name_prefix" {
  type = string
}

variable "kinesis_stream_arn" {
  type = string
}

variable "kinesis_stream_name" {
  type = string
}

variable "raw_bucket_name" {
  type = string
}

variable "raw_bucket_arn" {
  type = string
}

variable "artifacts_bucket" {
  type = string
}

variable "flink_jar_key" {
  type = string
}

variable "redshift_workgroup" {
  type = string
}

variable "redshift_endpoint" {
  type = string
}

variable "redshift_secret_arn" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}
