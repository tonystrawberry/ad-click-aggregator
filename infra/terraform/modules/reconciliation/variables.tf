variable "name_prefix" {
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

variable "glue_script_key" {
  type = string
}

variable "glue_database" {
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

variable "schedule_expression" {
  type    = string
  default = "rate(1 hour)"
}
