variable "name_prefix" {
  type = string
}

variable "ads_table_name" {
  type = string
}

variable "ads_table_arn" {
  type = string
}

variable "ads_gsi_name" {
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

variable "lambda_sg_id" {
  type = string
}

variable "lambda_zip_path" {
  description = "Path to the built query_service.zip (produced by make build-lambdas)."
  type        = string
  default     = "../../../../dist/query_service.zip"
}
