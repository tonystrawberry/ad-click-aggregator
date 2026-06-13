variable "name_prefix" {
  type = string
}

variable "ads_table_name" {
  type = string
}

variable "ads_table_arn" {
  type = string
}

variable "redis_endpoint" {
  type = string
}

variable "redis_port" {
  type = number
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

variable "kinesis_salt_factor" {
  type    = number
  default = 8
}

variable "impression_ttl_seconds" {
  type    = number
  default = 172800
}

variable "lambda_zip_path" {
  description = "Path to the built click_processor.zip (produced by make build-lambdas)."
  type        = string
  default     = "../../../../dist/click_processor.zip"
}
