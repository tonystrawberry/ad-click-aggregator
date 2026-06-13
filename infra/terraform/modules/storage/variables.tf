variable "name_prefix" {
  description = "Prefix for all resource names (e.g. ad-click-dev)."
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.20.0.0/16"
}

variable "private_subnet_cidrs" {
  description = "CIDRs for the two private subnets (one per AZ)."
  type        = list(string)
  default     = ["10.20.1.0/24", "10.20.2.0/24"]
}
