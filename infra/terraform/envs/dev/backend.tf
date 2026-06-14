# Local backend for the educational build. Switch to an S3 backend with DynamoDB
# state locking for any shared/production use.
terraform {
  backend "local" {
    path = "terraform.tfstate"
  }
}
