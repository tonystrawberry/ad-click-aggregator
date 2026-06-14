# Raw click archive (Firehose target, reconciliation source) + build artifacts bucket.

resource "aws_s3_bucket" "raw" {
  bucket        = "${var.name_prefix}-raw-clicks"
  force_destroy = true # educational: allow terraform destroy to clean up
  tags          = { Name = "${var.name_prefix}-raw-clicks" }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    id     = "expire-raw"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    expiration {
      days = 30 # retain enough for query windows + reconciliation (research D6 assumption)
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "artifacts" {
  bucket        = "${var.name_prefix}-artifacts"
  force_destroy = true
  tags          = { Name = "${var.name_prefix}-artifacts" }
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
