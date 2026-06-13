# Kinesis Data Firehose: click-events stream → S3 raw archive in Parquet,
# date/hour partitioned (research D5). Feeds the Spark reconciliation job.

data "aws_iam_policy_document" "firehose_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name               = "${var.name_prefix}-firehose-role"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume.json
}

data "aws_iam_policy_document" "firehose" {
  statement {
    sid       = "ReadClickStream"
    actions   = ["kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream", "kinesis:ListShards"]
    resources = [var.kinesis_stream_arn]
  }
  statement {
    sid       = "WriteRawArchive"
    actions   = ["s3:AbortMultipartUpload", "s3:GetBucketLocation", "s3:GetObject", "s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:PutObject"]
    resources = [var.raw_bucket_arn, "${var.raw_bucket_arn}/*"]
  }
  statement {
    sid       = "GlueSchema"
    actions   = ["glue:GetTable", "glue:GetTableVersion", "glue:GetTableVersions"]
    resources = ["*"]
  }
  statement {
    sid       = "Logs"
    actions   = ["logs:PutLogEvents"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "firehose" {
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose.json
}

resource "aws_kinesis_firehose_delivery_stream" "raw_archive" {
  name        = "${var.name_prefix}-raw-archive"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = var.kinesis_stream_arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = var.raw_bucket_arn

    # ~1 minute / 64 MB buffering keeps the archive close to real time (D5).
    buffering_interval = 60
    buffering_size     = 64

    prefix              = "raw/dt=!{timestamp:yyyy-MM-dd}/hr=!{timestamp:HH}/"
    error_output_prefix = "raw-errors/!{firehose:error-output-type}/dt=!{timestamp:yyyy-MM-dd}/"

    # JSON -> Parquet conversion using the Glue table schema.
    data_format_conversion_configuration {
      input_format_configuration {
        deserializer {
          open_x_json_ser_de {}
        }
      }
      output_format_configuration {
        serializer {
          parquet_ser_de {}
        }
      }
      schema_configuration {
        database_name = aws_glue_catalog_database.clicks.name
        table_name    = aws_glue_catalog_table.click_events.name
        role_arn      = aws_iam_role.firehose.arn
      }
    }
  }
}
