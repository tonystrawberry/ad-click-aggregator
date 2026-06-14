# Managed Service for Apache Flink application running the stream aggregator
# (research D4). The job jar is uploaded to the artifacts bucket by `make build-flink`.

data "aws_region" "current" {}

data "aws_secretsmanager_secret_version" "redshift" {
  secret_id = var.redshift_secret_arn
}

locals {
  redshift_creds = jsondecode(data.aws_secretsmanager_secret_version.redshift.secret_string)
  redshift_jdbc  = "jdbc:redshift://${var.redshift_endpoint}:5439/${local.redshift_creds.dbname}"
}

data "aws_iam_policy_document" "flink_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["kinesisanalytics.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "flink" {
  name               = "${var.name_prefix}-flink-role"
  assume_role_policy = data.aws_iam_policy_document.flink_assume.json
}

data "aws_iam_policy_document" "flink" {
  statement {
    sid       = "ReadClickStream"
    actions   = ["kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:ListShards", "kinesis:SubscribeToShard"]
    resources = [var.kinesis_stream_arn]
  }
  statement {
    sid       = "ReadJobArtifact"
    actions   = ["s3:GetObject"]
    resources = ["arn:aws:s3:::${var.artifacts_bucket}/*"]
  }
  statement {
    sid       = "VpcEni"
    actions   = ["ec2:CreateNetworkInterface", "ec2:DescribeNetworkInterfaces", "ec2:DeleteNetworkInterface", "ec2:DescribeVpcs", "ec2:DescribeSubnets", "ec2:DescribeSecurityGroups"]
    resources = ["*"]
  }
  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "flink" {
  role   = aws_iam_role.flink.id
  policy = data.aws_iam_policy_document.flink.json
}

resource "aws_security_group" "flink" {
  name        = "${var.name_prefix}-flink-sg"
  description = "Managed Flink app ENIs"
  vpc_id      = var.vpc_id
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_kinesisanalyticsv2_application" "aggregator" {
  name                   = "${var.name_prefix}-flink-aggregator"
  runtime_environment    = "FLINK-1_20"
  service_execution_role = aws_iam_role.flink.arn

  application_configuration {
    application_code_configuration {
      code_content {
        s3_content_location {
          bucket_arn = "arn:aws:s3:::${var.artifacts_bucket}"
          file_key   = var.flink_zip_key
        }
      }
      code_content_type = "ZIPFILE"
    }

    vpc_configuration {
      subnet_ids         = var.private_subnet_ids
      security_group_ids = [aws_security_group.flink.id]
    }

    environment_properties {
      # Tells Managed Flink this is a Python app: entry script + connector jar.
      property_group {
        property_group_id = "kinesis.analytics.flink.run.options"
        property_map = {
          "python"  = "main.py"
          "jarfile" = "lib/flink-sql-connector-kinesis-4.3.0-1.20.jar"
        }
      }
      property_group {
        property_group_id = "FlinkAppProperties"
        # NOTE (educational): Redshift password is passed via app properties for
        # simplicity. A hardened build would read the secret at runtime in-app.
        property_map = {
          "stream.name"       = var.kinesis_stream_name
          "aws.region"        = data.aws_region.current.name
          "scan.initpos"      = "LATEST"
          "redshift.jdbc.url" = local.redshift_jdbc
          "redshift.user"     = local.redshift_creds.username
          "redshift.password" = local.redshift_creds.password
          "sink.batch.size"   = "100"
        }
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type = "DEFAULT"
      }
      monitoring_configuration {
        configuration_type = "DEFAULT"
      }
      parallelism_configuration {
        configuration_type   = "CUSTOM"
        parallelism          = 2
        auto_scaling_enabled = true
      }
    }
  }
}
