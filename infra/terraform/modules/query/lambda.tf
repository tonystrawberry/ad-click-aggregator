# Query-service Lambda (Ruby 3.3), in-VPC to reach Redshift Serverless.

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "query" {
  name               = "${var.name_prefix}-query-role"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

data "aws_secretsmanager_secret_version" "redshift" {
  secret_id = var.redshift_secret_arn
}

locals {
  redshift_creds = jsondecode(data.aws_secretsmanager_secret_version.redshift.secret_string)
}

data "aws_iam_policy_document" "query" {
  statement {
    sid       = "OwnershipCheck"
    actions   = ["dynamodb:Query"]
    resources = ["${var.ads_table_arn}/index/${var.ads_gsi_name}"]
  }
  statement {
    sid       = "ReadRedshiftSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.redshift_secret_arn]
  }
}

resource "aws_iam_role_policy" "query" {
  role   = aws_iam_role.query.id
  policy = data.aws_iam_policy_document.query.json
}

resource "aws_iam_role_policy_attachment" "query_vpc" {
  role       = aws_iam_role.query.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_lambda_function" "query" {
  function_name    = "${var.name_prefix}-query-service"
  role             = aws_iam_role.query.arn
  runtime          = "ruby3.3"
  architectures    = ["arm64"]
  handler          = "handler.QueryService.lambda_handler"
  filename         = var.lambda_zip_path
  source_code_hash = filebase64sha256(var.lambda_zip_path)
  timeout          = 15
  memory_size      = 256

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [var.lambda_sg_id]
  }

  environment {
    variables = {
      ADS_TABLE         = var.ads_table_name
      ADS_GSI           = var.ads_gsi_name
      REDSHIFT_HOST     = var.redshift_endpoint
      REDSHIFT_PORT     = "5439"
      REDSHIFT_DB       = local.redshift_creds.dbname
      REDSHIFT_USER     = local.redshift_creds.username
      REDSHIFT_PASSWORD = local.redshift_creds.password
      # Make `require "bundler/setup"` find the vendored gems packaged in the zip.
      BUNDLE_GEMFILE = "/var/task/Gemfile"
      BUNDLE_PATH    = "/var/task/vendor/bundle"
      BUNDLE_WITHOUT = "development:test"
    }
  }
}
