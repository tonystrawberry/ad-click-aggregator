# Glue PySpark reconciliation job + hourly EventBridge schedule (research D8).

data "aws_region" "current" {}

data "aws_secretsmanager_secret_version" "redshift" {
  secret_id = var.redshift_secret_arn
}

locals {
  redshift_creds = jsondecode(data.aws_secretsmanager_secret_version.redshift.secret_string)
  redshift_jdbc  = "jdbc:redshift://${var.redshift_endpoint}:5439/${local.redshift_creds.dbname}"
}

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.name_prefix}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

data "aws_iam_policy_document" "glue" {
  statement {
    sid       = "ReadRawAndScript"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [var.raw_bucket_arn, "${var.raw_bucket_arn}/*", "arn:aws:s3:::${var.artifacts_bucket}", "arn:aws:s3:::${var.artifacts_bucket}/*"]
  }
  statement {
    sid       = "RedshiftDataApi"
    actions   = ["redshift-data:ExecuteStatement", "redshift-data:DescribeStatement", "redshift-serverless:GetCredentials"]
    resources = ["*"]
  }
  statement {
    sid       = "ReadSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.redshift_secret_arn]
  }
  statement {
    sid       = "GlueCatalog"
    actions   = ["glue:GetTable", "glue:GetPartitions", "glue:GetDatabase"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "glue" {
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_glue_job" "reconciliation" {
  name              = "${var.name_prefix}-reconciliation"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${var.artifacts_bucket}/${var.glue_script_key}"
    python_version  = "3"
  }

  default_arguments = {
    "--raw_path"      = "s3://${var.raw_bucket_name}/raw/"
    "--redshift_jdbc" = local.redshift_jdbc
    "--secret_arn"    = var.redshift_secret_arn
    "--region"        = data.aws_region.current.name
    "--workgroup"     = var.redshift_workgroup
    # period_start/period_end are supplied per-run by the EventBridge target
    # (previous closed hour). Defaults here are placeholders for manual runs.
    "--period_start"                     = "1970-01-01T00:00:00Z"
    "--period_end"                       = "1970-01-01T01:00:00Z"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"
  }
}

# --- Hourly schedule --------------------------------------------------------

resource "aws_scheduler_schedule" "reconciliation" {
  name = "${var.name_prefix}-reconciliation-schedule"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = var.schedule_expression

  target {
    arn      = "arn:aws:scheduler:::aws-sdk:glue:startJobRun"
    role_arn = aws_iam_role.scheduler.arn

    # Reconcile the previous closed hour. <aws.scheduler.scheduled-time> is the
    # firing time; the job reads [start, end) = [prev hour, this hour).
    input = jsonencode({
      JobName = aws_glue_job.reconciliation.name
      Arguments = {
        "--period_start" = "<aws.scheduler.scheduled-time>"
      }
    })
  }
}

data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${var.name_prefix}-recon-scheduler-role"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}

resource "aws_iam_role_policy" "scheduler" {
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["glue:StartJobRun"]
      Resource = "*"
    }]
  })
}
