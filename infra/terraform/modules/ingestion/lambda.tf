# Click-processor Lambda (Ruby 3.3), in-VPC so it can reach Redis.

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "click_processor" {
  name               = "${var.name_prefix}-click-processor-role"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

data "aws_iam_policy_document" "click_processor" {
  statement {
    sid       = "ReadAds"
    actions   = ["dynamodb:GetItem"]
    resources = [var.ads_table_arn]
  }
  statement {
    sid       = "WriteClickStream"
    actions   = ["kinesis:PutRecord", "kinesis:PutRecords"]
    resources = [aws_kinesis_stream.click_events.arn]
  }
}

resource "aws_iam_role_policy" "click_processor" {
  role   = aws_iam_role.click_processor.id
  policy = data.aws_iam_policy_document.click_processor.json
}

resource "aws_iam_role_policy_attachment" "click_processor_vpc" {
  role       = aws_iam_role.click_processor.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_lambda_function" "click_processor" {
  function_name    = "${var.name_prefix}-click-processor"
  role             = aws_iam_role.click_processor.arn
  runtime          = "ruby3.3"
  handler          = "handler.ClickProcessor.lambda_handler"
  filename         = var.lambda_zip_path
  source_code_hash = filebase64sha256(var.lambda_zip_path)
  timeout          = 10
  memory_size      = 256

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [var.lambda_sg_id]
  }

  environment {
    variables = {
      ADS_TABLE              = var.ads_table_name
      STREAM_NAME            = aws_kinesis_stream.click_events.name
      REDIS_HOST             = var.redis_endpoint
      REDIS_PORT             = tostring(var.redis_port)
      KINESIS_SALT_FACTOR    = tostring(var.kinesis_salt_factor)
      IMPRESSION_TTL_SECONDS = tostring(var.impression_ttl_seconds)
    }
  }
}
