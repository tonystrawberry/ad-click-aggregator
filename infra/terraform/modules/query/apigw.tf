# API Gateway HTTP API exposing GET /metrics → query-service Lambda.
# The Lambda derives advertiser_id from the bearer token itself (demo principal
# model, research D9); a production build would attach a JWT authorizer here.
resource "aws_apigatewayv2_api" "query" {
  name          = "${var.name_prefix}-query-api"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "query" {
  api_id                 = aws_apigatewayv2_api.query.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.query.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "metrics" {
  api_id    = aws_apigatewayv2_api.query.id
  route_key = "GET /metrics"
  target    = "integrations/${aws_apigatewayv2_integration.query.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.query.id
  name        = "$default"
  auto_deploy = true
}

resource "aws_lambda_permission" "apigw" {
  statement_id  = "AllowQueryApiInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.query.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.query.execution_arn}/*/*"
}
