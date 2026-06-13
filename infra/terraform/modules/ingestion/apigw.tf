# API Gateway HTTP API exposing GET /click → click-processor Lambda.
resource "aws_apigatewayv2_api" "click" {
  name          = "${var.name_prefix}-click-api"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "click" {
  api_id                 = aws_apigatewayv2_api.click.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.click_processor.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "click" {
  api_id    = aws_apigatewayv2_api.click.id
  route_key = "GET /click"
  target    = "integrations/${aws_apigatewayv2_integration.click.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.click.id
  name        = "$default"
  auto_deploy = true
}

resource "aws_lambda_permission" "apigw" {
  statement_id  = "AllowClickApiInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.click_processor.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.click.execution_arn}/*/*"
}
