output "query_api_url" {
  value = aws_apigatewayv2_stage.default.invoke_url
}

output "query_function_name" {
  value = aws_lambda_function.query.function_name
}
