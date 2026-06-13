output "click_api_url" {
  value = aws_apigatewayv2_stage.default.invoke_url
}

output "kinesis_stream_name" {
  value = aws_kinesis_stream.click_events.name
}

output "kinesis_stream_arn" {
  value = aws_kinesis_stream.click_events.arn
}
