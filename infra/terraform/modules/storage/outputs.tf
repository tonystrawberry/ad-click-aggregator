output "vpc_id" {
  value = aws_vpc.main.id
}

output "private_subnet_ids" {
  value = aws_subnet.private[*].id
}

output "lambda_sg_id" {
  value = aws_security_group.lambda.id
}

output "ads_table_name" {
  value = aws_dynamodb_table.ads.name
}

output "ads_table_arn" {
  value = aws_dynamodb_table.ads.arn
}

output "ads_gsi_name" {
  value = "advertiser-campaign-index"
}

output "raw_bucket_name" {
  value = aws_s3_bucket.raw.bucket
}

output "raw_bucket_arn" {
  value = aws_s3_bucket.raw.arn
}

output "artifacts_bucket_name" {
  value = aws_s3_bucket.artifacts.bucket
}

output "redis_endpoint" {
  value = aws_elasticache_cluster.redis.cache_nodes[0].address
}

output "redis_port" {
  value = aws_elasticache_cluster.redis.cache_nodes[0].port
}

output "redshift_workgroup_name" {
  value = aws_redshiftserverless_workgroup.main.workgroup_name
}

output "redshift_endpoint" {
  value = aws_redshiftserverless_workgroup.main.endpoint[0].address
}

output "redshift_secret_arn" {
  value = aws_secretsmanager_secret.redshift.arn
}
