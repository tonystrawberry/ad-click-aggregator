# ElastiCache Redis for impression-ID de-duplication (research D2).

resource "aws_security_group" "redis" {
  name        = "${var.name_prefix}-redis-sg"
  description = "Redis dedup cache"
  vpc_id      = aws_vpc.main.id

  ingress {
    description     = "Redis from Lambdas"
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [aws_security_group.lambda.id]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.name_prefix}-redis-sg" }
}

resource "aws_elasticache_subnet_group" "redis" {
  name       = "${var.name_prefix}-redis-subnets"
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_elasticache_cluster" "redis" {
  cluster_id           = "${var.name_prefix}-dedup"
  engine               = "redis"
  node_type            = "cache.t4g.micro" # smallest node — cost-aware (Principle VI)
  num_cache_nodes      = 1
  parameter_group_name = "default.redis7"
  engine_version       = "7.1"
  port                 = 6379
  subnet_group_name    = aws_elasticache_subnet_group.redis.name
  security_group_ids   = [aws_security_group.redis.id]
  tags                 = { Name = "${var.name_prefix}-dedup" }
}
