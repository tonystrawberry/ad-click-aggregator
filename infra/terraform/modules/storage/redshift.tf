# Redshift Serverless — the OLAP aggregate store (research D6).
# Admin credentials are generated and stored in Secrets Manager; Flink, the query
# service, and the Glue job all read them from there.

resource "random_password" "redshift_admin" {
  length  = 24
  special = false
}

resource "aws_secretsmanager_secret" "redshift" {
  name                    = "${var.name_prefix}-redshift-admin"
  recovery_window_in_days = 0 # educational: allow immediate delete on destroy
}

resource "aws_secretsmanager_secret_version" "redshift" {
  secret_id = aws_secretsmanager_secret.redshift.id
  secret_string = jsonencode({
    username = "adclick_admin"
    password = random_password.redshift_admin.result
    dbname   = "adclick"
  })
}

resource "aws_security_group" "redshift" {
  name        = "${var.name_prefix}-redshift-sg"
  description = "Redshift Serverless"
  vpc_id      = aws_vpc.main.id

  ingress {
    description     = "Redshift from Lambdas"
    from_port       = 5439
    to_port         = 5439
    protocol        = "tcp"
    security_groups = [aws_security_group.lambda.id]
  }
  ingress {
    description = "Redshift from within VPC (Flink/Glue ENIs)"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.name_prefix}-redshift-sg" }
}

resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = "${var.name_prefix}-ns"
  db_name             = "adclick"
  admin_username      = "adclick_admin"
  admin_user_password = random_password.redshift_admin.result
}

resource "aws_redshiftserverless_workgroup" "main" {
  namespace_name = aws_redshiftserverless_namespace.main.namespace_name
  workgroup_name = "${var.name_prefix}-wg"
  base_capacity  = 8 # smallest RPU footprint for the demo
  subnet_ids     = aws_subnet.private[*].id

  security_group_ids = [aws_security_group.redshift.id]

  publicly_accessible = false
}
