resource "random_password" "db" {
  length           = 32
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>?"
}

resource "aws_secretsmanager_secret" "db" {
  name                    = "${var.name_prefix}/rds/dagster"
  description             = "Master password for the Dagster metadata Postgres instance."
  recovery_window_in_days = 0
  tags                    = var.tags
}

resource "aws_secretsmanager_secret_version" "db" {
  secret_id = aws_secretsmanager_secret.db.id
  secret_string = jsonencode({
    username = var.db_username
    password = random_password.db.result
    dbname   = var.db_name
  })
}

resource "aws_db_subnet_group" "this" {
  name       = "${var.name_prefix}-dagster"
  subnet_ids = var.private_subnet_ids
  tags       = merge(var.tags, { Name = "${var.name_prefix}-dagster" })
}

resource "aws_security_group" "db" {
  name        = "${var.name_prefix}-rds"
  description = "RDS Postgres — Dagster metadata; reachable from within VPC."
  vpc_id      = var.vpc_id
  tags        = merge(var.tags, { Name = "${var.name_prefix}-rds" })
}

resource "aws_vpc_security_group_ingress_rule" "db" {
  security_group_id = aws_security_group.db.id
  description       = "Postgres from within the VPC"
  cidr_ipv4         = var.vpc_cidr_block
  from_port         = 5432
  to_port           = 5432
  ip_protocol       = "tcp"
}

resource "aws_db_instance" "dagster" {
  identifier              = "${var.name_prefix}-dagster"
  engine                  = "postgres"
  engine_version          = "16.4"
  instance_class          = var.instance_class
  allocated_storage       = var.allocated_storage_gb
  storage_type            = "gp3"
  storage_encrypted       = true
  db_name                 = var.db_name
  username                = var.db_username
  password                = random_password.db.result
  db_subnet_group_name    = aws_db_subnet_group.this.name
  vpc_security_group_ids  = [aws_security_group.db.id]
  publicly_accessible     = false
  backup_retention_period = 7
  skip_final_snapshot     = var.skip_final_snapshot
  # Managed password rotation would add a rotation Lambda; leaving it out in
  # favor of the Secrets Manager entry we write here — simpler blast radius.
  performance_insights_enabled = true
  deletion_protection          = !var.skip_final_snapshot
  tags                         = var.tags
}
