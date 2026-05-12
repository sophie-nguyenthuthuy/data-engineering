output "db_endpoint" {
  description = "Host:port for the Dagster metadata DB."
  value       = aws_db_instance.dagster.endpoint
}

output "db_address" {
  value = aws_db_instance.dagster.address
}

output "db_port" {
  value = aws_db_instance.dagster.port
}

output "db_name" {
  value = aws_db_instance.dagster.db_name
}

output "db_security_group_id" {
  value = aws_security_group.db.id
}

output "db_secret_arn" {
  description = "Secrets Manager ARN holding {username, password, dbname}."
  value       = aws_secretsmanager_secret.db.arn
}
