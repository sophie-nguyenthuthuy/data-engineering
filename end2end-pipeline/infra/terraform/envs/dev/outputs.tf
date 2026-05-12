output "vpc_id" {
  value = module.network.vpc_id
}

output "kafka_bootstrap_brokers" {
  description = "Set this as BROKERS on producer/consumer services."
  value       = module.kafka.bootstrap_brokers_sasl_iam
}

output "raw_bucket" {
  value = module.storage.raw_bucket_id
}

output "analysis_bucket" {
  value = module.storage.analysis_bucket_id
}

output "glue_database" {
  value = module.storage.glue_database_name
}

output "dagster_db_endpoint" {
  description = "Postgres host:port for Dagster."
  value       = module.analytics.db_endpoint
}

output "dagster_db_secret_arn" {
  value = module.analytics.db_secret_arn
}

output "api_url" {
  description = "Public ALB for the FastAPI service."
  value       = "http://${module.compute.api_alb_dns_name}"
}

output "ecr_producer" {
  value = module.compute.ecr_producer_url
}

output "ecr_api" {
  value = module.compute.ecr_api_url
}

output "amp_endpoint" {
  value = module.observability.amp_endpoint
}

output "grafana_endpoint" {
  value = module.observability.grafana_endpoint
}

output "gha_plan_role_arn" {
  value = module.iam.gha_plan_role_arn
}

output "gha_apply_role_arn" {
  value = module.iam.gha_apply_role_arn
}
