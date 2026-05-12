output "kafka_bootstrap_brokers" {
  value = module.kafka.bootstrap_brokers_sasl_iam
}

output "api_url" {
  value = "http://${module.compute.api_alb_dns_name}"
}

output "grafana_endpoint" {
  value = module.observability.grafana_endpoint
}

output "gha_apply_role_arn" {
  value = module.iam.gha_apply_role_arn
}
