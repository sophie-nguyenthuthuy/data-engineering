output "amp_workspace_id" {
  value = aws_prometheus_workspace.this.id
}

output "amp_endpoint" {
  description = "Remote-write endpoint for the OTel Collector."
  value       = aws_prometheus_workspace.this.prometheus_endpoint
}

output "grafana_endpoint" {
  description = "Grafana workspace URL (requires SSO sign-in)."
  value       = aws_grafana_workspace.this.endpoint
}

output "grafana_role_arn" {
  value = aws_iam_role.grafana.arn
}
