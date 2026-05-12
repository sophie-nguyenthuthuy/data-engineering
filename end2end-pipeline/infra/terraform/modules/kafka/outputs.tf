output "cluster_arn" {
  value = aws_msk_serverless_cluster.this.arn
}

output "cluster_name" {
  value = aws_msk_serverless_cluster.this.cluster_name
}

output "security_group_id" {
  value = aws_security_group.cluster.id
}

output "bootstrap_brokers_sasl_iam" {
  description = "Set this as bootstrap.servers on clients; pair with sasl.mechanism=AWS_MSK_IAM."
  value       = aws_msk_serverless_cluster.this.bootstrap_brokers_sasl_iam
}
