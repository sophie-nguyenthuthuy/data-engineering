output "ecs_cluster_name" {
  value = aws_ecs_cluster.this.name
}

output "api_alb_dns_name" {
  description = "DNS name of the API's public ALB."
  value       = aws_lb.api.dns_name
}

output "api_alb_zone_id" {
  description = "Hosted-zone ID for the ALB (use in Route53 alias records)."
  value       = aws_lb.api.zone_id
}

output "producer_task_role_arn" {
  value = aws_iam_role.producer_task.arn
}

output "api_task_role_arn" {
  value = aws_iam_role.api_task.arn
}

output "ecr_producer_url" {
  value = aws_ecr_repository.producer.repository_url
}

output "ecr_api_url" {
  value = aws_ecr_repository.api.repository_url
}

output "task_security_group_id" {
  value = aws_security_group.tasks.id
}
