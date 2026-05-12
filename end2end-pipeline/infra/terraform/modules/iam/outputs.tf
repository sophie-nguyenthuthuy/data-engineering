output "oidc_provider_arn" {
  value = aws_iam_openid_connect_provider.github.arn
}

output "gha_plan_role_arn" {
  description = "Assume this from the PR-triggered plan workflow."
  value       = aws_iam_role.plan.arn
}

output "gha_apply_role_arn" {
  description = "Assume this from the main-branch apply workflow."
  value       = aws_iam_role.apply.arn
}
