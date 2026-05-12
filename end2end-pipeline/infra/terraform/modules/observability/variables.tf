variable "name_prefix" {
  description = "Prefix applied to every resource name."
  type        = string
}

variable "grafana_admin_role_arns" {
  description = <<-EOT
    IAM role ARNs that map to Grafana admins. Managed Grafana assume-role auth
    means these ARNs get admin in the Grafana workspace. Leave empty to skip
    associations and wire them manually via SSO / IAM Identity Center.
  EOT
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Tags merged into every resource."
  type        = map(string)
  default     = {}
}
