variable "region" {
  description = "AWS region for all resources in this env."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Short prefix stamped into every resource name. Must be DNS-safe."
  type        = string
  default     = "end2end-dev"
}

variable "github_owner" {
  description = "GitHub org/user that owns the repo. Used for OIDC sub-claim binding."
  type        = string
}

variable "github_repo" {
  description = "GitHub repo name."
  type        = string
  default     = "end2end-pipeline"
}

variable "gha_allowed_refs" {
  description = "Tighter-than-default OIDC sub patterns. Falls back to main + PRs."
  type        = list(string)
  default     = []
}

variable "grafana_admin_sso_ids" {
  description = "AWS SSO user/group IDs for Grafana admins. Leave empty to wire via console."
  type        = list(string)
  default     = []
}
