variable "region" {
  type    = string
  default = "us-east-1"
}

variable "name_prefix" {
  type    = string
  default = "end2end-prod"
}

variable "github_owner" {
  type = string
}

variable "github_repo" {
  type    = string
  default = "end2end-pipeline"
}

variable "gha_allowed_refs" {
  description = "Prod should be narrow — main-only apply, tagged-release apply, etc."
  type        = list(string)
  default     = []
}

variable "grafana_admin_sso_ids" {
  type    = list(string)
  default = []
}
