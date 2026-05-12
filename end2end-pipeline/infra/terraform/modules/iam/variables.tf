variable "name_prefix" {
  description = "Prefix applied to every resource name."
  type        = string
}

variable "github_owner" {
  description = "GitHub org / user owning the repo."
  type        = string
}

variable "github_repo" {
  description = "Repo name, without the owner."
  type        = string
}

variable "allowed_refs" {
  description = <<-EOT
    sub-claim patterns allowed to assume the GHA role. Narrow this — the
    default lets *any* branch plan+apply, which is too loose for prod.
    Examples:
      repo:acme/pipeline:ref:refs/heads/main
      repo:acme/pipeline:environment:prod
  EOT
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Tags merged into every resource."
  type        = map(string)
  default     = {}
}
