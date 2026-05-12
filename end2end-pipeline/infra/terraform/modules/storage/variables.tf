variable "name_prefix" {
  description = "Prefix applied to every resource name. Bucket names get a random suffix on top."
  type        = string
}

variable "force_destroy" {
  description = "Allow terraform destroy to delete non-empty buckets. Dev-only."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags merged into every resource."
  type        = map(string)
  default     = {}
}
