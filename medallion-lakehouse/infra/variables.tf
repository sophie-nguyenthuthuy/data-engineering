variable "environment" {
  description = "Deployment environment (dev, staging, prod)."
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of dev, staging, prod."
  }
}

variable "location" {
  description = "Azure region."
  type        = string
  default     = "westeurope"
}

variable "project" {
  description = "Short project slug used in resource names."
  type        = string
  default     = "medallion"
  validation {
    condition     = can(regex("^[a-z0-9]{3,12}$", var.project))
    error_message = "project must be 3-12 lowercase alphanumeric chars."
  }
}

variable "databricks_account_id" {
  description = "Databricks account GUID (from https://accounts.azuredatabricks.net)."
  type        = string
}

variable "metastore_id" {
  description = "Unity Catalog metastore id for this region. Create once per region out-of-band."
  type        = string
}

variable "admin_group_name" {
  description = "Azure AD group that owns this workspace + catalog."
  type        = string
}

variable "allowed_cidrs" {
  description = "CIDRs allowed to reach the workspace control plane (empty = Azure-only private)."
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Tags applied to every resource."
  type        = map(string)
  default     = {}
}
