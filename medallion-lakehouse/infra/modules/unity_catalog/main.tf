terraform {
  required_providers {
    databricks = {
      source                = "databricks/databricks"
      configuration_aliases = [databricks.account, databricks]
    }
  }
}

variable "environment" { type = string }
variable "project" { type = string }
variable "metastore_id" { type = string }
variable "workspace_id" { type = string }
variable "admin_group_name" { type = string }
variable "storage_account_id" { type = string }
variable "storage_account_dfs" { type = string }
variable "access_connector_id" { type = string }

locals {
  catalog_name = "${var.environment}_${var.project}"
}

resource "databricks_metastore_assignment" "this" {
  provider     = databricks.account
  workspace_id = var.workspace_id
  metastore_id = var.metastore_id
}

resource "databricks_storage_credential" "this" {
  name = "sc-${local.catalog_name}"
  azure_managed_identity {
    access_connector_id = var.access_connector_id
  }
  depends_on = [databricks_metastore_assignment.this]
}

resource "databricks_external_location" "root" {
  name            = "el-${local.catalog_name}"
  url             = "abfss://metastore@${replace(var.storage_account_dfs, "https://", "")}"
  credential_name = databricks_storage_credential.this.name
}

resource "databricks_catalog" "this" {
  name           = local.catalog_name
  comment        = "Medallion lakehouse catalog for ${var.environment}"
  isolation_mode = "ISOLATED"
  properties = {
    purpose = "medallion_lakehouse"
  }
  depends_on = [databricks_external_location.root]
}

resource "databricks_schema" "layers" {
  for_each     = toset(["bronze", "silver", "gold"])
  catalog_name = databricks_catalog.this.name
  name         = each.key
  comment      = "${each.key} layer"
}

resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.this.name

  grant {
    principal  = var.admin_group_name
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = "account users"
    privileges = ["USE_CATALOG"]
  }
}

resource "databricks_grants" "gold_read" {
  schema = "${databricks_catalog.this.name}.${databricks_schema.layers["gold"].name}"

  grant {
    principal  = "account users"
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

output "catalog_name" { value = databricks_catalog.this.name }
