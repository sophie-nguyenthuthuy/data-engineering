locals {
  name_prefix = "${var.project}-${var.environment}"
  common_tags = merge(var.tags, {
    project     = var.project
    environment = var.environment
    managed_by  = "terraform"
  })
}

resource "azurerm_resource_group" "this" {
  name     = "rg-${local.name_prefix}"
  location = var.location
  tags     = local.common_tags
}

module "storage" {
  source = "./modules/storage"

  name_prefix         = local.name_prefix
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  containers          = ["landing", "bronze", "silver", "gold", "checkpoints", "metastore"]
  tags                = local.common_tags
}

module "keyvault" {
  source = "./modules/keyvault"

  name_prefix         = local.name_prefix
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tenant_id           = data.azurerm_client_config.current.tenant_id
  admin_object_ids    = [data.azuread_group.admins.object_id]
  tags                = local.common_tags
}

resource "azurerm_databricks_access_connector" "this" {
  name                = "dbac-${local.name_prefix}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  identity {
    type = "SystemAssigned"
  }
  tags = local.common_tags
}

resource "azurerm_role_assignment" "connector_storage" {
  scope                = module.storage.account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.this.identity[0].principal_id
}

resource "azurerm_databricks_workspace" "this" {
  name                        = "dbw-${local.name_prefix}"
  resource_group_name         = azurerm_resource_group.this.name
  location                    = azurerm_resource_group.this.location
  sku                         = var.environment == "prod" ? "premium" : "premium"
  managed_resource_group_name = "rg-${local.name_prefix}-managed"

  public_network_access_enabled         = length(var.allowed_cidrs) > 0
  network_security_group_rules_required = "NoAzureDatabricksRules"

  tags = local.common_tags
}

module "unity_catalog" {
  source = "./modules/unity_catalog"

  providers = {
    databricks         = databricks
    databricks.account = databricks.account
  }

  environment         = var.environment
  project             = var.project
  metastore_id        = var.metastore_id
  workspace_id        = azurerm_databricks_workspace.this.workspace_id
  admin_group_name    = var.admin_group_name
  storage_account_id  = module.storage.account_id
  storage_account_dfs = module.storage.primary_dfs_endpoint
  access_connector_id = azurerm_databricks_access_connector.this.id
}

data "azurerm_client_config" "current" {}

data "azuread_group" "admins" {
  display_name = var.admin_group_name
}
