output "resource_group_name" {
  value = azurerm_resource_group.this.name
}

output "storage_account_name" {
  value = module.storage.account_name
}

output "keyvault_uri" {
  value = module.keyvault.vault_uri
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.this.workspace_url
}

output "databricks_workspace_id" {
  value = azurerm_databricks_workspace.this.workspace_id
}

output "catalog_name" {
  value = module.unity_catalog.catalog_name
}

output "access_connector_id" {
  value = azurerm_databricks_access_connector.this.id
}
