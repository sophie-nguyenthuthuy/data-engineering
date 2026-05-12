variable "name_prefix" { type = string }
variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "tenant_id" { type = string }
variable "admin_object_ids" { type = list(string) }
variable "tags" { type = map(string) }

resource "random_string" "suffix" {
  length  = 6
  lower   = true
  numeric = true
  upper   = false
  special = false
}

resource "azurerm_key_vault" "this" {
  name                = substr("kv-${var.name_prefix}-${random_string.suffix.result}", 0, 24)
  resource_group_name = var.resource_group_name
  location            = var.location
  tenant_id           = var.tenant_id
  sku_name            = "standard"

  enable_rbac_authorization     = true
  purge_protection_enabled      = true
  soft_delete_retention_days    = 30
  public_network_access_enabled = false

  network_acls {
    default_action = "Deny"
    bypass         = "AzureServices"
  }

  tags = var.tags
}

resource "azurerm_role_assignment" "admins" {
  for_each             = toset(var.admin_object_ids)
  scope                = azurerm_key_vault.this.id
  role_definition_name = "Key Vault Administrator"
  principal_id         = each.key
}

output "vault_id" { value = azurerm_key_vault.this.id }
output "vault_uri" { value = azurerm_key_vault.this.vault_uri }
