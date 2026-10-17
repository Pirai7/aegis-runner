output "container_app_url" {
  description = "Public HTTPS FQDN of the AEGIS Runner deployment"
  value       = "https://${azurerm_container_app.app.ingress[0].fqdn}"
}

output "postgres_fqdn" {
  description = "Fully Qualified Domain Name of PostgreSQL Flexible Server"
  value       = azurerm_postgresql_flexible_server.db.fqdn
}
