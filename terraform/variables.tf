variable "project_name" {
  description = "Project name prefix for resources"
  type        = string
  default     = "aegis"
}

variable "location" {
  description = "Azure region for deployment"
  type        = string
  default     = "centralindia"
}

variable "environment" {
  description = "Deployment stage"
  type        = string
  default     = "prod"
}

variable "db_admin_user" {
  description = "PostgreSQL administrator login"
  type        = string
  default     = "aegisadmin"
}

variable "db_admin_password" {
  description = "PostgreSQL administrator password"
  type        = string
  sensitive   = true
  default     = "P@ssw0rdAegis2026!"
}

variable "container_image" {
  description = "Container image URI"
  type        = string
  default     = "mcr.microsoft.com/azuredocs/aci-helloworld:latest"
}

variable "worker_concurrency" {
  description = "Number of worker threads inside container"
  type        = number
  default     = 10
}
