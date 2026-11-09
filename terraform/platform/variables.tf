variable "image" {
  description = "Receiver and transmitter image. Load it into kind before the pods start."
  type        = string
  default     = "transfer:local"
}

variable "postgres_password" {
  description = "Local Postgres password. Use URL-safe characters; it is embedded in DATABASE_URL."
  type        = string
  default     = "transfer"
  sensitive   = true
}

variable "grafana_admin_password" {
  description = "Local Grafana admin password."
  type        = string
  default     = "admin"
  sensitive   = true
}

variable "transmitter_replicas" {
  description = "Transmitter replicas. Claims use SKIP LOCKED, so more than one is safe."
  type        = number
  default     = 1
}
