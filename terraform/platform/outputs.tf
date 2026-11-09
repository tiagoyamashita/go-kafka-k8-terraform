output "grafana_url" {
  description = "Grafana NodePort on the kind node."
  value       = "http://127.0.0.1:30080"
}

output "namespace" {
  description = "Namespace for the transfer workloads."
  value       = local.namespace
}
