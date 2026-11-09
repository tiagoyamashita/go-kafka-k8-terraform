output "grafana_url" {
  description = "Grafana NodePort on the kind node."
  value       = "http://127.0.0.1:30080"
}

output "namespace" {
  description = "Namespace for the transfer workloads."
  value       = local.namespace
}

output "kafka_bootstrap_localhost" {
  description = "Kafka bootstrap address from the host. In-cluster clients use kafka-0.kafka:9092."
  value       = "localhost:9092"
}

output "postgres_localhost" {
  description = "Postgres address from the host. In-cluster clients use postgres:5432."
  value       = "localhost:5432"
}
