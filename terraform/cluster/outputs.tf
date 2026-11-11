output "kubeconfig_path" {
  description = "Kubeconfig written for the platform stack."
  value       = kind_cluster.transfer.kubeconfig_path
}

output "cluster_name" {
  description = "Kind cluster name."
  value       = kind_cluster.transfer.name
}
