terraform {
  required_version = ">= 1.5.0"

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.35.0, < 3.0.0"
    }
  }
}

provider "kubernetes" {
  config_path = "${path.module}/../cluster/kubeconfig"
}
