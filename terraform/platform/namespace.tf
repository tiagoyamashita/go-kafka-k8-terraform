resource "kubernetes_namespace" "transfer" {
  metadata {
    name = "transfer"
  }
}

locals {
  namespace = kubernetes_namespace.transfer.metadata[0].name
}
