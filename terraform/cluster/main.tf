# The kind provider uses Podman when the docker CLI is not on PATH.
# `kind load` also needs KIND_EXPERIMENTAL_PROVIDER=podman so the image
# is loaded into that same engine.
resource "kind_cluster" "transfer" {
  name            = "transfer"
  node_image      = "kindest/node:v1.32.2"
  wait_for_ready  = true
  kubeconfig_path = "${path.module}/kubeconfig"

  kind_config {
    kind        = "Cluster"
    api_version = "kind.x-k8s.io/v1alpha4"

    node {
      role  = "control-plane"
      image = "kindest/node:v1.32.2"

      extra_port_mappings {
        container_port = 30080
        host_port      = 30080
        protocol       = "TCP"
      }
      extra_port_mappings {
        container_port = 30092
        host_port      = 9092
        protocol       = "TCP"
      }
      extra_port_mappings {
        container_port = 30432
        host_port      = 5432
        protocol       = "TCP"
      }
    }
  }
}
