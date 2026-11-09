resource "kubernetes_namespace" "local_path" {
  metadata {
    name = "local-path-storage"
  }
}

resource "kubernetes_service_account" "local_path" {
  metadata {
    name      = "local-path-provisioner-service-account"
    namespace = kubernetes_namespace.local_path.metadata[0].name
  }
}

resource "kubernetes_role" "local_path" {
  metadata {
    name      = "local-path-provisioner-role"
    namespace = kubernetes_namespace.local_path.metadata[0].name
  }

  rule {
    api_groups = [""]
    resources  = ["pods"]
    verbs      = ["get", "list", "watch", "create", "patch", "update", "delete"]
  }
}

resource "kubernetes_role_binding" "local_path" {
  metadata {
    name      = "local-path-provisioner-bind"
    namespace = kubernetes_namespace.local_path.metadata[0].name
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.local_path.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.local_path.metadata[0].name
    namespace = kubernetes_namespace.local_path.metadata[0].name
  }
}

resource "kubernetes_cluster_role" "local_path" {
  metadata {
    name = "local-path-provisioner-role"
  }

  rule {
    api_groups = [""]
    resources  = ["nodes", "persistentvolumeclaims", "configmaps", "pods", "pods/log"]
    verbs      = ["get", "list", "watch"]
  }

  rule {
    api_groups = [""]
    resources  = ["persistentvolumes"]
    verbs      = ["get", "list", "watch", "create", "patch", "update", "delete"]
  }

  rule {
    api_groups = [""]
    resources  = ["events"]
    verbs      = ["create", "patch"]
  }

  rule {
    api_groups = ["storage.k8s.io"]
    resources  = ["storageclasses"]
    verbs      = ["get", "list", "watch"]
  }
}

resource "kubernetes_cluster_role_binding" "local_path" {
  metadata {
    name = "local-path-provisioner-bind"
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.local_path.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.local_path.metadata[0].name
    namespace = kubernetes_namespace.local_path.metadata[0].name
  }
}

resource "kubernetes_config_map" "local_path" {
  metadata {
    name      = "local-path-config"
    namespace = kubernetes_namespace.local_path.metadata[0].name
  }

  data = {
    "config.json"    = replace(file("${path.module}/files/local-path-config.json"), "\r", "")
    "setup"          = replace(file("${path.module}/files/local-path-setup.sh"), "\r", "")
    "teardown"       = replace(file("${path.module}/files/local-path-teardown.sh"), "\r", "")
    "helperPod.yaml" = replace(file("${path.module}/files/helper-pod.yaml"), "\r", "")
  }
}

resource "kubernetes_deployment" "local_path" {
  metadata {
    name      = "local-path-provisioner"
    namespace = kubernetes_namespace.local_path.metadata[0].name
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "local-path-provisioner"
      }
    }

    template {
      metadata {
        labels = {
          app = "local-path-provisioner"
        }
      }

      spec {
        service_account_name = kubernetes_service_account.local_path.metadata[0].name

        container {
          name              = "local-path-provisioner"
          image             = "rancher/local-path-provisioner:v0.0.31"
          image_pull_policy = "IfNotPresent"
          command           = ["local-path-provisioner", "--debug", "start", "--config", "/etc/config/config.json"]

          env {
            name = "POD_NAMESPACE"
            value_from {
              field_ref {
                field_path = "metadata.namespace"
              }
            }
          }

          env {
            name  = "CONFIG_MOUNT_PATH"
            value = "/etc/config/"
          }

          volume_mount {
            name       = "config-volume"
            mount_path = "/etc/config/"
          }

          resources {
            requests = {
              cpu    = "20m"
              memory = "32Mi"
            }
            limits = {
              memory = "128Mi"
            }
          }
        }

        volume {
          name = "config-volume"
          config_map {
            name = kubernetes_config_map.local_path.metadata[0].name
          }
        }
      }
    }
  }
}

resource "kubernetes_storage_class" "local_path" {
  metadata {
    name = "local-path"
    annotations = {
      "storageclass.kubernetes.io/is-default-class" = "true"
    }
  }

  storage_provisioner = "rancher.io/local-path"
  reclaim_policy      = "Delete"
  volume_binding_mode = "WaitForFirstConsumer"

  depends_on = [kubernetes_deployment.local_path]
}
