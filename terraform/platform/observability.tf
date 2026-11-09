resource "kubernetes_config_map" "otel" {
  metadata {
    name      = "otel-collector"
    namespace = local.namespace
  }

  data = {
    "config.yaml" = replace(file("${path.module}/files/otel-collector.yaml"), "\r", "")
  }
}

resource "kubernetes_config_map" "prometheus" {
  metadata {
    name      = "prometheus"
    namespace = local.namespace
  }

  data = {
    "prometheus.yml" = replace(file("${path.module}/files/prometheus.yml"), "\r", "")
  }
}

resource "kubernetes_config_map" "tempo" {
  metadata {
    name      = "tempo"
    namespace = local.namespace
  }

  data = {
    "tempo.yaml" = replace(file("${path.module}/files/tempo.yaml"), "\r", "")
  }
}

resource "kubernetes_config_map" "grafana_datasources" {
  metadata {
    name      = "grafana-datasources"
    namespace = local.namespace
  }

  data = {
    "datasources.yaml" = replace(file("${path.module}/files/grafana-datasources.yaml"), "\r", "")
  }
}

resource "kubernetes_config_map" "grafana_dashboards_provider" {
  metadata {
    name      = "grafana-dashboards"
    namespace = local.namespace
  }

  data = {
    "dashboards.yaml" = replace(file("${path.module}/files/grafana-dashboards.yaml"), "\r", "")
  }
}

resource "kubernetes_config_map" "grafana_dashboard" {
  metadata {
    name      = "grafana-transfer-dashboard"
    namespace = local.namespace
  }

  data = {
    "transfer.json" = replace(file("${path.module}/files/transfer-dashboard.json"), "\r", "")
  }
}

resource "kubernetes_secret" "grafana" {
  metadata {
    name      = "grafana"
    namespace = local.namespace
  }

  data = {
    admin-user     = "admin"
    admin-password = var.grafana_admin_password
  }
}

resource "kubernetes_deployment" "otel" {
  metadata {
    name      = "otel-collector"
    namespace = local.namespace
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "otel-collector"
      }
    }

    template {
      metadata {
        labels = {
          app = "otel-collector"
        }
      }

      spec {
        container {
          name              = "otel-collector"
          image             = "otel/opentelemetry-collector-contrib:0.115.0"
          image_pull_policy = "IfNotPresent"
          args              = ["--config=/etc/otel/config.yaml"]

          port {
            name           = "otlp-grpc"
            container_port = 4317
          }
          port {
            name           = "otlp-http"
            container_port = 4318
          }
          port {
            name           = "metrics"
            container_port = 8889
          }

          volume_mount {
            name       = "config"
            mount_path = "/etc/otel/config.yaml"
            sub_path   = "config.yaml"
          }

          resources {
            requests = {
              cpu    = "20m"
              memory = "64Mi"
            }
            limits = {
              memory = "256Mi"
            }
          }
        }

        volume {
          name = "config"
          config_map {
            name = kubernetes_config_map.otel.metadata[0].name
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "otel" {
  metadata {
    name      = "otel-collector"
    namespace = local.namespace
  }

  spec {
    selector = {
      app = "otel-collector"
    }

    port {
      name        = "otlp-grpc"
      port        = 4317
      target_port = "4317"
    }

    port {
      name        = "otlp-http"
      port        = 4318
      target_port = "4318"
    }

    port {
      name        = "metrics"
      port        = 8889
      target_port = "8889"
    }
  }
}

resource "kubernetes_deployment" "prometheus" {
  metadata {
    name      = "prometheus"
    namespace = local.namespace
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "prometheus"
      }
    }

    template {
      metadata {
        labels = {
          app = "prometheus"
        }
      }

      spec {
        security_context {
          run_as_user  = 65534
          run_as_group = 65534
          fs_group     = 65534
        }

        container {
          name              = "prometheus"
          image             = "prom/prometheus:v2.55.1"
          image_pull_policy = "IfNotPresent"
          args = [
            "--config.file=/etc/prometheus/prometheus.yml",
            "--storage.tsdb.path=/prometheus",
            "--storage.tsdb.retention.time=6h",
          ]

          port {
            name           = "http"
            container_port = 9090
          }

          volume_mount {
            name       = "config"
            mount_path = "/etc/prometheus/prometheus.yml"
            sub_path   = "prometheus.yml"
          }

          volume_mount {
            name       = "data"
            mount_path = "/prometheus"
          }

          resources {
            requests = {
              cpu    = "50m"
              memory = "128Mi"
            }
            limits = {
              memory = "512Mi"
            }
          }
        }

        volume {
          name = "config"
          config_map {
            name = kubernetes_config_map.prometheus.metadata[0].name
          }
        }

        volume {
          name = "data"
          empty_dir {}
        }
      }
    }
  }
}

resource "kubernetes_service" "prometheus" {
  metadata {
    name      = "prometheus"
    namespace = local.namespace
  }

  spec {
    selector = {
      app = "prometheus"
    }

    port {
      name        = "http"
      port        = 9090
      target_port = "9090"
    }
  }
}

resource "kubernetes_deployment" "tempo" {
  metadata {
    name      = "tempo"
    namespace = local.namespace
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "tempo"
      }
    }

    template {
      metadata {
        labels = {
          app = "tempo"
        }
      }

      spec {
        security_context {
          run_as_user  = 10001
          run_as_group = 10001
          fs_group     = 10001
        }

        container {
          name              = "tempo"
          image             = "grafana/tempo:2.6.1"
          image_pull_policy = "IfNotPresent"
          args              = ["-config.file=/etc/tempo/tempo.yaml"]

          port {
            name           = "otlp"
            container_port = 4317
          }
          port {
            name           = "http"
            container_port = 3200
          }

          volume_mount {
            name       = "config"
            mount_path = "/etc/tempo/tempo.yaml"
            sub_path   = "tempo.yaml"
          }

          volume_mount {
            name       = "data"
            mount_path = "/var/tempo"
          }

          resources {
            requests = {
              cpu    = "50m"
              memory = "128Mi"
            }
            limits = {
              memory = "512Mi"
            }
          }
        }

        volume {
          name = "config"
          config_map {
            name = kubernetes_config_map.tempo.metadata[0].name
          }
        }

        volume {
          name = "data"
          empty_dir {}
        }
      }
    }
  }
}

resource "kubernetes_service" "tempo" {
  metadata {
    name      = "tempo"
    namespace = local.namespace
  }

  spec {
    selector = {
      app = "tempo"
    }

    port {
      name        = "otlp"
      port        = 4317
      target_port = "4317"
    }

    port {
      name        = "http"
      port        = 3200
      target_port = "3200"
    }
  }
}

resource "kubernetes_deployment" "grafana" {
  metadata {
    name      = "grafana"
    namespace = local.namespace
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "grafana"
      }
    }

    template {
      metadata {
        labels = {
          app = "grafana"
        }
      }

      spec {
        security_context {
          run_as_user  = 472
          run_as_group = 472
          fs_group     = 472
        }

        container {
          name              = "grafana"
          image             = "grafana/grafana:11.4.0"
          image_pull_policy = "IfNotPresent"

          port {
            name           = "http"
            container_port = 3000
          }

          env {
            name = "GF_SECURITY_ADMIN_USER"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.grafana.metadata[0].name
                key  = "admin-user"
              }
            }
          }

          env {
            name = "GF_SECURITY_ADMIN_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.grafana.metadata[0].name
                key  = "admin-password"
              }
            }
          }

          volume_mount {
            name       = "datasources"
            mount_path = "/etc/grafana/provisioning/datasources"
          }

          volume_mount {
            name       = "dashboard-provider"
            mount_path = "/etc/grafana/provisioning/dashboards"
          }

          volume_mount {
            name       = "data"
            mount_path = "/var/lib/grafana"
          }

          volume_mount {
            name       = "dashboard"
            mount_path = "/var/lib/grafana/dashboards"
          }

          resources {
            requests = {
              cpu    = "20m"
              memory = "64Mi"
            }
            limits = {
              memory = "256Mi"
            }
          }
        }

        volume {
          name = "datasources"
          config_map {
            name = kubernetes_config_map.grafana_datasources.metadata[0].name
          }
        }

        volume {
          name = "dashboard-provider"
          config_map {
            name = kubernetes_config_map.grafana_dashboards_provider.metadata[0].name
          }
        }

        volume {
          name = "dashboard"
          config_map {
            name = kubernetes_config_map.grafana_dashboard.metadata[0].name
          }
        }

        volume {
          name = "data"
          empty_dir {}
        }
      }
    }
  }
}

resource "kubernetes_service" "grafana" {
  metadata {
    name      = "grafana"
    namespace = local.namespace
  }

  spec {
    type = "NodePort"
    selector = {
      app = "grafana"
    }

    port {
      name        = "http"
      port        = 3000
      target_port = "3000"
      node_port   = 30080
    }
  }
}
