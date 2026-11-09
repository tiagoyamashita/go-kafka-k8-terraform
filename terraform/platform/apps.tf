resource "kubernetes_config_map" "transfer" {
  metadata {
    name      = "transfer"
    namespace = local.namespace
  }

  data = {
    KAFKA_BOOTSTRAP_SERVERS     = "kafka-0.kafka:9092"
    KAFKA_SECURITY_PROTOCOL     = "PLAINTEXT"
    KAFKA_INBOUND_TOPIC         = "transfers.in"
    KAFKA_OUTBOUND_TOPIC        = "transfers.out"
    KAFKA_DLQ_TOPIC             = "transfers.dlq"
    KAFKA_GROUP_ID              = "transfer-receiver"
    OTEL_EXPORTER_OTLP_ENDPOINT = "otel-collector:4317"
    HTTP_ADDR                   = ":8080"
    POLL_INTERVAL               = "2s"
    BATCH_SIZE                  = "50"
    MAX_ATTEMPTS                = "5"
    PUBLISH_LEASE               = "2m"
  }
}

resource "kubernetes_secret" "transfer" {
  metadata {
    name      = "transfer"
    namespace = local.namespace
  }

  data = {
    DATABASE_URL = "postgres://transfer:${var.postgres_password}@postgres:5432/transfers?sslmode=disable"
  }
}

resource "kubernetes_deployment" "receiver" {
  metadata {
    name      = "receiver"
    namespace = local.namespace
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "receiver"
      }
    }

    template {
      metadata {
        labels = {
          app = "receiver"
        }
      }

      spec {
        container {
          name              = "receiver"
          image             = var.image
          image_pull_policy = "IfNotPresent"
          command           = ["/receiver"]

          env_from {
            config_map_ref {
              name = kubernetes_config_map.transfer.metadata[0].name
            }
          }

          env {
            name  = "OTEL_SERVICE_NAME"
            value = "receiver"
          }

          env {
            name = "DATABASE_URL"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.transfer.metadata[0].name
                key  = "DATABASE_URL"
              }
            }
          }

          port {
            name           = "http"
            container_port = 8080
          }

          liveness_probe {
            http_get {
              path = "/healthz"
              port = "8080"
            }
            initial_delay_seconds = 5
            period_seconds        = 10
          }

          readiness_probe {
            http_get {
              path = "/readyz"
              port = "8080"
            }
            initial_delay_seconds = 5
            period_seconds        = 5
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
      }
    }
  }

  depends_on = [
    kubernetes_stateful_set.postgres,
    kubernetes_stateful_set.kafka,
    kubernetes_job.kafka_topics,
  ]
}

resource "kubernetes_deployment" "transmitter" {
  metadata {
    name      = "transmitter"
    namespace = local.namespace
  }

  spec {
    replicas = var.transmitter_replicas

    selector {
      match_labels = {
        app = "transmitter"
      }
    }

    template {
      metadata {
        labels = {
          app = "transmitter"
        }
      }

      spec {
        container {
          name              = "transmitter"
          image             = var.image
          image_pull_policy = "IfNotPresent"
          command           = ["/transmitter"]

          env_from {
            config_map_ref {
              name = kubernetes_config_map.transfer.metadata[0].name
            }
          }

          env {
            name  = "OTEL_SERVICE_NAME"
            value = "transmitter"
          }

          env {
            name = "DATABASE_URL"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.transfer.metadata[0].name
                key  = "DATABASE_URL"
              }
            }
          }

          port {
            name           = "http"
            container_port = 8080
          }

          liveness_probe {
            http_get {
              path = "/healthz"
              port = "8080"
            }
            initial_delay_seconds = 5
            period_seconds        = 10
          }

          readiness_probe {
            http_get {
              path = "/readyz"
              port = "8080"
            }
            initial_delay_seconds = 5
            period_seconds        = 5
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
      }
    }
  }

  depends_on = [
    kubernetes_stateful_set.postgres,
    kubernetes_stateful_set.kafka,
    kubernetes_job.kafka_topics,
  ]
}

resource "kubernetes_service" "receiver" {
  metadata {
    name      = "receiver"
    namespace = local.namespace
  }

  spec {
    selector = {
      app = "receiver"
    }

    port {
      name        = "http"
      port        = 8080
      target_port = "8080"
    }
  }
}

resource "kubernetes_service" "transmitter" {
  metadata {
    name      = "transmitter"
    namespace = local.namespace
  }

  spec {
    selector = {
      app = "transmitter"
    }

    port {
      name        = "http"
      port        = 8080
      target_port = "8080"
    }
  }
}
