resource "kubernetes_secret" "postgres" {
  metadata {
    name      = "postgres"
    namespace = local.namespace
  }

  data = {
    password = var.postgres_password
  }
}

resource "kubernetes_service" "postgres" {
  metadata {
    name      = "postgres"
    namespace = local.namespace
  }

  spec {
    cluster_ip = "None"
    selector = {
      app = "postgres"
    }

    port {
      name        = "postgres"
      port        = 5432
      target_port = "5432"
    }
  }
}

resource "kubernetes_service" "postgres_node" {
  metadata {
    name      = "postgres-node"
    namespace = local.namespace
  }

  spec {
    type = "NodePort"
    selector = {
      app = "postgres"
    }

    port {
      name        = "postgres"
      port        = 5432
      target_port = "5432"
      node_port   = 30432
    }
  }
}

resource "kubernetes_stateful_set" "postgres" {
  metadata {
    name      = "postgres"
    namespace = local.namespace
  }

  spec {
    service_name = kubernetes_service.postgres.metadata[0].name
    replicas     = 1

    selector {
      match_labels = {
        app = "postgres"
      }
    }

    template {
      metadata {
        labels = {
          app = "postgres"
        }
      }

      spec {
        security_context {
          fs_group = 999
        }

        container {
          name              = "postgres"
          image             = "postgres:16.10-alpine"
          image_pull_policy = "IfNotPresent"

          port {
            name           = "postgres"
            container_port = 5432
          }

          env {
            name  = "POSTGRES_DB"
            value = "transfers"
          }
          env {
            name  = "POSTGRES_USER"
            value = "transfer"
          }
          env {
            name = "POSTGRES_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.postgres.metadata[0].name
                key  = "password"
              }
            }
          }
          env {
            name  = "PGDATA"
            value = "/var/lib/postgresql/data/pgdata"
          }

          volume_mount {
            name       = "data"
            mount_path = "/var/lib/postgresql/data"
          }

          readiness_probe {
            exec {
              command = ["pg_isready", "-U", "transfer", "-d", "transfers"]
            }
            initial_delay_seconds = 5
            period_seconds        = 5
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
      }
    }

    volume_claim_template {
      metadata {
        name = "data"
      }
      spec {
        access_modes       = ["ReadWriteOnce"]
        storage_class_name = "standard"
        resources {
          requests = {
            storage = "1Gi"
          }
        }
      }
    }
  }
}
