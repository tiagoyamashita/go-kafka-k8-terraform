resource "kubernetes_service" "kafka" {
  metadata {
    name      = "kafka"
    namespace = local.namespace
  }

  spec {
    cluster_ip                  = "None"
    publish_not_ready_addresses = true
    selector = {
      app = "kafka"
    }

    port {
      name        = "plaintext"
      port        = 9092
      target_port = "9092"
    }

    port {
      name        = "controller"
      port        = 9093
      target_port = "9093"
    }
  }
}

resource "kubernetes_stateful_set" "kafka" {
  metadata {
    name      = "kafka"
    namespace = local.namespace
  }

  spec {
    service_name = kubernetes_service.kafka.metadata[0].name
    replicas     = 1

    selector {
      match_labels = {
        app = "kafka"
      }
    }

    template {
      metadata {
        labels = {
          app = "kafka"
        }
      }

      spec {
        security_context {
          fs_group = 1000
        }

        container {
          name              = "kafka"
          image             = "apache/kafka:3.9.1"
          image_pull_policy = "IfNotPresent"

          port {
            name           = "plaintext"
            container_port = 9092
          }

          port {
            name           = "external"
            container_port = 9094
          }

          port {
            name           = "controller"
            container_port = 9093
          }

          env {
            name  = "KAFKA_NODE_ID"
            value = "1"
          }
          env {
            name  = "KAFKA_PROCESS_ROLES"
            value = "broker,controller"
          }
          env {
            name  = "KAFKA_LISTENERS"
            value = "PLAINTEXT://:9092,PLAINTEXT_HOST://:9094,CONTROLLER://:9093"
          }
          env {
            name  = "KAFKA_ADVERTISED_LISTENERS"
            value = "PLAINTEXT://kafka-0.kafka:9092,PLAINTEXT_HOST://localhost:9092"
          }
          env {
            name  = "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"
            value = "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"
          }
          env {
            name  = "KAFKA_CONTROLLER_LISTENER_NAMES"
            value = "CONTROLLER"
          }
          env {
            name  = "KAFKA_INTER_BROKER_LISTENER_NAME"
            value = "PLAINTEXT"
          }
          env {
            name  = "KAFKA_CONTROLLER_QUORUM_VOTERS"
            value = "1@kafka-0.kafka:9093"
          }
          env {
            name  = "CLUSTER_ID"
            value = "4L6g3nShT-eMCtK--X86sw"
          }
          env {
            name  = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"
            value = "1"
          }
          env {
            name  = "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR"
            value = "1"
          }
          env {
            name  = "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR"
            value = "1"
          }
          env {
            name  = "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS"
            value = "0"
          }
          env {
            name  = "KAFKA_NUM_PARTITIONS"
            value = "3"
          }
          env {
            name  = "KAFKA_AUTO_CREATE_TOPICS_ENABLE"
            value = "false"
          }
          env {
            name  = "KAFKA_LOG_DIRS"
            value = "/var/lib/kafka/data"
          }
          env {
            name  = "KAFKA_HEAP_OPTS"
            value = "-Xms128m -Xmx256m"
          }

          volume_mount {
            name       = "data"
            mount_path = "/var/lib/kafka/data"
          }

          readiness_probe {
            tcp_socket {
              port = "9092"
            }
            initial_delay_seconds = 15
            period_seconds        = 5
          }

          resources {
            requests = {
              cpu    = "100m"
              memory = "384Mi"
            }
            limits = {
              memory = "768Mi"
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

resource "kubernetes_job" "kafka_topics" {
  metadata {
    name      = "kafka-topics"
    namespace = local.namespace
  }

  spec {
    backoff_limit = 20

    template {
      metadata {
        labels = {
          app = "kafka-topics"
        }
      }

      spec {
        restart_policy = "OnFailure"

        container {
          name              = "topics"
          image             = "apache/kafka:3.9.1"
          image_pull_policy = "IfNotPresent"
          command = [
            "/bin/bash",
            "-lc",
            replace(file("${path.module}/files/kafka-topics.sh"), "\r", ""),
          ]
        }
      }
    }
  }

  depends_on = [kubernetes_stateful_set.kafka]
}

resource "kubernetes_service" "kafka_node" {
  metadata {
    name      = "kafka-node"
    namespace = local.namespace
  }

  spec {
    type = "NodePort"
    selector = {
      app = "kafka"
    }

    port {
      name        = "external"
      port        = 9094
      target_port = "9094"
      node_port   = 30092
    }
  }
}
