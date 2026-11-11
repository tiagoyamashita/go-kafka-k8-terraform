# go-kafka-k8-terraform

A local transfer pipeline. A receiver consumes JSON from Kafka and stores each message in PostgreSQL. A transmitter publishes those stored JSON documents to an outbound topic.

```
transfers.in -> receiver -> Postgres (payload jsonb) -> transmitter -> transfers.out
                                                      \-> transfers.dlq
```

The same binaries talk to a local broker or to Confluent Cloud. Only the Kafka environment variables change.

How the pieces fit together is in [docs/architecture.md](docs/architecture.md). Install Go, Terraform, Podman, kind, and kubectl with [docs/setup.md](docs/setup.md), then build and run with [docs/build-and-run.md](docs/build-and-run.md). An AWS cost estimate for the same processes is in [docs/aws-cost.md](docs/aws-cost.md).

## What is stored

Each inbound value must be a JSON object with a UUID `id`:

```json
{"id":"11111111-1111-1111-1111-111111111111","type":"transfer","payload":{"amount":10}}
```

The receiver writes that document, unchanged, into `transfers.payload` as `jsonb`. Kafka coordinates, status, and attempt counts stay in columns so the outbox can claim rows. A repeated id or source offset does not overwrite the stored JSON.

Status moves from `received` to `publishing`, then to `published`. A failed publish goes back to `failed` and is retried until `MAX_ATTEMPTS`. After that the original JSON is produced to `transfers.dlq` and the row becomes `dead`.

Delivery is at-least-once. The outbound record key is the transfer id. A crash after a successful produce and before the row is marked published can publish the same JSON twice.

Invalid JSON, or a message without a UUID `id`, is not stored. It is written to `transfers.dlq` as JSON. If that produce fails, the source offset stays uncommitted.

## Layout

- `cmd/receiver` consumes `transfers.in` and inserts JSON.
- `cmd/transmitter` claims rows with `FOR UPDATE SKIP LOCKED` and produces them.
- `terraform/cluster` creates the kind cluster.
- `terraform/platform` installs Kafka, Postgres, both workloads, and the OpenTelemetry collector, Prometheus, Tempo, and Grafana.

## Run the tests

Go 1.26 or newer:

```powershell
go test ./...
```

## Bring up the local cluster

Podman, kind, and Terraform 1.5 or newer are required. Set `KIND_EXPERIMENTAL_PROVIDER=podman` first. `make data` (or the targeted apply in [docs/build-and-run.md](docs/build-and-run.md)) starts only Kafka and Postgres and publishes them on `localhost:9092` and `localhost:5432`. The full apply below also starts the receiver, transmitter, and Grafana. The cluster stack and the platform stack are separate because the Kubernetes provider needs a kubeconfig that already exists.

```powershell
$env:KIND_EXPERIMENTAL_PROVIDER = "podman"
terraform -chdir=terraform/cluster init
terraform -chdir=terraform/cluster apply -auto-approve
podman build -t transfer:local .
kind load docker-image transfer:local --name transfer
terraform -chdir=terraform/platform init
terraform -chdir=terraform/platform apply -auto-approve
```

`make up` runs the same steps when `make` is available. `make kind-up` creates the same cluster from `kind/cluster.yaml` instead of Terraform. Do not create that cluster both ways; the name `transfer` is shared.

Grafana is at http://127.0.0.1:30080. The local admin user and password are `admin` / `admin`. The Transfers dashboard shows receive rate, publish rate, persist latency, publish latency, outbox depth, retries, errors, and dead letters.

## Send a message

```powershell
kubectl -n transfer exec -i kafka-0 -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-0.kafka:9092 --topic transfers.in
```

Paste one JSON object and press Enter:

```json
{"id":"11111111-1111-1111-1111-111111111111","type":"transfer","payload":{"hello":"world"}}
```

Then read the stored JSON:

```powershell
kubectl -n transfer exec postgres-0 -- psql -U transfer -d transfers -c "select id, status, payload from transfers;"
```

The outbound copy is on `transfers.out`.

## Configuration

| Variable | Local default | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | required | Postgres connection string |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka-0.kafka:9092` | Broker list |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL` |
| `KAFKA_SASL_MECHANISM` | `PLAIN` | `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512` |
| `KAFKA_SASL_USERNAME` | empty | Confluent Cloud API key |
| `KAFKA_SASL_PASSWORD` | empty | Confluent Cloud API secret |
| `KAFKA_INBOUND_TOPIC` | `transfers.in` | Receiver topic |
| `KAFKA_OUTBOUND_TOPIC` | `transfers.out` | Transmitter topic |
| `KAFKA_DLQ_TOPIC` | `transfers.dlq` | Poison and exhausted messages |
| `KAFKA_GROUP_ID` | `transfer-receiver` | Receiver consumer group |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `otel-collector:4317` | Traces and metrics. `https://` turns on TLS |
| `OTEL_SERVICE_NAME` | process name | `receiver` or `transmitter` |
| `MAX_ATTEMPTS` | `5` | Publish attempts before the DLQ |
| `PUBLISH_LEASE` | `2m` | How long a crashed publish claim stays locked |
| `POLL_INTERVAL` | `2s` | Transmitter idle poll |
| `BATCH_SIZE` | `50` | Rows claimed per poll |
| `HTTP_ADDR` | `:8080` | `/healthz`, `/readyz`, and `/metrics` |

## Confluent Cloud

Point the same image at Confluent Cloud by replacing the broker settings. Create `transfers.in`, `transfers.out`, and `transfers.dlq` in the cluster first.

```text
KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.region.aws.confluent.cloud:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=<api-key>
KAFKA_SASL_PASSWORD=<api-secret>
```

`SCRAM-SHA-256` and `SCRAM-SHA-512` are accepted for `KAFKA_SASL_MECHANISM` as well. No code change is required.
