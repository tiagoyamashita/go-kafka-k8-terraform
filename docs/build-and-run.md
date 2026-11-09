# Build and run

Build the receiver and transmitter, load them into a local kind cluster, and send one JSON message through Postgres to the outbound topic.

Install Go, Terraform, Podman, kind, and kubectl with [setup.md](setup.md) first. Go must be 1.26 or newer and Terraform 1.5 or newer. The Podman machine must be rootful, and this shell must send kind to Podman:

```powershell
$env:KIND_EXPERIMENTAL_PROVIDER = "podman"
terraform version
podman info --format "{{.Host.Security.Rootless}}"
```

`terraform version` has to print 1.5 or newer. When it prints 0.13.4, Chocolatey's `terraform.exe` is the one on `PATH`. Put the winget binary first in this window:

```powershell
$env:Path = "$env:LOCALAPPDATA\Microsoft\WinGet\Links;" + $env:Path
terraform version
```

`Rootless` has to print `false`. [setup.md](setup.md) has the `podman machine set --rootful` commands. kind refuses to create the node on a rootless machine.

## Test

From the repository root:

```powershell
go test ./...
```

With `make`:

```text
make test
```

## Build the binaries

One module, two commands. Both are static binaries.

```powershell
go build -o bin/receiver.exe ./cmd/receiver
go build -o bin/transmitter.exe ./cmd/transmitter
```

On Linux or macOS, drop the `.exe` suffix. The cluster does not use these files. Kubernetes runs the image below.

To run a binary yourself, point it at a reachable Postgres and Kafka broker:

```powershell
$env:DATABASE_URL = "postgres://transfer:transfer@localhost:5432/transfers?sslmode=disable"
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:KAFKA_SECURITY_PROTOCOL = "PLAINTEXT"
.\bin\receiver.exe
```

Start the transmitter in a second terminal with the same `DATABASE_URL` and broker settings. Those `localhost` addresses work after the Kafka and Postgres apply below. Inside the cluster the same services are `kafka-0.kafka:9092` and `postgres:5432`.

## Launch Kafka and Postgres

This apply creates the kind cluster, then Kafka and Postgres only. The receiver, transmitter, and Grafana are left out, so no application image is required. Volumes use the StorageClass `standard` that the kind node already installs.

```powershell
$env:KIND_EXPERIMENTAL_PROVIDER = "podman"
terraform -chdir=terraform/cluster init
terraform -chdir=terraform/cluster apply -auto-approve
```

`terraform\cluster\kubeconfig` must exist before the platform commands. When that file is already there, skip the cluster apply and continue:

```powershell
terraform -chdir=terraform/platform init
terraform -chdir=terraform/platform apply -auto-approve -target="kubernetes_stateful_set.kafka" -target="kubernetes_stateful_set.postgres" -target="kubernetes_job.kafka_topics" -target="kubernetes_service.kafka_node" -target="kubernetes_service.postgres_node"
```

Quote each `-target` value. PowerShell otherwise cuts the resource name off at the dot.

With `make`, that sequence is `make data`. `make` sets `KIND_EXPERIMENTAL_PROVIDER` for its own commands.

```powershell
$env:KUBECONFIG = "$pwd\terraform\cluster\kubeconfig"
kubectl -n transfer rollout status statefulset/kafka
kubectl -n transfer rollout status statefulset/postgres
```

From this machine:

| Service | Address | Credentials |
| --- | --- | --- |
| Kafka | `localhost:9092` | plaintext, no auth |
| Postgres | `localhost:5432` | database `transfers`, user `transfer`, password `transfer` |

Pods in the cluster keep using `kafka-0.kafka:9092` and `postgres:5432`. A later full apply adds the receiver, transmitter, and observability on top of these same brokers.

## Build the image

The image contains both binaries. The receiver is the default command. The transmitter deployment overrides it with `/transmitter`.

```powershell
podman build -t transfer:local .
```

With `make`:

```text
make image
```

## Start the cluster

The checks at the top of this page must already pass: Terraform 1.5 or newer, a rootful Podman machine, and `KIND_EXPERIMENTAL_PROVIDER=podman` in this shell. Create the kind cluster first. The platform stack reads `terraform/cluster/kubeconfig`, which does not exist until that apply finishes. Build and load the image before the platform apply so the pods can find `transfer:local`.

```powershell
$env:KIND_EXPERIMENTAL_PROVIDER = "podman"
terraform -chdir=terraform/cluster init
terraform -chdir=terraform/cluster apply -auto-approve
podman build -t transfer:local .
kind load docker-image transfer:local --name transfer
terraform -chdir=terraform/platform init
terraform -chdir=terraform/platform apply -auto-approve
```

`kind load docker-image` is the kind subcommand name. With the provider set to `podman`, it loads the image from the Podman store.

With `make`, `make up` runs that same sequence.

Point kubectl at the kubeconfig Terraform wrote:

```powershell
$env:KUBECONFIG = "$pwd\terraform\cluster\kubeconfig"
kubectl -n transfer get pods
```

Wait until the workloads are ready:

```powershell
kubectl -n transfer rollout status statefulset/kafka
kubectl -n transfer rollout status statefulset/postgres
kubectl -n transfer rollout status deploy/receiver
kubectl -n transfer rollout status deploy/transmitter
```

Grafana is at http://127.0.0.1:30080. Sign in as `admin` / `admin` and open the Transfers dashboard.

`make kind-up` creates the same cluster from `kind/cluster.yaml` instead of Terraform. Use one of those paths. Both want the cluster name `transfer`.

## Send a message

```powershell
kubectl -n transfer exec -i kafka-0 -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-0.kafka:9092 --topic transfers.in
```

Paste one line and press Enter:

```json
{"id":"11111111-1111-1111-1111-111111111111","type":"transfer","payload":{"hello":"world"}}
```

`Ctrl+C` closes the producer.

Read the stored JSON:

```powershell
kubectl -n transfer exec postgres-0 -- psql -U transfer -d transfers -c "select id, status, payload from transfers;"
```

`status` moves to `published` after the transmitter produces. Read the outbound copy:

```powershell
kubectl -n transfer exec kafka-0 -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-0.kafka:9092 --topic transfers.out --from-beginning --max-messages 1
```

The Postgres password for this local cluster is `transfer`. The database name is `transfers`.

## Rebuild after a code change

```powershell
$env:KIND_EXPERIMENTAL_PROVIDER = "podman"
podman build -t transfer:local .
kind load docker-image transfer:local --name transfer
kubectl -n transfer rollout restart deploy/receiver deploy/transmitter
```

`imagePullPolicy` is `IfNotPresent`, so a restart is what picks up the image you just loaded.

## Stop

Destroy the platform first, then the cluster:

```powershell
terraform -chdir=terraform/platform destroy -auto-approve
terraform -chdir=terraform/cluster destroy -auto-approve
```

If the cluster was created with `make kind-up` instead of Terraform:

```powershell
kind delete cluster --name transfer
```
