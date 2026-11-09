# Build and run

Build the receiver and transmitter, load them into a local kind cluster, and send one JSON message through Postgres to the outbound topic.

You need:

- Go 1.26 or newer
- Docker
- [kind](https://kind.sigs.k8s.io/)
- kubectl
- Terraform 1.5 or newer

The `go` command on this machine may still be older than 1.26. Use a newer toolchain for `go test` and `go build`.

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

Start the transmitter in a second terminal with the same `DATABASE_URL` and broker settings. The local kind brokers are only reachable as `kafka-0.kafka:9092` from inside the cluster, so the steps below are the supported way to run the whole path.

## Build the image

The image contains both binaries. The receiver is the default command. The transmitter deployment overrides it with `/transmitter`.

```powershell
docker build -t transfer:local .
```

With `make`:

```text
make image
```

## Start the cluster

Create the kind cluster first. The platform stack reads `terraform/cluster/kubeconfig`, which does not exist until that apply finishes. Build and load the image before the platform apply so the pods can find `transfer:local`.

```powershell
terraform -chdir=terraform/cluster init
terraform -chdir=terraform/cluster apply -auto-approve
docker build -t transfer:local .
kind load docker-image transfer:local --name transfer
terraform -chdir=terraform/platform init
terraform -chdir=terraform/platform apply -auto-approve
```

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
docker build -t transfer:local .
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
