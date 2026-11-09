IMAGE ?= transfer:local
CLUSTER ?= transfer

.PHONY: test image kind-up kind-load tf-init tf-apply data up

test:
	go test ./...

image:
	docker build -t $(IMAGE) .

kind-up:
	kind create cluster --name $(CLUSTER) --config kind/cluster.yaml

kind-load: image
	kind load docker-image $(IMAGE) --name $(CLUSTER)

tf-init:
	terraform -chdir=terraform/cluster init
	terraform -chdir=terraform/platform init

tf-apply: tf-init
	terraform -chdir=terraform/cluster apply -auto-approve
	terraform -chdir=terraform/platform apply -auto-approve

data: tf-init
	terraform -chdir=terraform/cluster apply -auto-approve
	terraform -chdir=terraform/platform apply -auto-approve \
		-target=kubernetes_stateful_set.kafka \
		-target=kubernetes_stateful_set.postgres \
		-target=kubernetes_job.kafka_topics \
		-target=kubernetes_service.kafka_node \
		-target=kubernetes_service.postgres_node

up: tf-init
	terraform -chdir=terraform/cluster apply -auto-approve
	$(MAKE) kind-load
	terraform -chdir=terraform/platform apply -auto-approve
