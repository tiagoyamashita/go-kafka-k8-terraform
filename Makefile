IMAGE ?= transfer:local
CLUSTER ?= transfer

.PHONY: test image kind-up kind-load tf-init tf-apply up

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

up: tf-init
	terraform -chdir=terraform/cluster apply -auto-approve
	$(MAKE) kind-load
	terraform -chdir=terraform/platform apply -auto-approve
