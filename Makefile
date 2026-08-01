SHELL := /bin/bash

CONTROLLER_IMAGE ?= agentstorm-controller:dev
WORKER_IMAGE ?= agentstorm-worker:dev

.PHONY: fmt vet test build worker-local docker-build install deploy undeploy

fmt:
	gofmt -w $$(find api cmd internal -name '*.go')

vet:
	go vet ./...

test:
	go test ./...
	PYTHONPATH=worker/src python3 -m unittest discover -s worker/tests -v

build:
	mkdir -p bin
	go build -o bin/agentstorm-controller ./cmd/controller

worker-local:
	PYTHONPATH=worker/src python3 -m agentstorm_worker run \
		--config examples/run.local.json \
		--dataset examples/datasets/basic.jsonl \
		--output .out/local

docker-build:
	docker build -f Dockerfile.controller -t $(CONTROLLER_IMAGE) .
	docker build -f worker/Dockerfile -t $(WORKER_IMAGE) worker

install:
	kubectl apply -f config/crd/bases/agentstorm.io_agenttestruns.yaml

deploy:
	kubectl apply -k config/default

undeploy:
	kubectl delete -k config/default
