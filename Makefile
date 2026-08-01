SHELL := /bin/bash

CONTROLLER_IMAGE ?= agentstorm-controller:dev
WORKER_IMAGE ?= agentstorm-worker:dev
RESULT_API_IMAGE ?= agentstorm-result-api:dev
ENVTEST_K8S_VERSION ?= v1.33.0

.PHONY: fmt vet test test-envtest test-results-integration test-result-pipeline build build-result-api worker-local docker-build docker-build-result-api install deploy deploy-namespace undeploy undeploy-namespace e2e-local

fmt:
	gofmt -w $$(find api cmd internal -name '*.go')

vet:
	go vet ./...

test:
	go test ./...
	PYTHONPATH=worker/src python3 -m unittest discover -s worker/tests -v

test-envtest:
	ENVTEST_K8S_VERSION=$(ENVTEST_K8S_VERSION) go test -tags=envtest ./api/v1alpha1 -run TestAgentTestRunCRDContract -count=1

test-results-integration:
	go test -tags=integration ./internal/results -count=1

test-result-pipeline:
	./hack/test-result-pipeline.sh

build:
	mkdir -p bin
	go build -o bin/agentstorm-controller ./cmd/controller

build-result-api:
	mkdir -p bin
	go build -o bin/agentstorm-result-api ./cmd/result-api

worker-local:
	PYTHONPATH=worker/src python3 -m agentstorm_worker run \
		--config examples/run.local.json \
		--dataset examples/datasets/basic.jsonl \
		--output .out/local

docker-build:
	docker build -f Dockerfile.controller -t $(CONTROLLER_IMAGE) .
	docker build -f worker/Dockerfile -t $(WORKER_IMAGE) worker

docker-build-result-api:
	docker build -f Dockerfile.result-api -t $(RESULT_API_IMAGE) .

install:
	kubectl apply -f config/crd/bases/agentstorm.io_agenttestruns.yaml

deploy:
	kubectl apply -k config/default
	kubectl delete rolebinding agentstorm-controller -n agentstorm-system --ignore-not-found
	kubectl delete role agentstorm-controller -n agentstorm-system --ignore-not-found
	kubectl delete networkpolicy agentstorm-worker -n agentstorm-system --ignore-not-found

deploy-namespace:
	kubectl apply -k config/namespace-scoped
	kubectl delete clusterrolebinding agentstorm-controller --ignore-not-found
	kubectl delete clusterrole agentstorm-controller --ignore-not-found

undeploy:
	kubectl delete -k config/default

undeploy-namespace:
	kubectl delete -k config/namespace-scoped

e2e-local:
	CONTROLLER_IMAGE=$(CONTROLLER_IMAGE) WORKER_IMAGE=$(WORKER_IMAGE) ./hack/e2e-local.sh
