# Repository Guidelines

AgentStorm contains a Go Kubernetes controller and a Python execution worker.

## Commands

- `make fmt`: format Go sources.
- `make vet`: run Go static checks.
- `make test`: run all Go and Python tests.
- `make worker-local`: execute the no-cost fake-provider smoke workload.
- `make docker-build`: build controller and worker images.

## Boundaries

- Keep Kubernetes reconciliation in `internal/controller` and provider SDKs in `worker`.
- Never serialize Secrets into generated ConfigMaps, logs, status, or result fixtures.
- Keep execution fields immutable through CRD CEL rules; only declarative cancellation is mutable.
- Use deterministic assertions before adding model-based graders.
- Do not claim roadmap features as implemented in README examples or resume wording.

Update API types, CRD YAML, samples, docs, and tests together when changing the run contract.
