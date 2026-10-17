# Changelog

All notable changes are recorded here. AgentStorm follows semantic versioning for release artifacts;
the Kubernetes API remains `v1alpha1` until its compatibility contract is explicitly promoted.

## [Unreleased]

### Added

- `openai-compatible` Worker provider for pinned OpenAI-compatible chat and tool-calling endpoints,
  including domestic cloud APIs and self-hosted servers such as vLLM.
- SRE benchmark runners for a no-cost local mock, an explicit in-process real endpoint, and a
  Kubernetes/KEDA real endpoint. Real modes require a spend opt-in, a pinned model, and a price
  snapshot. No real-model result is included.

### Fixed

- Controller `/readyz` stays unsuccessful until the process is elected leader, so
  `kubectl rollout status` does not succeed while the manager cannot reach the API.
  `/healthz` remains a ping. Electing Deployments in `config/manager` and the Helm chart
  use one replica and RollingUpdate `maxUnavailable: 1` / `maxSurge: 0`, which removes the
  current pod before starting the replacement and stays valid when server-side apply upgrades
  the default strategy. The process releases its Lease on shutdown, so a replacement can become
  leader. Extra electing replicas stay Running and become Ready only if they win the lease.
- The kind E2E retries once when the controller log contains `error retrieving resource lock`
  and either `kubectl rollout status` times out or another wait times out while a worker Job
  is already Complete. The retry keeps the first attempt's diagnostics, deletes that kind
  cluster, and runs again. Any other failure stops immediately.

### Changed

- Bundled result-stack object storage uses digest-pinned distroless `pgsty/silo`
  `RELEASE.2026-10-15T00-00-00Z`. The `agentstorm-minio` Service, S3 API, and `/minio/health/*`
  probes stay in place.

## [0.6.0] - 2026-08-05

### Added

- Trusted image-bundled Adapter factories selected through `target.adapterEntrypoint`.
- A 32-incident SRE diagnostic Agent with deterministic incident, metrics, logs, and runbook tools.
- Attempt/case/run/comparison model and tool call accounting with unknown-model-call preservation.
- Helm chart, fixed-matrix benchmark tooling, ADRs, contribution guidance, and demo automation.

The images and OCI chart are published and verified on amd64 and arm64. The stable release includes
checksummed evidence for the complete 384+32 fake Agent-lifecycle shape; performance fields are
deliberately omitted. Real-Provider performance evidence is tracked after v0.6 and is never inferred
from fake executions or release smoke tests.

## [0.5.0-alpha.1] - 2026-08-04

- Durable PostgreSQL shard queue and KEDA ScaledJob scale-to-zero.
- Resource-profile/quota admission and distributed Provider permits.

## [0.4.0-alpha.1] - 2026-08-02

- Deterministic fault injection, conservative retries, circuit breaking, and durable cancellation.

## [0.3.0-alpha.1] - 2026-08-02

- Assertions, cost accounting, OpenTelemetry redaction, and Promptfoo replay.

## [0.2.0-alpha.1] - 2026-08-02

- Durable Result API with PostgreSQL, object storage, idempotent aggregation, and comparison.

## [0.1.0-alpha.1] - 2026-08-01

- First public multi-architecture Controller and Worker images.
