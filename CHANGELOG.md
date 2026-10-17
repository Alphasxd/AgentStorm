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

- OpenAI-compatible benchmark runners estimate the next run's token spend before starting it and
  refuse when recorded spend plus that estimate would pass USD 8. Input uses an allowance of 8192
  tokens per call, above the scripted fixture requests and the assistant history those calls
  resend. The endpoint does not enforce that allowance. Output uses the 512-token `max_tokens`
  limit. A timeout, `5xx`, or connection failure prices that one call at the same allowance and
  keeps tokens from earlier calls, so one transient failure does not null the case cost.
  Cancelling an in-flight call is re-raised, and the runner records the run as cancelled. The
  real-run case timeout stays above two attempts times eight calls times the 120 second urllib
  socket timeout. That timeout applies to each socket operation, not to the whole HTTP request.
- Incomplete or reset response bodies map to an ambiguous `connection_error`. The failed call is
  not counted as a completion. `model_call_count` stays at the number of completed calls. A failed
  call the provider may already have accepted is recorded on the adapter response as
  `agentstorm.usage.unknown_calls` and is not added to `model_call_count`. A tool `ExecutionError`
  closes its lifecycle span before it propagates. Base URL shape checks apply only to
  `openai-compatible`. The Kubernetes runner rejects loopback and unspecified addresses, including
  a trailing DNS dot and IPv4-mapped forms such as `::ffff:127.0.0.1`, including on Python before
  3.13. The 8-concurrent / 60 rpm cap remains the development KEDA overlay and is not a Helm chart
  default.
- The SRE compatible factory and the scripted request measurement share one tool definition, so
  the chat-completions schemas cannot drift from the tools the factory sends.
- A failed OpenAI-compatible call that omits `max_tokens` is priced at the named allowance
  `UNKNOWN_CALL_OUTPUT_TOKENS` (8192). The previous unnamed fallback was 512, the SRE benchmark
  cap, which undercharges an uncapped completion. A request that sets `max_tokens` still charges
  that limit.
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
