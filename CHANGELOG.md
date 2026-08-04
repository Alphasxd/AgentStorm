# Changelog

All notable changes are recorded here. AgentStorm follows semantic versioning for release artifacts;
the Kubernetes API remains `v1alpha1` until its compatibility contract is explicitly promoted.

## [Unreleased]

## [0.6.0] - 2026-08-05

### Added

- Trusted image-bundled Adapter factories selected through `target.adapterEntrypoint`.
- A 32-incident SRE diagnostic Agent with deterministic incident, metrics, logs, and runbook tools.
- Attempt/case/run/comparison model and tool call accounting with unknown-model-call preservation.
- Helm chart, canonical benchmark tooling, ADRs, contribution guidance, and demo automation.

The images and OCI chart are published and verified on amd64 and arm64. Canonical paid benchmark
evidence is intentionally tracked separately and is not inferred from release smoke tests.

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
