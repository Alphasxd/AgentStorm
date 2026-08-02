# Development plan

The project is deliberately split into vertical milestones. Every milestone must end with a runnable
demo, tests, and evidence that can be shown in a README or interview.

## M0: Alpha foundation — implemented

- Define the `AgentTestRun` v1alpha1 API and status model.
- Reconcile generated ConfigMaps and Indexed Jobs.
- Support dataset gating, cancellation, owner references, and Job status projection.
- Enforce one-shot execution fields with CRD CEL transition rules.
- Implement a sharded, concurrent worker with fake and OpenAI Agents SDK adapters.
- Produce deterministic assertions, JSONL results, and a thresholded summary.
- Run Go and Python tests in CI.

Acceptance criteria:

- `make test` passes without API keys or a Kubernetes cluster.
- A missing dataset never creates a worker Job.
- Two indexed workers receive distinct dataset shards.
- A failed threshold returns a non-zero worker exit code.

## M1: Reproducible local cluster demo — implemented

- [x] Add a kind/OrbStack bootstrap script and local image loading.
- [x] Add envtest coverage for CRD validation and status subresources.
- [x] Add controller tests for cancellation, Job success/failure, collision-safe truncation, and missing Secrets.
- [x] Add namespace-scoped deployment mode and NetworkPolicies.
- [x] Publish public multi-architecture images and pin public manifests to immutable image-index digests.

Acceptance criteria:

- [x] One command creates or reuses a local cluster, deploys AgentStorm, runs the fake dataset, and reaches `Succeeded`.
- [x] Deleting a run garbage-collects its ConfigMap and Job.
- [x] Cancelling a running test reaches `Cancelled` without creating a replacement Job.
- [x] Published digests run without local build or image loading on amd64 kind and arm64 OrbStack.

The acceptance path is automated by `make e2e-local` and uses the no-cost fake provider. Release
`v0.1.0-alpha.1` publishes immutable multi-architecture images, and public manifests reference their
image-index digests.

## M2: Durable result service and comparison — implemented

- [x] Add a Go HTTP service for run registration, shard result ingestion, and summary queries.
- [x] Store run metadata and summaries in PostgreSQL.
- [x] Store raw case events in object storage or compressed files before considering ClickHouse.
- [x] Require idempotency keys for every shard and case result.
- [x] Add baseline-vs-candidate comparison for prompts, models, tools, and workflow versions.

Acceptance criteria:

- [x] Duplicate shard uploads do not duplicate counters.
- [x] A run is complete only after all expected shard summaries arrive.
- [x] The API returns quality, latency, token, and error deltas between two runs.

Release `v0.2.0-alpha.1` publishes the controller, worker, and Result API as immutable
multi-architecture images. `config/results` is the namespace-scoped public reference stack; its
single-replica PostgreSQL and MinIO workloads are intended for alpha evaluation, not production HA.

## M3: Agent observability and evaluation — implemented

- Add OpenTelemetry spans for run, case, model generation, tool call, handoff, and evaluator.
  - [x] Add opt-in run, case, provider invocation, and deterministic evaluator spans.
  - [x] Add provider-independent, content-safe tool and handoff lifecycle events and spans.
- Export RED metrics and token/cost counters to Prometheus.
  - [x] Export bounded Result API RED, ingestion, case-result, and Token metrics.
  - [x] Add Result API recording rules and a persistent development dashboard stack.
  - [x] Add provider cost configuration and counters.
- [x] Add assertion plugins: JSON schema, exact/contains/regex, tool path, latency, and custom Python.
- [x] Integrate Promptfoo as an optional external evaluator rather than coupling its internals.
- [x] Add redaction controls for prompts, outputs, tool arguments, and trace attributes.

Acceptance criteria:

- [x] A Grafana dashboard can drill from a run into failed cases and provider errors.
- [x] Deterministic assertions work without an LLM judge.
- [x] Sensitive content is absent from default traces and logs.

Release `v0.3.0-alpha.1` publishes the controller, worker, and Result API as verified immutable
multi-architecture images. Public manifests pin their image-index digests. The completed M3 path
includes provider-independent local-tool/handoff tracing, deterministic assertions, explicit
price-snapshot cost accounting, redaction controls, and optional no-model Promptfoo replay.
`config/dev/telemetry` runs single-replica Tempo, Prometheus, and Grafana workloads for local
evaluation; they are not a production HA topology.

## M4: Fault injection and reliability testing — implemented

- [x] Add adapter middleware for latency, timeout, HTTP error, malformed response, rate-limit, and tool-error injection.
- [x] Add Provider retry budgets, Worker-local circuit-breaker experiments, and durable cancellation propagation.
- [x] Add reproducible random seeds and strictly validated, immutable scenario snapshots.
- [x] Separate harness, Provider, tool, and evaluation failures in durable reports and comparisons.

Acceptance criteria:

- [x] Every injected fault appears under a stable error category.
- [x] A rerun with the same seed selects the same failure points across shard and concurrency layouts.
- [x] Reports distinguish quality regression from infrastructure instability.

Release `v0.4.0-alpha.1` publishes verified immutable Controller, Worker, and Result API
multi-architecture images. The no-cost published-image gates cover all six faults, safe and
ambiguous retry policy, Token/cost completeness, circuit open/reject/half-open/close transitions,
partial cancellation, and quality/infrastructure aggregates on amd64 and arm64. Public manifests
pin the verified v0.4 image-index digests.

## M5: Event-driven scaling and scheduling

- Queue pending shards and add KEDA ScaledJob support.
- Introduce resource profiles, node selectors, tolerations, and per-namespace quotas.
- Add global and provider-specific concurrency/rate limits.
- Add admission validation for unsafe parallelism and missing resource bounds.

Acceptance criteria:

- Worker capacity scales from zero based on pending shards.
- Global rate limits remain valid while worker replicas change.
- A quota violation is rejected before model calls begin.

## M6: Stable open-source release

- Maintain stable image releases and publish a Helm chart, changelog, architecture decision records,
  and demo video.
- Add one realistic public Agent dataset with deterministic expected behavior.
- Publish benchmark methodology and raw reproducible result artifacts.
- Mark beginner-friendly issues and document the adapter/plugin contribution path.

## What not to build yet

- A general-purpose visual Agent builder.
- A proprietary prompt management product.
- A complete LangGraph or OpenAI Agents SDK replacement.
- A Kubernetes scheduler plugin.
- A large frontend before the API and result semantics stabilize.

These features add surface area without strengthening the initial reliability-testing thesis.
