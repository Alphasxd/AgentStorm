# AgentStorm

AgentStorm is a Kubernetes-native distributed load-testing, reliability-testing, and performance-
observability platform for AI Agents. It measures complete multi-turn model/tool execution under
concurrency, rate limits, faults, retries, and autoscaling. Deterministic evaluation is the
correctness guardrail for that load test; AgentStorm is not a customer-service classifier or a
single-prompt benchmark.

> Status: the `v0.6.0` Controller, Worker, Result API, and Helm chart are published. Public
> multi-architecture images are available. M2 provides authenticated durable ingestion,
> PostgreSQL/object storage, and baseline
> comparison. M3 currently provides deterministic assertion plugins, tool/handoff tracing, explicit
> price-snapshot cost accounting, configurable trace redaction, bounded Prometheus metrics, and a
> Promptfoo durable replay bridge. The development stack persists traces and metrics and provisions
> Grafana. M3 is implemented in `v0.3.0-alpha.1`. M4 fault injection, conservative full-Agent
> retry, Worker-local circuit breaking, durable cancellation, and quality/infrastructure reporting
> are implemented in `v0.4.0-alpha.1`. M5 adds an opt-in durable shard queue, KEDA scale-to-zero,
> resource admission, and distributed Provider limits in `v0.5.0-alpha.1`. M6 adds
> trusted Adapter plugins, a 32-incident SRE Agent workload, Agent-call accounting, Helm packaging,
> and reproducible benchmark tooling. The paid canonical performance report and final GitHub Release
> remain gated on a complete, checksummed real-model benchmark; release smokes are not performance
> evidence.

## Why this project

Most Agent demos optimize for a single successful conversation. AgentStorm focuses on the
engineering questions that appear after a workflow becomes a service:

- How many complete Agent executions can the system finish per second, and what happens to P95/P99?
- Does tool/model-call behavior remain correct as KEDA changes Worker count?
- What happens under concurrent load, rate limiting, timeouts, and tool failures?
- Can a run be scheduled, cancelled, retried, observed, and cleaned up declaratively?
- Can test evidence be compared without treating an LLM judge as the only source of truth?

## Current capabilities

- `AgentTestRun` CRD with lifecycle status and declarative cancellation.
- CEL-enforced immutable execution fields so one run cannot mix configuration versions.
- Go controller built with controller-runtime.
- Indexed Kubernetes Job execution with per-pod dataset sharding.
- Dataset readiness checks and Secret-based API key injection.
- Required provider Secret/key readiness gating before worker creation.
- Python worker with bounded asynchronous concurrency.
- No-cost deterministic `fake` provider and optional OpenAI Agents SDK adapter.
- JSONL case results with exact/contains/regex/JSON Schema/tool-path/latency/Python assertions.
- Explicit immutable Provider price snapshots with fixed-precision case, run, and comparison cost.
- Threshold-based process exit codes suitable for CI and Kubernetes Job status.
- Hardened worker Pods without ServiceAccount tokens or writable root filesystems.
- Authenticated, idempotent result ingestion with PostgreSQL metadata, compressed S3 raw shards,
  paginated case queries, and baseline comparisons.
- Default-omit worker OpenTelemetry spans with explicitly gated configurable redaction.
- Bounded Result API RED/Token/cost Prometheus metrics.
- A development-only persistent Tempo/Prometheus stack with a provisioned Grafana run drill-down.
- Optional Promptfoo replay of sensitive durable output without another model call.
- Deterministic fault scenarios with stable failure categories and per-attempt evidence.
- Explicit full-Agent retry budgets, Worker-local circuit breakers, and durable partial cancellation.
- Optional PostgreSQL-backed queued scheduling with KEDA scale-to-zero and bounded live Workers.
- Resource-profile/quota admission plus global and Provider-specific distributed request limits.
- Trusted, image-bundled `module:function` Adapter plugins with no inline or downloaded code.
- Per-attempt, case, run, and comparison model/tool call accounting.
- A realistic 32-case SRE diagnostic Agent using four deterministic read-only tools.

## Architecture

```mermaid
flowchart LR
    User["kubectl / future API"] --> CR["AgentTestRun CRD"]
    CR --> Controller["Go Controller"]
    Controller --> Config["Generated run ConfigMap"]
    Controller --> Job["Indexed Kubernetes Job"]
    Controller -. "queued strategy" .-> ScaledJob["KEDA ScaledJob"]
    KEDA["KEDA operator"] --> ScaledJob
    ResultAPI -. "available shard count" .-> KEDA
    ScaledJob --> Job
    Dataset["Dataset ConfigMap"] --> Job
    Secret["Provider Secret"] --> Job
    Job --> W1["Worker shard 0"]
    Job --> W2["Worker shard N"]
    W1 --> Result["JSONL results + summary"]
    W2 --> Result
    Result --> ResultAPI["Authenticated Result API"]
    ResultAPI --> PostgreSQL["PostgreSQL metadata"]
    PostgreSQL --> Queue["Durable shard queue + permits"]
    ResultAPI --> ObjectStore["Compressed S3 shards"]
    W1 -. optional OTLP .-> Telemetry["OpenTelemetry Collector"]
    W2 -. optional OTLP .-> Telemetry
    Telemetry --> Tempo["Tempo traces"]
    ResultAPI --> Prometheus["Prometheus metrics"]
    Tempo --> Grafana["Grafana drill-down"]
    Prometheus --> Grafana
```

The controller never serializes API keys into generated ConfigMaps. Provider credentials are
injected directly from a Kubernetes Secret into worker pods.

## Local quickstart

The fake provider uses only the Python standard library.

```bash
make worker-local
```

Run the full fake SRE Agent lifecycle (32 incidents, 128 tool calls, deterministic assertions):

```bash
make sre-local
```

Validate the complete benchmark shape without a model bill (384 capacity executions plus the
32-case reliability run and checksummed artifacts):

```bash
make benchmark-fake
```

Results are written to:

```text
.out/local/results.jsonl
.out/local/summary.json
```

Run all tests:

```bash
make test
```

Run CRD validation and status-subresource tests against an isolated API server:

```bash
make test-envtest
```

Build the controller:

```bash
make build
```

## Released Kubernetes quickstart

Prerequisite: `kubectl` access to a Kubernetes cluster. The public manifests use immutable
`v0.6.0` image-index digests and need no local image build or registry credentials.

```bash
kubectl apply -k config/default
kubectl apply --dry-run=server -f config/samples/agentstorm_v1alpha1_agenttestrun.yaml
kubectl apply -f config/samples/agentstorm_v1alpha1_agenttestrun.yaml
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded \
  agenttestrun/agentstorm-demo --timeout=120s
kubectl get agenttestrun/agentstorm-demo
```

The released images are public for anonymous pulls:

```text
ghcr.io/alphasxd/agentstorm-controller@sha256:b37ae7edcbc5da61f5f1680f3aca5d5d6311c6a2b7745ebea088dde3279428ca
ghcr.io/alphasxd/agentstorm-worker@sha256:a06b8416f11355ddcfa470aa7932d9ce02546d8959a7ebee4c2ca2a53938cc41
ghcr.io/alphasxd/agentstorm-result-api@sha256:8dec868c6e4c118fdf5d0afb7645400422244c532464e5bd10d038887ebb70dd
```

All three images include SPDX SBOM and SLSA provenance attestations. Verify them with the GitHub CLI:

```bash
gh attestation verify \
  oci://ghcr.io/alphasxd/agentstorm-controller@sha256:b37ae7edcbc5da61f5f1680f3aca5d5d6311c6a2b7745ebea088dde3279428ca \
  --repo Alphasxd/AgentStorm

gh attestation verify \
  oci://ghcr.io/alphasxd/agentstorm-worker@sha256:a06b8416f11355ddcfa470aa7932d9ce02546d8959a7ebee4c2ca2a53938cc41 \
  --repo Alphasxd/AgentStorm

gh attestation verify \
  oci://ghcr.io/alphasxd/agentstorm-result-api@sha256:8dec868c6e4c118fdf5d0afb7645400422244c532464e5bd10d038887ebb70dd \
  --repo Alphasxd/AgentStorm
```

### Released Helm chart

The source chart defaults to a namespace-scoped Controller and CRD. Result API is optional;
PostgreSQL, S3, and KEDA are always operator-provided. Values contain only Secret names and keys,
never token/password values:

```bash
helm show chart oci://ghcr.io/alphasxd/charts/agentstorm --version 0.6.0
helm install agentstorm oci://ghcr.io/alphasxd/charts/agentstorm \
  --version 0.6.0 --namespace agentstorm-system --create-namespace
```

The published chart digest is
`sha256:ad20aa4da8f389f21f01ebcfe807090318f51cc4fbb833403ff2e4d2f867b509`.
The source chart remains available under `charts/agentstorm` for linting and local changes.
The [v0.6.0 verification record](docs/release-v0.6.0.md) captures the tag, digests, attestations,
and amd64/arm64 smoke evidence separately from the pending real-model performance report.

### Released durable result stack

The public namespace-scoped result stack needs storage and API credentials supplied as Secrets.
This example generates disposable values locally; none are committed to the repository:

```bash
kubectl apply -k config/namespace

POSTGRES_PASSWORD="$(openssl rand -hex 24)"
S3_SECRET_KEY="$(openssl rand -hex 24)"
WRITE_TOKEN="$(openssl rand -hex 24)"
READ_TOKEN="$(openssl rand -hex 24)"

kubectl -n agentstorm-system create secret generic agentstorm-result-storage \
  --from-literal=database-url="postgres://agentstorm:${POSTGRES_PASSWORD}@agentstorm-postgres:5432/agentstorm?sslmode=disable" \
  --from-literal=postgres-password="$POSTGRES_PASSWORD" \
  --from-literal=s3-access-key=agentstorm \
  --from-literal=s3-secret-key="$S3_SECRET_KEY"
kubectl -n agentstorm-system create secret generic agentstorm-result-auth \
  --from-literal=write-token="$WRITE_TOKEN" \
  --from-literal=read-token="$READ_TOKEN"

kubectl apply -k config/results
kubectl -n agentstorm-system rollout status deployment/agentstorm-result-api --timeout=180s
kubectl apply -n agentstorm-system -f config/samples/agentstorm_v1alpha1_agenttestrun.yaml
kubectl wait -n agentstorm-system --for=jsonpath='{.status.phase}'=Succeeded \
  agenttestrun/agentstorm-demo --timeout=180s
```

`config/results` is an alpha reference deployment with single-replica PostgreSQL and MinIO, two
1 GiB persistent-volume claims, and restrictive NetworkPolicies. It is not a high-availability
production storage design; adapt storage classes, backups, CNI rules, and credentials before using
it beyond evaluation.

## Local development E2E

Prerequisites: `kubectl`, Docker, and either OrbStack Kubernetes or `kind`. This path builds the
current source as local `agentstorm-controller:dev` and `agentstorm-worker:dev` images, deploys the
controller, runs a fresh two-shard fake workload, and checks Secret gating, garbage collection, and
cancellation.

```bash
# Active OrbStack context
make e2e-local

# Or create/reuse a local kind cluster named agentstorm
CLUSTER_PROVIDER=kind make e2e-local
```

The script refuses arbitrary Kubernetes contexts. Set `KEEP_E2E_RESOURCES=false` for CI cleanup.
For a single-namespace runtime with Role/RoleBinding and worker NetworkPolicies, use:

```bash
DEPLOYMENT_PROFILE=namespace make e2e-local
```

Run the full source-built M2 development stack, including PostgreSQL, MinIO, Result API Secret
gating, two durable runs, case reads, and a baseline comparison:

```bash
CLUSTER_PROVIDER=kind make e2e-results-local
```

The released M5 gate exercises the durable PostgreSQL shard queue, KEDA scale-to-zero,
resource-profile/quota admission, and distributed Provider concurrency/rate permits. It installs
KEDA 2.20 into the selected disposable local cluster when needed and uses only the fake Provider:

```bash
CLUSTER_PROVIDER=kind make e2e-keda-local
```

To verify the released images without building or loading local images:

```bash
CLUSTER_PROVIDER=kind E2E_TIMEOUT=300s SKIP_IMAGE_BUILD=true LOAD_LOCAL_IMAGES=false \
  CONTROLLER_IMAGE=ghcr.io/alphasxd/agentstorm-controller@sha256:b37ae7edcbc5da61f5f1680f3aca5d5d6311c6a2b7745ebea088dde3279428ca \
  WORKER_IMAGE=ghcr.io/alphasxd/agentstorm-worker@sha256:a06b8416f11355ddcfa470aa7932d9ce02546d8959a7ebee4c2ca2a53938cc41 \
  RESULT_API_IMAGE=ghcr.io/alphasxd/agentstorm-result-api@sha256:8dec868c6e4c118fdf5d0afb7645400422244c532464e5bd10d038887ebb70dd \
  make e2e-keda-local
```

See [event-driven scheduling](docs/scheduling.md) for the immutable API, release quickstart, and
safety boundaries. The queue-status endpoint must be network-isolated; PostgreSQL and Result API
form part of the scheduling control plane, and distributed limits are not billing guarantees.

The local result overlay lives under `config/dev/results`; `config/dev/telemetry` adds a
digest-pinned Collector, Tempo, Prometheus, and provisioned Grafana dashboard. Source-image E2E
persists telemetry to PVCs, proves it remains queryable after backend restarts, and verifies default
omit plus explicitly redacted content. The E2E uses test-only Secrets and replaces the released
AgentStorm component images with source-built `:dev` images. See [Result API](docs/result-api.md) and
[observability](docs/observability.md) for the storage and signal contracts.

### Reliability validation

M4 ships in the released images. The no-cost result-stack E2E uses only the fake Provider to
exercise latency, timeout, HTTP, malformed-response, rate-limit, and tool failures. It also checks
deterministic selection across sharding and concurrency, conservative and explicitly ambiguous
retries, circuit transitions, quality versus infrastructure reporting, and durable cancellation:

```bash
CLUSTER_PROVIDER=kind ENABLE_RELIABILITY_E2E=true make e2e-results-local
```

To enable a deterministic scenario for a released Run, apply the sample ConfigMap and add this
fragment to `spec`:

```bash
kubectl -n agentstorm-system apply -f config/samples/fault-scenario.yaml
```

```yaml
reliability:
  seed: 42
  scenarioRef:
    name: agentstorm-fault-scenario
    key: scenario.json
  retry:
    maxAttempts: 3
    allowAmbiguousRetries: false
  circuitBreaker:
    failureThreshold: 5
    openDurationMs: 30000
```

Reliability is configured under immutable `spec.reliability`; the scenario document is stored in a
separate ConfigMap and snapshotted by the Controller before a Job starts. A retry repeats the entire
Agent execution and can duplicate model charges and tool side effects. Ambiguous timeout, HTTP 5xx,
and malformed-response retries therefore require an explicit opt-in. Circuit breakers are scoped to
one Worker process, not the whole Run. See the [FaultScenario contract](docs/fault-scenarios.md).

Setting `spec.cancel: true` first attempts to persist a `cancelled` terminal record, then removes the
Job. Completed cases remain readable as a partial run; cancellation still completes when the Result
API is unavailable, with the persistence failure exposed through the CR condition and Event.

### Optional Promptfoo replay

A completed run can be replayed through Promptfoo without issuing another model request when the
controller was explicitly configured to upload sensitive durable output. The generator accepts the
read token only from `AGENTSTORM_RESULT_READ_TOKEN`; neither the token nor model outputs are written
to its config. It maps exact, contains, regex, and JSON Schema assertions, while latency, tool-path,
and trusted Python outcomes remain AgentStorm-authored metadata.

```bash
export AGENTSTORM_RESULT_READ_TOKEN="$READ_TOKEN"
python3 integrations/promptfoo/generate.py \
  --result-api-url http://127.0.0.1:18080 \
  --run-id "$RUN_ID" \
  --dataset examples/datasets/basic.jsonl \
  --output .out/promptfoo.json

PROMPTFOO_DISABLE_TELEMETRY=true PROMPTFOO_PYTHON=python3 \
  npx --yes promptfoo@0.121.19 eval --config .out/promptfoo.json --no-cache
```

The default Result API path omits output, so replay fails safely unless sensitive durable results
were enabled before execution. See the [Promptfoo integration guide](integrations/promptfoo/README.md).

Deploying either profile removes the other profile's runtime RBAC so permissions cannot accumulate
when switching modes. The CRD definition is installed cluster-wide, while `AgentTestRun` resources
remain namespaced in both modes.

## `AgentTestRun` example

```yaml
apiVersion: agentstorm.io/v1alpha1
kind: AgentTestRun
metadata:
  name: smoke
spec:
  target:
    provider: fake
    pricing:
      inputUSDPerMillionTokens: "2.5"
      outputUSDPerMillionTokens: "10"
  workload:
    datasetRef:
      name: agentstorm-demo-dataset
      key: cases.jsonl
    parallelism: 2
    concurrencyPerWorker: 4
    iterations: 1
    timeoutSeconds: 300
  evaluation:
    minSuccessRate: 1
    maxErrorRate: 0
    maxP95LatencyMs: 1000
  telemetry:
    contentMode: omit
  runner:
    image: ghcr.io/alphasxd/agentstorm-worker@sha256:a06b8416f11355ddcfa470aa7932d9ce02546d8959a7ebee4c2ca2a53938cc41
    imagePullPolicy: IfNotPresent
```

An image-bundled trusted Adapter can be selected without adding domain logic to the generic
OpenAI Adapter:

```yaml
target:
  provider: openai-agents
  model: gpt-5.6-luna
  adapterEntrypoint: agentstorm_worker.benchmarks.sre:create_adapter
```

The factory context contains only provider, model, and base URL. It never receives API keys,
pricing, or Kubernetes Secret objects through that interface; trusted plugin code still shares the
Worker process environment and must already exist in the immutable Worker image.

Set `spec.cancel: true` to request cancellation. Worker Jobs use `backoffLimit: 0` by default so
an expensive Agent case is not silently replayed after a pod failure.

An `AgentTestRun` is a one-shot execution record. Its target, workload, evaluation, telemetry, and
runner fields are immutable after creation; create another resource to run a changed configuration.

## Repository layout

```text
api/v1alpha1/       Kubernetes API types
cmd/controller/     controller-manager entrypoint
internal/controller reconciliation and Job construction
config/             CRD, RBAC, deployment, and samples
worker/             Python Agent execution worker
integrations/       Optional external evaluator bridges
examples/           local run configuration and datasets
benchmarks/          reproducible SRE benchmark methodology
charts/              namespace-scoped Helm distribution
docs/               architecture, roadmap, and reference designs
```

## Development roadmap

1. Run the fixed real SRE capacity/reliability matrix with the published v0.6 digests.
2. Publish checksummed evidence and the demo asset, close M6, and create the final GitHub Release.

See [development plan](docs/development-plan.md), [architecture](docs/architecture.md), and
[reference designs](docs/reference-designs.md) for the decision-complete design.

## License

MIT
