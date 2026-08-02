# AgentStorm

AgentStorm is a Kubernetes-native platform for load testing, evaluating, and observing AI
Agent workloads. It turns an `AgentTestRun` custom resource into indexed Kubernetes Jobs,
runs a JSONL scenario set with bounded concurrency, and evaluates deterministic quality and
latency thresholds.

> Status: early alpha. Public multi-architecture controller, worker, and Result API images are
> available. M2 provides authenticated durable ingestion, PostgreSQL/object storage, and baseline
> comparison. M3 currently provides deterministic assertion plugins, tool/handoff tracing, explicit
> price-snapshot cost accounting, configurable trace redaction, bounded Prometheus metrics, and a
> Promptfoo durable replay bridge. The development stack persists traces and metrics and provisions
> Grafana. M3 is implemented in `v0.3.0-alpha.1`. M4 fault injection, conservative full-Agent
> retry, Worker-local circuit breaking, durable cancellation, and quality/infrastructure reporting
> are implemented in `v0.4.0-alpha.1`. Event-driven autoscaling remains planned.

## Why this project

Most Agent demos optimize for a single successful conversation. AgentStorm focuses on the
engineering questions that appear after a workflow becomes a service:

- Does quality regress when the prompt, model, tool, or workflow version changes?
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
- Source-built deterministic fault scenarios with stable failure categories and per-attempt evidence.
- Explicit full-Agent retry budgets, Worker-local circuit breakers, and durable partial cancellation.

## Architecture

```mermaid
flowchart LR
    User["kubectl / future API"] --> CR["AgentTestRun CRD"]
    CR --> Controller["Go Controller"]
    Controller --> Config["Generated run ConfigMap"]
    Controller --> Job["Indexed Kubernetes Job"]
    Dataset["Dataset ConfigMap"] --> Job
    Secret["Provider Secret"] --> Job
    Job --> W1["Worker shard 0"]
    Job --> W2["Worker shard N"]
    W1 --> Result["JSONL results + summary"]
    W2 --> Result
    Result --> ResultAPI["Authenticated Result API"]
    ResultAPI --> PostgreSQL["PostgreSQL metadata"]
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
`v0.4.0-alpha.1` image-index digests and need no local image build or registry credentials.

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
ghcr.io/alphasxd/agentstorm-controller@sha256:28b5ba2bc2a1d2a8fa4e00154483981e1c5459229d44cefbc1a9bab0065f2eb0
ghcr.io/alphasxd/agentstorm-worker@sha256:a189cc0735a8f28b8119315f18c4d87fe5713927025529a6007dfce6933e0118
ghcr.io/alphasxd/agentstorm-result-api@sha256:b09a346666c08747948f33089c53cf77a0c6c626f8c0242d3098353f4dbd0162
```

All three images include SPDX SBOM and SLSA provenance attestations. Verify them with the GitHub CLI:

```bash
gh attestation verify \
  oci://ghcr.io/alphasxd/agentstorm-controller@sha256:28b5ba2bc2a1d2a8fa4e00154483981e1c5459229d44cefbc1a9bab0065f2eb0 \
  --repo Alphasxd/AgentStorm

gh attestation verify \
  oci://ghcr.io/alphasxd/agentstorm-worker@sha256:a189cc0735a8f28b8119315f18c4d87fe5713927025529a6007dfce6933e0118 \
  --repo Alphasxd/AgentStorm

gh attestation verify \
  oci://ghcr.io/alphasxd/agentstorm-result-api@sha256:b09a346666c08747948f33089c53cf77a0c6c626f8c0242d3098353f4dbd0162 \
  --repo Alphasxd/AgentStorm
```

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
    image: ghcr.io/alphasxd/agentstorm-worker@sha256:a189cc0735a8f28b8119315f18c4d87fe5713927025529a6007dfce6933e0118
    imagePullPolicy: IfNotPresent
```

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
docs/               architecture, roadmap, and reference designs
```

## Development roadmap

1. Add KEDA-based queue scaling and resource-aware scheduling.
2. Publish Helm charts, reproducible benchmarks, and a public demo.

See [development plan](docs/development-plan.md), [architecture](docs/architecture.md), and
[reference designs](docs/reference-designs.md) for the decision-complete design.

## License

MIT
