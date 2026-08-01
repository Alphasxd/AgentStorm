# AgentStorm

AgentStorm is a Kubernetes-native platform for load testing, evaluating, and observing AI
Agent workloads. It turns an `AgentTestRun` custom resource into indexed Kubernetes Jobs,
runs a JSONL scenario set with bounded concurrency, and evaluates deterministic quality and
latency thresholds.

> Status: early alpha. Public multi-architecture controller and worker images are available. The
> authenticated Result API and PostgreSQL/object-storage path run from source in the development
> overlay; its public image release is still pending. OpenTelemetry export, fault injection, and
> autoscaling remain planned milestones.

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
- JSONL case results plus success rate, error rate, token counters, and P95 latency summary.
- Threshold-based process exit codes suitable for CI and Kubernetes Job status.
- Hardened worker Pods without ServiceAccount tokens or writable root filesystems.
- Authenticated, idempotent result ingestion with PostgreSQL metadata, compressed S3 raw shards,
  paginated case queries, and baseline comparisons in the development stack.

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
    ResultAPI -. planned .-> Telemetry["OTel / Prometheus"]
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
`v0.1.0-alpha.1` image-index digests and need no local image build or registry credentials.

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
ghcr.io/alphasxd/agentstorm-controller@sha256:d8722216fc9f532aa5f7a5d4c7f401ca432df7752f2a9f510f821122f11e1045
ghcr.io/alphasxd/agentstorm-worker@sha256:3dbef77e5ecfd7f73bfa36532aef9fce72ccdbd4590f897c47ee503e1099d8e4
```

Both images include SPDX SBOM and SLSA provenance attestations. Verify them with the GitHub CLI:

```bash
gh attestation verify \
  oci://ghcr.io/alphasxd/agentstorm-controller@sha256:d8722216fc9f532aa5f7a5d4c7f401ca432df7752f2a9f510f821122f11e1045 \
  --repo Alphasxd/AgentStorm

gh attestation verify \
  oci://ghcr.io/alphasxd/agentstorm-worker@sha256:3dbef77e5ecfd7f73bfa36532aef9fce72ccdbd4590f897c47ee503e1099d8e4 \
  --repo Alphasxd/AgentStorm
```

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

The result overlay lives under `config/dev/results`, uses test-only Secrets created by the E2E
script, and is not the released public manifest. See [Result API](docs/result-api.md) for its HTTP and
storage contract.

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
  runner:
    image: ghcr.io/alphasxd/agentstorm-worker@sha256:3dbef77e5ecfd7f73bfa36532aef9fce72ccdbd4590f897c47ee503e1099d8e4
    imagePullPolicy: IfNotPresent
```

Set `spec.cancel: true` to request cancellation. Worker Jobs use `backoffLimit: 0` by default so
an expensive Agent case is not silently replayed after a pod failure.

An `AgentTestRun` is a one-shot execution record. Its target, workload, evaluation, and runner
fields are immutable after creation; create another resource to run a changed configuration.

## Repository layout

```text
api/v1alpha1/       Kubernetes API types
cmd/controller/     controller-manager entrypoint
internal/controller reconciliation and Job construction
config/             CRD, RBAC, deployment, and samples
worker/             Python Agent execution worker
examples/           local run configuration and datasets
docs/               architecture, roadmap, and reference designs
```

## Development roadmap

1. Persist and compare run results through an authenticated result API.
2. Export Agent, model, and tool-call telemetry with OpenTelemetry conventions.
3. Add reusable assertions, provider adapters, and controlled fault injection.
4. Add KEDA-based queue scaling and resource-aware scheduling.
5. Publish Helm charts, reproducible benchmarks, and a public demo.

See [development plan](docs/development-plan.md), [architecture](docs/architecture.md), and
[reference designs](docs/reference-designs.md) for the decision-complete design.

## License

MIT
