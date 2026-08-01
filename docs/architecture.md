# Architecture

## Design goals

AgentStorm separates orchestration from execution:

- Kubernetes owns desired state, scheduling, pod placement, and garbage collection.
- The controller owns run validation, child resources, cancellation, and lifecycle status.
- Workers own provider calls, per-case evaluation, and result production.
- The result service owns durable aggregation and run comparison.

This prevents model-specific SDK details from leaking into the Kubernetes controller and keeps
worker adapters independently testable.

## API model

`AgentTestRun.spec` has four stable boundaries:

| Section | Responsibility |
| --- | --- |
| `target` | Provider, model, endpoint, and Secret reference |
| `workload` | Dataset, parallelism, per-worker concurrency, iterations, and timeout |
| `evaluation` | Run-level quality and latency thresholds |
| `runner` | Worker image and pull policy |

`AgentTestRun.status` is controller-owned and records phase, child Job, pod counters, timestamps,
observed generation, and Kubernetes conditions.

## Reconciliation flow

1. Load and validate the `AgentTestRun`.
2. If `spec.cancel=true`, delete the worker Job and mark the run `Cancelled`.
3. Verify that the referenced dataset ConfigMap and key exist.
4. Generate a Secret-free worker configuration ConfigMap.
5. Create one Indexed Job with `parallelism == completions`.
6. Map Job counters and terminal conditions back into `AgentTestRun.status`.
7. Rely on owner references for child-resource garbage collection when the run is deleted.

The controller treats a run as an execution record. CRD transition rules implemented with Kubernetes
CEL keep target, workload, evaluation, and runner fields immutable while leaving `spec.cancel`
declarative. Create a new `AgentTestRun` for a new configuration; no admission webhook is required.

## Worker execution

Each worker receives `AGENTSTORM_SHARD_INDEX` and `AGENTSTORM_SHARD_COUNT`. Dataset row `i` belongs
to a worker when `i % shard_count == shard_index`. Iterations are applied after sharding, so every
case executes the requested number of times globally without duplication between shards.

Concurrency is bounded by one `asyncio.Semaphore` per worker. Every provider call has a timeout and
is converted into a `CaseResult`; a provider error does not abort the remaining dataset.

Deterministic assertions run before any future model-based grader. This keeps simple correctness
checks cheap, explainable, and reproducible.

## Result model

The alpha worker writes one `results.jsonl` and one `summary.json` per shard and prints the summary
to stdout. This is sufficient for local use and Job-level success, but pod-local files are not a
durable cluster result store.

The M2 result path is:

```text
Worker -> authenticated result API -> PostgreSQL run metadata
                                  -> object storage raw events
                                  -> Prometheus / OpenTelemetry aggregates
```

ClickHouse should only be added after event volume and comparison queries justify it. The project
must not introduce a database solely to enlarge the technology list.

Result API clients first register the expected shard count, then upload one idempotent shard
document. The service reserves the shard in PostgreSQL, writes the canonical JSON as a gzip object,
and only then commits case rows and recomputes the run aggregate. A retry with the same key and body
is a no-op; reusing a key with different content is a conflict. A run becomes complete only when
every expected shard receipt is finalized.

## Failure semantics

| Failure | Current behavior | Planned improvement |
| --- | --- | --- |
| Dataset missing | Run remains Pending | Event and clearer status reason |
| Provider timeout/error | Case fails; worker continues | Error taxonomy and retry policy |
| Worker pod failure | Job fails, no automatic replay | Explicit idempotent retry command |
| Controller restart | Reconcile reconstructs state from API objects | Leader-election tests |
| Run cancellation | Job deleted, run marked Cancelled | Grace period and partial result flush |
| Result upload failure | Worker exits non-zero and the Job fails | Durable worker outbox |

Expensive model calls make hidden retries dangerous. Future retries must use the idempotency key
`run_id / case_id / iteration` and distinguish transport failures from completed model calls.

## Security boundaries

- API keys remain in Kubernetes Secrets and never enter generated ConfigMaps.
- Missing non-optional provider Secrets or keys keep a run Pending instead of creating a failing Pod.
- Secret readiness checks use the uncached API reader, so runtime RBAC grants `get` without broad
  `list` or `watch` access to Secrets.
- Controller logs must not include prompts, outputs, tool arguments, or Secret values.
- Worker Pods run as non-root, drop all capabilities, disable ServiceAccount token mounting, and use
  a read-only root filesystem with a dedicated EmptyDir for ephemeral results.
- Raw trace content is opt-in because prompts and tool results may contain sensitive data.
- Durable case uploads omit output and full error text unless the controller's explicit sensitive
  result flag is enabled.
- Arbitrary shell, browser, or MCP tools are out of scope until a sandbox policy is defined.
- The default local profile uses cluster-wide RBAC. The `config/namespace-scoped` profile limits the
  controller cache and permissions to `agentstorm-system` with Role/RoleBinding resources.
- NetworkPolicies deny worker ingress and restrict controller/worker egress to DNS and HTTPS. Custom
  provider ports require an explicit policy extension.
