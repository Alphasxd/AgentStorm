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

`AgentTestRun.spec` has seven stable boundaries:

| Section | Responsibility |
| --- | --- |
| `target` | Provider, model, endpoint, Secret reference, and optional trusted Adapter entrypoint |
| `workload` | Dataset, parallelism, per-worker concurrency, iterations, and timeout |
| `evaluation` | Run-level quality and latency thresholds |
| `telemetry` | Content omission/redaction policy for optional traces |
| `reliability` | Fault scenario, retry, and Worker-local circuit-breaker policy |
| `runner` | Worker image and pull policy |
| `scheduling` | Indexed or durable queued execution, bounded resources, and placement |

`AgentTestRun.status` is controller-owned and records phase, child Job or ScaledJob, shard/Pod
counters, timestamps, observed generation, and Kubernetes conditions.

## Reconciliation flow

1. Load and validate the `AgentTestRun`.
2. If `spec.cancel=true`, delete the worker Job and mark the run `Cancelled`.
3. Verify that the referenced dataset ConfigMap and key exist.
4. Generate a Secret-free worker configuration ConfigMap.
5. Admit the fixed resource profile against controller safety caps and namespace quota.
6. Create one Indexed Job, or register a durable shard queue and create a KEDA ScaledJob.
7. Map Job counters or durable queue state back into `AgentTestRun.status`.
8. Rely on owner references for child-resource garbage collection when the run is deleted.

The controller treats a run as an execution record. CRD transition rules implemented with Kubernetes
CEL keep target, workload, evaluation, telemetry, reliability, runner, and scheduling fields immutable while leaving `spec.cancel`
declarative. Create a new `AgentTestRun` for a new configuration; no admission webhook is required.

## Worker execution

Each worker receives `AGENTSTORM_SHARD_INDEX` and `AGENTSTORM_SHARD_COUNT`. Dataset row `i` belongs
to a worker when `i % shard_count == shard_index`. Iterations are applied after sharding, so every
case executes the requested number of times globally without duplication between shards.

Concurrency is bounded by a fixed asynchronous task pool per Worker. When durable results and the explicit
distributed-limit controller flag are enabled, every Provider attempt also passes through a
PostgreSQL-backed global and Provider-specific permit
lease, so replica changes cannot bypass configured admission limits. Every provider call has a
timeout and is converted into a `CaseResult`; a provider error does not abort the remaining dataset.

The optional M5 queued path separates shard count from live Worker count. KEDA polls the Result API
for available PostgreSQL queue entries and creates one-shard Jobs up to `scheduling.maxWorkers`.
Workers claim and renew opaque leases; expired leases are eligible for another Worker. See
[event-driven scheduling](scheduling.md) for the admission and failure contract.

Deterministic assertions run before any future model-based grader. This keeps simple correctness
checks cheap, explainable, and reproducible.

The optional `target.adapterEntrypoint` loads a `module:function` factory already present in the
Worker image. Its context contains only provider, model, and base URL. The Controller only validates
and snapshots the string; import, SDK, and domain behavior stay in the Worker. Inline code,
ConfigMap scripts, remote downloads, and passing credential values through the factory context are
intentionally unsupported. Trusted plugin code still shares the Worker process environment.

## Result model

The alpha worker writes one `results.jsonl` and one `summary.json` per shard and prints the summary
to stdout. This is sufficient for local use and Job-level success, but pod-local files are not a
durable cluster result store.

The M2 result path is:

```text
Worker -> authenticated result API -> PostgreSQL run metadata
                                  -> object storage raw events
```

The M3 foundation adds an unauthenticated, NetworkPolicy-restricted `/metrics` endpoint to the
Result API and optional OTLP/HTTP worker traces. Tracing is enabled only when the controller injects
an explicitly configured Collector endpoint. The development overlay persists traces in Tempo and
metrics in Prometheus, then provisions Grafana with Run ID scoped failed-case and provider-error
queries. Adapter lifecycle callbacks add provider-independent local-tool and handoff spans while
keeping SDK objects inside each adapter. Default spans contain correlation IDs, bounded result
categories, latency, and Token usage, but never prompt, output, expected-value, tool payload,
handoff input, or exception-message content. See
[observability](observability.md) for the signal contract and production limitations.

ClickHouse should only be added after event volume and comparison queries justify it. The project
must not introduce a database solely to enlarge the technology list.

The controller first registers the expected shard count and immutable execution snapshot, then
workers upload one idempotent shard
document. The service reserves the shard in PostgreSQL, writes the canonical JSON as a gzip object,
and only then commits case rows and recomputes the run aggregate. A retry with the same key and body
is a no-op; reusing a key with different content is a conflict. A run becomes complete only when
every expected shard receipt is finalized.

M6 adds nullable model-call and exact tool-call counts to each attempt and case. Aggregates remain
`null` for model calls if any Worker cannot confirm the value from public SDK usage data; unknown is
never converted to zero. Tool counts come from the provider-independent lifecycle, and successful-
Agent averages make multi-turn/tool behavior visible alongside throughput.

## Failure semantics

| Failure | Current behavior | Planned improvement |
| --- | --- | --- |
| Dataset missing | Run remains Pending | Event and clearer status reason |
| Provider timeout/error | Case fails; worker continues | Error taxonomy and retry policy |
| Worker pod failure | Indexed Job fails; an expired queued-shard lease can be reclaimed | Durable Worker outbox |
| Controller restart | Reconcile reconstructs state from API objects | Leader-election tests |
| Run cancellation | Durable terminal is attempted, in-flight work stops, partial results flush, then Job or ScaledJob children are deleted | Faster queue-wide cancellation notification |
| Result upload failure | Indexed Job fails; queued Worker exits and its lease expires for reclaim | Durable Worker outbox |

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
