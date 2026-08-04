# Event-driven scheduling

M5 adds an optional durable shard queue and KEDA `ScaledJob` execution path while preserving the
existing Indexed Job behavior. It is released in `v0.5.0-alpha.1`; all three components must use
the matching immutable release digests when queued scheduling is enabled.

## Run contract

Scheduling is an immutable part of `AgentTestRun.spec`:

```yaml
spec:
  workload:
    parallelism: 64        # durable shard count
    concurrencyPerWorker: 2
  scheduling:
    strategy: keda         # indexed or keda; default indexed
    maxWorkers: 8          # maximum simultaneous KEDA Jobs
    resourceProfile: small # small, medium, or large
    nodeSelector:
      agentstorm.io/pool: evaluation
    tolerations:
      - key: evaluation
        operator: Exists
        effect: NoSchedule
```

For `indexed`, `workload.parallelism` is both shard count and Job parallelism. For `keda`, it remains
the immutable shard count while `scheduling.maxWorkers` caps live one-shard Jobs. Each Job claims one
available shard from PostgreSQL, renews a 45-second lease while running, and must present the opaque
lease token when uploading its result. An expired lease makes the shard available again. Tokens are
stored only as SHA-256 hashes and never enter a generated ConfigMap, status, Event, or fixture.
If the Result API becomes unavailable while a queued Worker claims, renews, or completes a shard,
that Worker exits without writing a Run terminal state; the lease expires and another Job can claim
the shard. Configuration, adapter construction, and other genuine harness failures retain the M4
`harness_failed` terminal behavior.

The built-in resource profiles are fixed controller policy:

| Profile | CPU request / limit | Memory request / limit |
| --- | --- | --- |
| `small` | `100m` / `1` | `128Mi` / `512Mi` |
| `medium` | `500m` / `2` | `512Mi` / `2Gi` |
| `large` | `2` / `8` | `2Gi` / `8Gi` |

The Controller rejects more than 10,000 shards, more than its configured Worker cap, or an aggregate
Worker concurrency above its safety cap. It also checks namespace `ResourceQuota` capacity for
Pods, Jobs, CPU, and memory before creating the Worker ConfigMap or ScaledJob. With
`--require-runner-resource-quota`, a namespace without a quota remains `Pending` with
`SchedulingReady=False`. Admission becomes fixed when the Job or ScaledJob child is created, so
later namespace usage does not change an already running Run. KEDA admission also reserves quota headroom for the
configured successful and failed Job history; Indexed execution counts one Job object plus all
parallel Worker Pods.

## KEDA deployment

KEDA support is disabled by default. Queued scheduling requires:

```text
--enable-keda
--keda-metrics-api-url=http://agentstorm-result-api.agentstorm-system.svc.cluster.local:8080
--result-api-url=http://agentstorm-result-api.agentstorm-system.svc.cluster.local:8080
```

Add `--enable-distributed-limits` when the Result API is configured with global or Provider caps.
This explicit switch avoids adding permit traffic to durable Indexed or KEDA runs that do not need
distributed admission control; the M5 development overlay enables it for the limit gate.

Optional safety flags are `--max-workers-per-run` (default `100`), `--max-in-flight-cases`
(default `1000`), `--require-runner-resource-quota`, and `--keda-polling-interval` (default two
seconds). The metrics URL must be reachable by the KEDA operator and cannot contain credentials,
query parameters, or a fragment.

The Controller creates a per-Run [KEDA ScaledJob](https://keda.sh/docs/2.20/reference/scaledjob-spec/)
using KEDA's [metrics-api scaler](https://keda.sh/docs/2.20/scalers/metrics-api/), `accurate` scaling
strategy, scale-to-zero, and `available` queue depth as its external value. The queue-status endpoint
is intentionally unauthenticated because KEDA polls it directly; it exposes only bounded counts and
terminal state. Production deployments must restrict it to the Controller and KEDA namespaces with
NetworkPolicy or an equivalent network boundary. Claims, renewals, permits, result uploads, and
terminal writes still require the Result API writer token.

The no-cost local and release-verification paths install KEDA 2.20 when needed:

```bash
CLUSTER_PROVIDER=kind make e2e-keda-local
```

That no-cost gate installs KEDA when needed, starts from zero Worker Jobs, processes an eight-shard
fake-provider Run with at most four Workers, verifies scale-down, exercises the distributed limit,
and proves an oversized Run is rejected by quota before a ScaledJob exists.

## Distributed Provider limits

When durable results and `--enable-distributed-limits` are enabled, every Provider attempt acquires
a PostgreSQL-backed lease before calling the adapter. This applies to both Indexed and KEDA
scheduling, so changing Worker replica count cannot bypass the policy. Configure the Result API
with:

| Variable | Default | Meaning |
| --- | --- | --- |
| `AGENTSTORM_GLOBAL_MAX_CONCURRENCY` | `0` | Maximum live Provider attempts across all runs; zero disables the cap |
| `AGENTSTORM_GLOBAL_REQUESTS_PER_MINUTE` | `0` | Sliding one-minute global start limit; zero disables it |
| `AGENTSTORM_PROVIDER_LIMITS_JSON` | `{}` | Provider-specific `max_concurrency` and `requests_per_minute` caps |
| `AGENTSTORM_PERMIT_LEASE_SECONDS` | `30` | Permit lease duration, from 10 through 300 seconds |

Global and Provider-specific limits are both enforced; the effective admission must satisfy every
configured cap. A saturated cap returns `429` with a bounded retry delay, and the Worker waits within
the existing case timeout without consuming an M4 retry attempt. Permit expiry is fail-closed: the
in-flight call is cancelled and recorded as a harness failure because its accounting can no longer
be proven. A non-capacity Result API failure during permit acquisition is likewise classified as a
harness failure, never as a Provider failure. Released and expired permit bookkeeping is retained for one hour for idempotent retries,
then pruned during admission. The Controller flag is intentionally explicit: without it, durable
result ingestion does not add a database round trip to every Provider attempt. When enabled, set at
least one non-zero Result API cap; zero means unlimited. Limits are admission controls, not billing
guarantees—Provider-side retries or work that
continues after transport cancellation may still incur cost.

M5 does not add a Kubernetes scheduler plugin, global circuit breaker, distributed tool semaphore,
or production queue HA. PostgreSQL and the Result API are part of the scheduling control plane and
must be operated with appropriate availability, backups, and capacity before production use.
