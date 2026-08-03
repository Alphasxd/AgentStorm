# Result API

The Result API is the durable M2 boundary between execution workers and result consumers. It stores
run metadata, shard receipts, normalized case indexes, and aggregates in PostgreSQL. The canonical
shard document is compressed and written to S3-compatible object storage before a shard is marked
complete.

## HTTP contract

Health, metrics, and bounded queue-status endpoints are unauthenticated. All mutation and result-read
endpoints require `Authorization: Bearer <token>`. Writer and reader tokens are separate so worker
Pods do not receive result-read access. Kubernetes deployments must restrict metrics and queue
status access with NetworkPolicy; KEDA needs queue-status access but never receives a writer token.

| Method | Path | Token | Purpose |
| --- | --- | --- | --- |
| `GET` | `/healthz` | none | Process liveness |
| `GET` | `/readyz` | none | PostgreSQL and object-store readiness |
| `GET` | `/metrics` | none | Prometheus RED, result, reliability, token, and cost counters |
| `PUT` | `/v1/runs/{runID}` | writer | Register immutable run metadata and expected shards |
| `PUT` | `/v1/runs/{runID}/terminal` | writer | Persist `cancelled` or `harness_failed` without exception text |
| `PUT` | `/v1/runs/{runID}/shards/{index}` | writer | Persist one complete shard document |
| `GET` | `/v1/runs/{runID}/queue` | none | Return bounded pending/available/leased/completed counts for KEDA and Controller polling |
| `POST` | `/v1/runs/{runID}/queue/claims` | writer | Claim one available queued shard with an opaque lease |
| `POST` | `/v1/runs/{runID}/queue/shards/{index}/renew` | writer | Renew an active queued-shard lease |
| `POST` | `/v1/runs/{runID}/permits` | writer | Acquire a distributed Provider concurrency/rate permit |
| `POST` | `/v1/runs/{runID}/permits/{permitID}/renew` | writer | Renew a Provider permit lease |
| `POST` | `/v1/runs/{runID}/permits/{permitID}/release` | writer | Idempotently release Provider capacity |
| `GET` | `/v1/runs/{runID}` | reader | Read status, received shards, and aggregates |
| `GET` | `/v1/runs/{runID}/cases` | reader | Page through cases; `failed=true` filters failures |
| `GET` | `/v1/comparisons?baseline={id}&candidate={id}` | reader | Compare two complete runs |

The registration key is `run/{runID}`. A shard key is `run/{runID}/shard/{index}`. Every case in a
shard carries `run/{runID}/case/{urlEncodedCaseID}/iteration/{iteration}`. Sending the same key and
canonical body again returns success without changing counters. Reusing an identity with different
content returns HTTP `409`.

A terminal key is `run/{runID}/terminal/{status}` and accepts only a stable `reason_code` plus
`cancelled` or `harness_failed`. Repeating the same terminal request is a no-op; changing the reason
or terminal status conflicts, and a complete run cannot be moved backwards. Completed partial
shards may arrive after a terminal update. `GET /v1/runs/{runID}` then reports `partial: true` and a
structured `terminal_reason`; Promptfoo and baseline comparison continue to require `complete`.

Run comparison reports candidate-minus-baseline deltas for success and failure rates, quality and
infrastructure failure counts/rates, attempt and retry effectiveness, injected faults, circuit
rejections, P50/P95/P99 latency, Token counts, and derived USD cost. Percentage deltas are `null`
when the baseline value is zero. Cost fields are fixed-precision decimal strings and remain `null`
when a run did not register an explicit `spec.target.pricing` snapshot; AgentStorm never guesses
current Provider prices.
Case pages also return ordered tool paths, structured assertion outcomes, attempt history, stable
failure category/code, and usage completeness. Assertion values and custom messages remain omitted
unless sensitive result upload was explicitly enabled. A case and run cost remains `null` whenever
any attempt has unknown Token usage, while known Token totals are still retained.

The optional [Promptfoo replay bridge](../integrations/promptfoo/README.md) reads only these public
run and case endpoints. It refuses collecting runs and case pages without durable output, and it
never triggers a Provider or model request. Read access remains protected by the Result API bearer
token; the generated Promptfoo config contains neither that token nor saved model output.

## Configuration

The service reads configuration from environment variables:

| Variable | Required | Description |
| --- | --- | --- |
| `AGENTSTORM_DATABASE_URL` | yes | PostgreSQL connection URL |
| `AGENTSTORM_S3_ENDPOINT` | yes | S3 endpoint without a URL scheme |
| `AGENTSTORM_S3_ACCESS_KEY` | yes | S3 access key |
| `AGENTSTORM_S3_SECRET_KEY` | yes | S3 secret key |
| `AGENTSTORM_S3_BUCKET` | no | Defaults to `agentstorm-results` |
| `AGENTSTORM_S3_REGION` | no | Defaults to `us-east-1` |
| `AGENTSTORM_S3_USE_SSL` | no | Defaults to `false` |
| `AGENTSTORM_RESULT_WRITE_TOKEN` | yes | Worker bearer token |
| `AGENTSTORM_RESULT_READ_TOKEN` | yes | Query bearer token |
| `AGENTSTORM_LISTEN_ADDR` | no | Defaults to `:8080` |
| `AGENTSTORM_GLOBAL_MAX_CONCURRENCY` | no | Global live Provider-attempt cap; `0` disables it |
| `AGENTSTORM_GLOBAL_REQUESTS_PER_MINUTE` | no | Global one-minute Provider-start cap; `0` disables it |
| `AGENTSTORM_PROVIDER_LIMITS_JSON` | no | Provider-to-limit JSON object with `max_concurrency` and `requests_per_minute` |
| `AGENTSTORM_PERMIT_LEASE_SECONDS` | no | Provider permit lease duration; defaults to `30`, range `10..300` |

Database migrations are embedded in the binary and applied under a PostgreSQL advisory lock before
the service starts accepting traffic.

The controller enables durable results with `--result-api-url`. Before creating a Job it reads the
selected key from the per-run-namespace Secret named by `--result-write-token-secret-name`, registers
the immutable run/reliability snapshot, and discards its in-memory copy after the request. The token
is never serialized to a ConfigMap, status, Event, log, or fixture. Workers receive the same selected
Secret key only as an environment reference for shard and terminal writes. `--include-sensitive-results`
is disabled by default, and `--result-upload-timeout` defaults to 30 seconds.

Distributed Provider permits are separately enabled with `--enable-distributed-limits`; the flag
requires `--result-api-url`. This explicit opt-in prevents ordinary durable ingestion from adding a
Result API round trip to every Provider attempt. Configure at least one non-zero global or Provider
cap in the Result API when enabling it. See [event-driven scheduling](scheduling.md).

## Public Kubernetes stack

`config/results` composes the namespace-scoped controller, single-replica PostgreSQL and MinIO,
the public Result API image, and restrictive NetworkPolicies. All container images are pinned to
immutable multi-architecture digests. The overlay deliberately contains no credentials; create
`agentstorm-result-storage` and `agentstorm-result-auth` in `agentstorm-system` before applying it.
See the released durable result-stack quickstart in the README for the required Secret keys.

This is an alpha reference stack, not a high-availability production database or object-store
topology. Operators must define storage classes, backups, credential rotation, availability, and
CNI-specific policy extensions for their environment.

## Local Kubernetes stack

`config/dev/results` is a development-only namespace-scoped overlay. It runs PostgreSQL and MinIO
with persistent volumes, starts the Result API from `agentstorm-result-api:dev`, and configures the
controller to send workers to the in-cluster service. The overlay deliberately does not contain
credentials; create `agentstorm-result-storage` and `agentstorm-result-auth` Secrets before applying
it. For compatibility with nested macOS kind/OrbStack combinations that incorrectly enforce
otherwise standard port-only rules, this development-only overlay disables all AgentStorm
NetworkPolicy selectors. The public `config/results` stack retains the policies, and API traffic in
both modes still requires separate read/write bearer tokens. The reusable test path creates
disposable credentials, resets only resources bearing its test-management label, and verifies the
full data path:

```bash
CLUSTER_PROVIDER=kind make e2e-results-local
```

To inspect retained results, forward the read endpoint and use the test-only read token created by
the E2E script:

```bash
kubectl -n agentstorm-system port-forward service/agentstorm-result-api 18080:8080
RUN_ID=$(kubectl -n agentstorm-system get agenttestrun agentstorm-results-baseline \
  -o jsonpath='{.metadata.uid}')
READ_TOKEN=$(kubectl -n agentstorm-system get secret agentstorm-result-auth \
  -o jsonpath='{.data.read-token}' | \
  python3 -c 'import base64, sys; print(base64.b64decode(sys.stdin.read()).decode())')
curl -H "Authorization: Bearer $READ_TOKEN" \
  "http://127.0.0.1:18080/v1/runs/$RUN_ID"
```

The `:dev` image and disabled NetworkPolicy selectors are intentionally limited to this local
overlay. `config/dev/telemetry` composes this stack with persistent Prometheus scraping, while
per-run and per-case investigation remains in Tempo rather than high-cardinality metric labels. The
public stack uses the verified `v0.4.0-alpha.1` Result API image-index digest.

## Sensitive content

Case output and full error text are optional fields. Workers omit them by default and include them
only when the controller's sensitive-result switch is enabled for a controlled environment. Secrets,
provider API keys, and bearer tokens must never be included in request bodies, raw objects, logs, or
test fixtures.

See [event-driven scheduling](scheduling.md) for queue leases, KEDA, resource admission, and
distributed Provider limits. See [Observability](observability.md) for metric labels, cardinality
rules, and the local OTLP trace verification path.
