# Result API

The Result API is the durable M2 boundary between execution workers and result consumers. It stores
run metadata, shard receipts, normalized case indexes, and aggregates in PostgreSQL. The canonical
shard document is compressed and written to S3-compatible object storage before a shard is marked
complete.

## HTTP contract

Health and metrics endpoints are unauthenticated. All other endpoints require
`Authorization: Bearer <token>`. Writer and reader tokens are separate so worker Pods do not receive
result-read access. The public Kubernetes stack restricts metrics access with NetworkPolicy.

| Method | Path | Token | Purpose |
| --- | --- | --- | --- |
| `GET` | `/healthz` | none | Process liveness |
| `GET` | `/readyz` | none | PostgreSQL and object-store readiness |
| `GET` | `/metrics` | none | Prometheus RED, result, case, and token counters |
| `PUT` | `/v1/runs/{runID}` | writer | Register immutable run metadata and expected shards |
| `PUT` | `/v1/runs/{runID}/shards/{index}` | writer | Persist one complete shard document |
| `GET` | `/v1/runs/{runID}` | reader | Read status, received shards, and aggregates |
| `GET` | `/v1/runs/{runID}/cases` | reader | Page through cases; `failed=true` filters failures |
| `GET` | `/v1/comparisons?baseline={id}&candidate={id}` | reader | Compare two complete runs |

The registration key is `run/{runID}`. A shard key is `run/{runID}/shard/{index}`. Every case in a
shard carries `run/{runID}/case/{urlEncodedCaseID}/iteration/{iteration}`. Sending the same key and
canonical body again returns success without changing counters. Reusing an identity with different
content returns HTTP `409`.

Run comparison reports candidate-minus-baseline deltas for success and failure rates, P50/P95/P99
latency, and token counts. Latency percentage deltas are `null` when the baseline value is zero.

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

Database migrations are embedded in the binary and applied under a PostgreSQL advisory lock before
the service starts accepting traffic.

The controller enables worker uploads with `--result-api-url`. It reads no Result API token itself;
instead it verifies the per-run-namespace Secret named by `--result-write-token-secret-name` and
injects only the selected key into the worker container. `--include-sensitive-results` is disabled
by default, and `--result-upload-timeout` defaults to 30 seconds.

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
overlay. The public stack uses the `v0.2.0-alpha.1` Result API image-index digest.

## Sensitive content

Case output and full error text are optional fields. Workers omit them by default and include them
only when the controller's sensitive-result switch is enabled for a controlled environment. Secrets,
provider API keys, and bearer tokens must never be included in request bodies, raw objects, logs, or
test fixtures.

See [Observability](observability.md) for the metric labels, cardinality rules, and local OTLP trace
verification path.
