# Result API

The Result API is the durable M2 boundary between execution workers and result consumers. It stores
run metadata, shard receipts, normalized case indexes, and aggregates in PostgreSQL. The canonical
shard document is compressed and written to S3-compatible object storage before a shard is marked
complete.

## HTTP contract

Health endpoints are unauthenticated. All other endpoints require `Authorization: Bearer <token>`.
Writer and reader tokens are separate so worker Pods do not receive result-read access.

| Method | Path | Token | Purpose |
| --- | --- | --- | --- |
| `GET` | `/healthz` | none | Process liveness |
| `GET` | `/readyz` | none | PostgreSQL and object-store readiness |
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

## Sensitive content

Case output and full error text are optional fields. Worker integration must omit them by default and
add an explicit sensitive-result switch for controlled environments. Secrets, provider API keys, and
bearer tokens must never be included in request bodies, raw objects, logs, or test fixtures.
