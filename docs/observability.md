# Observability

M3 starts with an opt-in, content-safe tracing boundary in the worker and low-cardinality
Prometheus metrics in the Result API. Tracing remains disabled unless the controller is given an
OTLP/HTTP endpoint. Metrics are always available at `GET /metrics`, but the public result-stack
NetworkPolicy only permits workers, result readers, and Pods labeled
`agentstorm.io/metrics-reader=true` to reach the Result API.

## Worker traces

Configure the controller with a Collector base URL:

```text
--otel-exporter-otlp-endpoint=http://collector.example:4318
```

The controller validates that the URL is HTTP(S), contains no embedded credentials, query, or
fragment, and then injects these non-secret variables into worker Pods:

```text
AGENTSTORM_OTEL_ENABLED=true
OTEL_EXPORTER_OTLP_ENDPOINT=http://collector.example:4318
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
OTEL_SERVICE_NAME=agentstorm-worker
```

The worker uses the standard OpenTelemetry OTLP/HTTP exporter configuration, so deployment systems
can supply supported exporter headers and sampling variables directly when needed. The initial span
hierarchy is:

```text
agentstorm.run
└── agentstorm.case
    ├── gen_ai.invoke_agent
    └── agentstorm.evaluate
```

Run and case spans carry correlation identifiers, shard position, success/failure category, and
Token counts. Provider spans use `gen_ai.operation.name`, `gen_ai.provider.name`,
`gen_ai.request.model`, and `gen_ai.usage.*`. Deterministic evaluator spans use
`gen_ai.evaluation.*`. The `gen_ai.*` conventions are still evolving in the dedicated
[OpenTelemetry GenAI semantic-conventions repository](https://github.com/open-telemetry/semantic-conventions-genai),
so AgentStorm treats this mapping as experimental and keeps its persisted result schema independent.

Prompt text, model output, expected values, tool arguments/results, and exception messages are never
added to default spans. Exceptions record only their type. OpenTelemetry automatic exception
recording is explicitly disabled so an exporter cannot recover an error body through the standard
exception event. Tool and handoff spans will be added after the adapter interface exposes stable,
provider-independent lifecycle events.

## Result API metrics

The Result API exposes a dedicated Prometheus registry with:

| Metric | Labels | Meaning |
| --- | --- | --- |
| `agentstorm_result_api_http_requests_total` | method, route, status class | Request rate and errors |
| `agentstorm_result_api_http_request_duration_seconds` | method, route | Request latency histogram |
| `agentstorm_result_api_run_registrations_total` | bounded outcome | Created, duplicate, invalid, conflict, or error registrations |
| `agentstorm_result_api_shard_uploads_total` | bounded outcome | Idempotent shard ingestion outcomes |
| `agentstorm_result_api_cases_total` | result, bounded failure kind | Uniquely persisted success/failure cases |
| `agentstorm_result_api_tokens_total` | direction | Input and output Tokens from unique shards |

Run IDs, case IDs, model names, source names, bearer tokens, outputs, and error bodies are not metric
labels. Unknown worker failure kinds collapse to `other`. This keeps cardinality bounded and prevents
sensitive values from entering the Prometheus index. Detailed per-run and failed-case data remains in
the authenticated Result API.

## Local verification

`config/dev/telemetry` composes the source-built result stack with a digest-pinned OpenTelemetry
Collector whose debug exporter is used only as an E2E assertion sink. It is not a production trace
backend. A normal source-built result E2E enables this path automatically:

```bash
CLUSTER_PROVIDER=kind make e2e-results-local
```

The test verifies the complete OTLP export, required span hierarchy, Prometheus metrics, and default
content redaction. Published-image smoke tests for releases that predate this interface automatically
leave telemetry disabled; `ENABLE_TELEMETRY_E2E=true|false` can override that selection.

A persistent trace backend, Prometheus deployment, and Grafana drill-down dashboard are the next M3
delivery after these signal contracts stabilize.
