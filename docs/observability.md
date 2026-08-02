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
    │   ├── gen_ai.execute_tool
    │   └── agentstorm.handoff
    └── agentstorm.evaluate
```

Run and case spans carry correlation identifiers, shard position, success/failure category, and
Token counts. Provider spans use `gen_ai.operation.name`, `gen_ai.provider.name`,
`gen_ai.request.model`, and `gen_ai.usage.*`. Deterministic evaluator spans use
`gen_ai.evaluation.*`. The `gen_ai.*` conventions are still evolving in the dedicated
[OpenTelemetry GenAI semantic-conventions repository](https://github.com/open-telemetry/semantic-conventions-genai),
so AgentStorm treats this mapping as experimental and keeps its persisted result schema independent.

Adapters receive a provider-independent lifecycle sink. Local tool callbacks create
`gen_ai.execute_tool` spans with `gen_ai.operation.name=execute_tool`, tool name/type, optional call
ID, and agent name. Handoffs create `agentstorm.handoff` spans with source and target agent names.
The OpenAI Agents SDK adapter maps `RunHooks` into this sink; provider SDK objects do not cross the
adapter boundary. Its SDK-native tracing is also forced to omit sensitive LLM and tool content. The
no-cost fake adapter emits a deterministic synthetic tool and handoff so the cluster E2E can verify
this trace shape without an API key.

Prompt text, model output, expected values, tool arguments/results, handoff input, and exception
messages are never added to default spans or lifecycle events. Exceptions record only their type.
OpenTelemetry automatic exception recording is explicitly disabled so an exporter cannot recover an
error body through the standard exception event. Arbitrary shell, browser, hosted, and MCP tool
execution remains outside the alpha worker's sandbox boundary; their trace mapping should be added
with the execution policy that enables those tools.

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

## Local persistent stack

`config/dev/telemetry` composes the source-built result stack with four digest-pinned workloads:

| Component | Local role | Persistence |
| --- | --- | --- |
| OpenTelemetry Collector 0.157.0 | Receive worker OTLP/HTTP; forward to Tempo and retain the debug exporter for redaction assertions | none |
| Tempo 2.10.5 | Search traces with TraceQL using the Run ID and trace attributes | 1 GiB PVC, 24-hour retention |
| Prometheus 3.11.0 | Scrape Result API metrics and evaluate three RED recording rules | 1 GiB PVC, 24-hour retention |
| Grafana 13.1.0 | Provision Prometheus/Tempo data sources and the `AgentStorm Observability` dashboard | 1 GiB PVC |

Grafana's provisioned dashboard combines low-cardinality operational metrics with high-cardinality
trace search. Enter the `AgentTestRun` UID in its `Run ID` variable to list failed case traces and
provider-error traces. Selecting a trace ID opens the complete span tree in the dashboard without
copying run or case identifiers into Prometheus labels.

Run the complete source-built path and retain its resources:

```bash
CLUSTER_PROVIDER=kind make e2e-results-local
kubectl -n agentstorm-system port-forward service/agentstorm-grafana 13000:3000
```

Retrieve a run ID and open the dashboard:

```bash
RUN_ID=$(kubectl -n agentstorm-system get agenttestrun agentstorm-results-failure \
  -o jsonpath='{.metadata.uid}')
echo "http://127.0.0.1:13000/d/agentstorm-observability/agentstorm-observability?var-run_id=$RUN_ID"
```

The E2E creates a deterministic failed case, queries its trace from Tempo, verifies Prometheus
metrics and recording-rule health, loads the provisioned dashboard through the Grafana API, and then
restarts Tempo and Prometheus to prove both signals remain queryable from their PVCs. It also checks
the required run/case/provider/tool/handoff/evaluator span hierarchy and confirms that prompts,
expected values, case IDs, run IDs, and bearer tokens do not enter the wrong telemetry surface:
correlation IDs remain trace-only, while content and credentials are excluded from both traces and
metric labels.

Published-image smoke tests for releases that predate this interface automatically leave telemetry
disabled; `ENABLE_TELEMETRY_E2E=true|false` can override that selection.

## Deployment boundary

This overlay is a single-replica development stack, not a production observability topology. Tempo
uses its monolithic local-filesystem backend; Prometheus has local TSDB retention; Grafana enables
anonymous Viewer access and exposes only a ClusterIP service intended for `kubectl port-forward`.
Do not expose these services outside a trusted local cluster. Production deployments need external
object storage, authentication/TLS, backup and retention policies, resource sizing, and HA chosen by
the operator. Use `KEEP_E2E_RESOURCES=false` to remove the test stack and its PVCs after verification.
