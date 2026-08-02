# AgentStorm Worker

The worker reads one run configuration and a JSONL dataset, executes its assigned shard,
evaluates deterministic assertions, and writes `results.jsonl` plus `summary.json`.

The default `fake` provider has no third-party dependency. Install the optional OpenAI adapter with
`pip install -e '.[openai]'`. Install OTLP tracing support with `pip install -e '.[telemetry]'`, or
both integrations with `pip install -e '.[openai,telemetry]'`.

The `openai-agents` adapter accepts an explicit model and optional OpenAI-compatible `baseURL`.
Credentials are read from `OPENAI_API_KEY`; in Kubernetes the controller injects that variable
directly from `spec.target.apiKeySecretRef` without writing it to a ConfigMap.

When `AGENTSTORM_RESULT_API_URL` is set, the worker validates the complete dataset, registers the run
before creating the provider adapter, and uploads its shard after execution. Registration or upload
failure exits non-zero. Duplicate case IDs are rejected before any provider call.

| Variable | Default | Purpose |
| --- | --- | --- |
| `AGENTSTORM_RESULT_API_URL` | disabled | Result API base URL |
| `AGENTSTORM_RESULT_WRITE_TOKEN` | required when enabled | Writer bearer token injected from a Secret |
| `AGENTSTORM_RESULT_TIMEOUT_SECONDS` | `30` | Timeout for each registration or upload request |
| `AGENTSTORM_INCLUDE_SENSITIVE_RESULTS` | `false` | Include output and full error text in durable uploads |
| `AGENTSTORM_OTEL_ENABLED` | `false` | Enable worker OpenTelemetry spans |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | SDK default | OTLP HTTP exporter base URL |
| `OTEL_SERVICE_NAME` | `agentstorm-worker` | Worker trace service name |

Local `results.jsonl` files retain their existing detail. Durable uploads omit output and full error
text unless the sensitive-result switch is explicitly enabled.

Tracing emits run, case, provider-call, local-tool, handoff, and deterministic-evaluator spans. The
adapter contract exposes content-free lifecycle callbacks; the OpenAI Agents SDK adapter maps its
run hooks to those callbacks and disables sensitive content in SDK-native tracing. Traces record
bounded identifiers, provider/model/tool/agent names, timings, outcomes, and token counts, but never
record prompts, model or tool output, tool arguments, handoff input, expected values, exception
messages, API keys, or bearer tokens. See
[`docs/observability.md`](../docs/observability.md) for the complete contract.
