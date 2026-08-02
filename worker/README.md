# AgentStorm Worker

The worker reads one run configuration and a JSONL dataset, executes its assigned shard,
evaluates deterministic assertions, and writes `results.jsonl` plus `summary.json`.

Each JSONL case may declare an ordered `assertions` array. Built-in assertion types are `exact`,
`contains`, `regex`, `json_schema`, `tool_path`, and `latency`. The legacy `expected_contains`
field remains supported and is evaluated before entries in `assertions`; every assertion must pass.
For example:

```json
{"id":"tool-case","input":"hello","assertions":[{"type":"contains","value":"hello"},{"type":"tool_path","path":["fake.echo"]},{"type":"latency","max_ms":1000}]}
```

Trusted custom assertions use `{"type":"python","entrypoint":"package.module:function","config":{}}`.
The module must already be installed in the Worker image. The function receives JSON-compatible
`context` and `config` dictionaries and returns either a boolean or a `passed`/`message` object.
Dataset-provided inline Python is never executed.

An optional immutable target price snapshot enables reproducible USD cost accounting:

```json
{"target":{"provider":"fake","pricing":{"inputUSDPerMillionTokens":"2.5","outputUSDPerMillionTokens":"10"}}}
```

Prices are decimal strings, not floating-point values. Local case and summary files expose
fixed-precision cost strings; when durable ingestion is enabled, the Result API independently
derives cost from the registered snapshot and persisted Token counts. Missing pricing produces
`null` cost rather than consulting a mutable provider catalog.

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
text unless the sensitive-result switch is explicitly enabled. Tool paths and content-safe assertion
outcomes are always uploaded; custom assertion messages use the same sensitive-result gate.

Tracing emits run, case, provider-call, local-tool, handoff, and deterministic-evaluator spans. The
adapter contract exposes content-free lifecycle callbacks; the OpenAI Agents SDK adapter maps its
run hooks to those callbacks and disables sensitive content in SDK-native tracing. Traces record
bounded identifiers, provider/model/tool/agent names, timings, outcomes, and token counts, but never
record prompts, model or tool output, tool arguments, handoff input, expected values, exception
messages, API keys, or bearer tokens. See
[`docs/observability.md`](../docs/observability.md) for the complete contract.
