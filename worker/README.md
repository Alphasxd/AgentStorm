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

`target.adapterEntrypoint` may select a trusted `module:function` factory already installed in the
Worker image. Its factory context contains only provider, model, and base URL; no API key, price,
or Kubernetes Secret object is passed through that interface. Because plugin code is trusted and
runs in the Worker process, it can still read process environment variables such as Provider
credentials. Import/factory/return failures stop before the first model call. Inline and remotely
downloaded plugins are unsupported.

Attempt and case results include `model_call_count` and `tool_call_count`. Model calls remain `null`
when a public SDK usage object cannot prove the value; tool calls come from the provider-independent
lifecycle. The bundled `agentstorm_worker.benchmarks.sre:create_adapter` demonstrates a 32-incident
four-tool Agent with matching fake and OpenAI execution paths.

When `AGENTSTORM_RESULT_API_URL` is set, the worker validates the complete dataset, registers the run
before creating the provider adapter, and uploads its shard after execution. Registration or upload
failure exits non-zero. Duplicate case IDs are rejected before any provider call.

| Variable | Default | Purpose |
| --- | --- | --- |
| `AGENTSTORM_RESULT_API_URL` | disabled | Result API base URL |
| `AGENTSTORM_RESULT_WRITE_TOKEN` | required when enabled | Writer bearer token injected from a Secret |
| `AGENTSTORM_RESULT_TIMEOUT_SECONDS` | `30` | Timeout for each registration or upload request |
| `AGENTSTORM_INCLUDE_SENSITIVE_RESULTS` | `false` | Include output and full error text in durable uploads |
| `AGENTSTORM_QUEUE_MODE` | `false` | Claim one durable shard instead of using an Indexed Job completion index |
| `AGENTSTORM_WORKER_ID` | `local-worker` | Pod identity used for queue and permit ownership |
| `AGENTSTORM_DISTRIBUTED_LIMITS` | `false` | Acquire and renew a Result API Provider permit around every Agent attempt |
| `AGENTSTORM_OTEL_ENABLED` | `false` | Enable worker OpenTelemetry spans |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | SDK default | OTLP HTTP exporter base URL |
| `OTEL_SERVICE_NAME` | `agentstorm-worker` | Worker trace service name |

Local `results.jsonl` files retain their existing detail. Durable uploads omit output and full error
text unless the sensitive-result switch is explicitly enabled. Tool paths and content-safe assertion
outcomes are always uploaded; custom assertion messages use the same sensitive-result gate.

Queue and distributed-limit variables are Controller-owned M5 interfaces, not end-user credentials.
Queue claims and permits use opaque renewable tokens, send those tokens only in authenticated request
bodies or the shard-upload header, and store only hashes in PostgreSQL. A lost queue lease stops that
Worker and leaves the shard eligible for another Job. A transient queued Result API failure also
leaves the Run non-terminal so the shard can be reclaimed after lease expiry. A lost Provider permit
fails closed because the in-flight call can no longer be safely accounted for. See
[`docs/scheduling.md`](../docs/scheduling.md).

Tracing emits run, case, provider-call, local-tool, handoff, and deterministic-evaluator spans.
`telemetry.contentMode` defaults to `omit`, which excludes prompts, model/tool content, dataset
metadata, expected values, and exception bodies. `redacted` mode adds sanitized prompt, output,
available tool arguments/results, and explicitly allowlisted metadata. It removes credential-like
keys, replaces up to 20 configured regular expressions with `[REDACTED]`, and UTF-8-safely truncates
content attributes to 2048 bytes. The controller requires both an OTLP endpoint and
`--allow-redacted-telemetry` before it creates a Worker Job in this mode; raw tracing is unsupported.
The OpenAI Agents SDK adapter disables sensitive SDK-native tracing and does not inspect private SDK
objects to recover missing tool arguments. See
[`docs/observability.md`](../docs/observability.md) for the complete contract.
