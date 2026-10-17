# OpenAI-compatible provider

`target.provider: openai-compatible` calls a pinned chat-completions endpoint. Domestic cloud APIs
and self-hosted servers use the same adapter. The Controller records the model id and base URL; the
Worker performs HTTP. Retry, rate limits, fault injection, circuit breaking, price snapshots, and
traces stay in the existing Worker middleware.

## Run contract

```yaml
target:
  provider: openai-compatible
  model: pinned-model-id
  baseURL: https://example.invalid/compatible-mode/v1
  adapterEntrypoint: agentstorm_worker.benchmarks.sre:create_adapter
  apiKeySecretRef:
    name: provider-credentials
    key: api-key
  pricing:
    inputUSDPerMillionTokens: "1"
    outputUSDPerMillionTokens: "2"
```

`model` and `baseURL` are required for `openai-compatible`. CEL and the Controller reject a missing
model, a missing base URL, whitespace, credentials in the URL, a query or fragment, and a base URL
whose path already ends in `/chat/completions`. The base URL is the API root. The Worker appends
`/chat/completions`. Those shape checks apply only to this provider. `openai-agents` and `fake`
keep a base URL they already accepted, including a query string or a `/chat/completions` suffix.

The Controller allows any non-empty model id without whitespace. Benchmark runners additionally
reject `latest`, `auto`, and `default`. A price snapshot is optional on a single `AgentTestRun` and
required by every real benchmark mode. Missing pricing leaves cost null.

`apiKeySecretRef` is injected as `OPENAI_API_KEY`. The generated ConfigMap stores the Secret name and
key only. When the variable is empty, the adapter sends no `Authorization` header. Real benchmark
runners still require a non-empty key so the Secret path is exercised; an open inference server must
accept that bearer token or ignore it.

Built-in provider keys are `fake`, `openai-agents`, and `openai-compatible`. The development KEDA
overlay in `config/dev/keda` caps `openai-compatible` at 8 concurrent attempts and 60 requests per
minute, matching `openai-agents`. That cap is not a Helm chart default. The chart leaves provider
limits unset until an operator configures them.

## HTTP behavior

The adapter uses `urllib` on a worker thread. Requests are non-streaming JSON. Redirects are
rejected. Response bodies never appear in error strings. Tool calls returned in one assistant
message run one after another so fault injection stays deterministic. `parallel_tool_calls` and
`response_format` are not sent.

The generic adapter, selected with no `adapterEntrypoint`, is a plain chat client. It sends no
sampling fields and no tools. The SRE factory
`agentstorm_worker.benchmarks.sre:create_adapter` adds the four read-only incident tools, pins
`temperature` 0 and `max_tokens` 512, and stops after 8 model calls. Those two sampling fields are
benchmark configuration, not a hidden default of every compatible call. The factory records them in
the run manifest.

HTTP status maps onto the existing error taxonomy: `429` is retry-safe and does not count as a
completed model call; `408`, `5xx`, redirects, timeouts, and connection failures are ambiguous. A
connection reset while reading a success or error response is the same ambiguous `connection_error`.
The failed call is not counted as a completion. Tokens from earlier calls in that attempt stay.
The missing call is priced at an input allowance of 8192 tokens and at `max_tokens` when the
request sets it, otherwise 512. `usage_complete` stays true, so one transient failure does not
null the case cost. That price is an estimate for the missing call, not a meter reading, and the
endpoint does not enforce the input allowance. A `200` that omits usage, or a malformed `200`,
still leaves the attempt unpriced. Token totals are not stored as zero in those cases.

Cancelling the in-flight task re-raises `CancelledError`. urllib's timeout still bounds each
socket operation, and cancelling the task does not interrupt a socket call that is already
blocked. The runner records the run as cancelled rather than as a provider timeout. A tool
`ExecutionError` closes its lifecycle span with the tool error code before it propagates. Real
benchmark runs set the case timeout to 2100 seconds, above two reliability attempts times eight
model calls times the 120 second per-call timeout, so those calls finish or time out inside the
case.

## Endpoint examples

These are operator-pinned examples, not defaults and not measured results:

| Deployment | Base URL shape |
| --- | --- |
| DashScope legacy compatible mode | `https://dashscope.aliyuncs.com/compatible-mode/v1` |
| DashScope workspace domain | `https://{WorkspaceId}.{region}.maas.aliyuncs.com/compatible-mode/v1` |
| vLLM or SGLang | `http://host:8000/v1` |

The default worker NetworkPolicy allows egress to TCP 443 and DNS. A self-hosted server on another
port needs an explicit policy. Do not widen worker egress to arbitrary ports in the default profile.
The Kubernetes benchmark runner rejects loopback and unspecified addresses, including `localhost`
with a trailing dot, `0.0.0.0`, and `::`. A Worker pod cannot treat the operator laptop, or an
address that is not a remote host, as the endpoint.

## What this provider is not

It does not replace `openai-agents`. It does not deploy vLLM, probe GPUs, or call `/v1/models`.
SSH is a way to reach an HTTP server, not an adapter. Codex and ChatGPT sessions are not benchmark
endpoints. The published `v0.6.0` Worker digest predates this provider; a Kubernetes run needs a
digest-pinned image built from a revision that contains it.
