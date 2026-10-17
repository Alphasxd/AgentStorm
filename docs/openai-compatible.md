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

`python -m agentstorm_worker run` calls `openai-compatible` and `openai-agents` only when
`AGENTSTORM_ALLOW_PROVIDER_SPEND=1`. `validate` does not call a provider and does not need the
variable. The controller sets it to `1` on Workers it starts for those two providers, because
creating the AgentTestRun is the operator's authorization. `fake` is ungated. The worker process
has no USD stop. Benchmark runners keep the USD 8 recorded-cost stop. A price of `0` makes that
estimate `0`, so those runners reject it unless `--price-is-zero-confirmed` is set after the
operator checks the provider bill. That confirmation disables the USD 8 stop and is recorded as
`spend_guard=confirmed-zero-price`. A non-zero price still reserves spend and ignores the flag.
An AgentTestRun priced at `0` has no second confirmation field; the CR itself is the snapshot.

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
tools. `target.maxTokens` (1 through 131072) becomes `max_tokens` on that request. Leave it empty
and the generic adapter sends no completion cap. The SRE factory
`agentstorm_worker.benchmarks.sre:create_adapter` adds the four read-only incident tools, pins
`temperature` 0, uses `target.maxTokens` or 512, and stops after 8 model calls. `lookup_runbook`
returns the exact `root_cause`, `severity`, `recommended_action`, `evidence`, and `runbook` the
case schema requires, and the instructions tell the model to copy those values. `evidence` is a
JSON array of strings.

`target.requestFields` is extra JSON merged into the chat body, for provider options such as
`thinking: {type: disabled}`. It allows at most 16 identifier keys. Values are JSON, at most 1024
bytes, and at most six levels deep. `model`, `messages`, `stream`, `tools`, `tool_choice`,
`temperature`, and `max_tokens` are reserved so they cannot diverge from the recorded request.
Benchmark manifests record `max_tokens` and `request_fields`.

One chat call retries HTTP 429, 502, 503, and 504 inside the adapter. The default is 3 attempts,
with backoff starting at 0.5 seconds, doubling, and capped at 8 seconds. A `Retry-After`
delta-seconds or HTTP-date value replaces that backoff and is still capped at 8 seconds. There is
no jitter. Completed tool turns stay in the message list, so a 429 does not rerun earlier tools.
Other 5xx statuses, timeouts, and connection errors stay single-shot. A 429 adds no tokens and is
not a model call. Intermediate 502, 503, and 504 responses are not charged. A terminal 5xx is
priced once, at the same unknown-call allowance as a single failure, so retries do not multiply
that allowance. That can undercount a 5xx the provider billed. Case-level retry still applies when
the adapter gives up on a retry-safe 429.

Response bodies stay out of error strings. When `error.code` is an integer or string of at most 32
characters matching `[A-Za-z0-9_.-]`, and it is not the API key, the adapter keeps it as
`agentstorm.provider.error_code`. Message text is dropped. The Result API `error_code` stays the
harness reason, such as `rate_limited`, because that field does not accept provider codes such as
`1305`. Local attempt records also store that provider code, the rate-limit event count, and
`finish_reason` when it matches the same allowlist.

`finish_reason=length` with no tool call and empty or whitespace content is
`provider/output_truncated`. It is not a final answer and it is not retried. A non-empty truncated
completion is still returned so the evaluator can score it. An empty completion with another
finish reason stays an empty answer.

HTTP status maps onto the existing error taxonomy: `429` is retry-safe and does not count as a
completed model call; `408`, `5xx`, redirects, timeouts, and connection failures are ambiguous. A
connection reset while reading a success or error response is the same ambiguous `connection_error`.
`model_call_count` is the number of calls that returned a response the adapter finished. A failed
call is omitted from that count even when the provider may already have accepted it. The adapter
records those omitted calls on the response as `agentstorm.usage.unknown_calls`. That marker is
not added to `model_call_count`, and stored attempt and case results keep the completed-call count.
Tokens from earlier calls in that attempt stay. The missing call is priced at an input allowance
of 8192 tokens (`UNKNOWN_CALL_INPUT_TOKENS`) and at `max_tokens` when the request sets a positive
integer. When the request omits `max_tokens`, as the generic adapter does, the output charge is
the named constant `UNKNOWN_CALL_OUTPUT_TOKENS` (8192). That allowance is larger than the
512-token SRE benchmark cap because an uncapped completion may already have generated more than
512 tokens. It is still an estimate, not a meter reading, and it can be below a model's maximum
output. The endpoint does not enforce either allowance. `usage_complete` stays true, so one
transient failure does not null the case cost. A `200` that omits usage, or a malformed `200`,
still leaves the attempt unpriced. Token totals are not stored as zero in those cases.

Cancelling the in-flight task re-raises `CancelledError`. The urllib timeout, 120 seconds on the
SRE factory, applies to each socket operation, such as connect, send, or read. It does not bound
the whole HTTP request. Cancelling the task does not interrupt a socket call that is already
blocked. The runner records the run as cancelled rather than as a provider timeout. A tool
`ExecutionError` closes its lifecycle span with the tool error code before it propagates. Real
benchmark runs set the case timeout to 2100 seconds, above two reliability attempts times eight
model calls times that 120 second socket timeout. One request can still outlast a single 120
second interval when more than one socket operation uses the timeout.

## Endpoint examples

These are operator-pinned examples, not defaults and not measured results:

| Deployment | Base URL shape | Endpoint class |
| --- | --- | --- |
| DashScope legacy compatible mode | `https://dashscope.aliyuncs.com/compatible-mode/v1` | `domestic-cloud` |
| DashScope workspace domain | `https://{WorkspaceId}.{region}.maas.aliyuncs.com/compatible-mode/v1` | `domestic-cloud` |
| Hosted OpenAI-compatible API | `https://api.example.invalid/v1` | `hosted-api` |
| vLLM or SGLang | `http://host:8000/v1` | `self-hosted` |

`hosted-api` uses the same `https` rule as `domestic-cloud`. It is metadata for a hosted
compatible endpoint that is not a domestic-cloud deployment. A short validation procedure for
Z.ai `glm-4.7-flash` is in the [SRE benchmark](../benchmarks/sre/README.md).

The default worker NetworkPolicy allows egress to TCP 443 and DNS. A self-hosted server on another
port needs an explicit policy. Do not widen worker egress to arbitrary ports in the default profile.
The Kubernetes benchmark runner rejects loopback and unspecified addresses, including `localhost`
with a trailing dot, `0.0.0.0`, `::`, and IPv4-mapped forms such as `::ffff:127.0.0.1` and
`::ffff:0.0.0.0`. Python before 3.13 does not report those mapped addresses as loopback, so the
check uses `ipv4_mapped`. A Worker pod cannot treat the operator laptop, or an address that is
not a remote host, as the endpoint.

## What this provider is not

It does not replace `openai-agents`. It does not deploy vLLM, probe GPUs, or call `/v1/models`.
SSH is a way to reach an HTTP server, not an adapter. Codex and ChatGPT sessions are not benchmark
endpoints. The published `v0.6.0` Worker digest predates this provider; a Kubernetes run needs a
digest-pinned image built from a revision that contains it.
