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

`model` and `baseURL` are required. CEL and the Controller reject a missing model, a missing base
URL, whitespace, credentials in the URL, a query or fragment, and a base URL whose path already ends
in `/chat/completions`. The base URL is the API root. The Worker appends `/chat/completions`.

The Controller allows any non-empty model id without whitespace. Benchmark runners additionally
reject `latest`, `auto`, and `default`. A price snapshot is optional on a single `AgentTestRun` and
required by every real benchmark mode. Missing pricing leaves cost null.

`apiKeySecretRef` is injected as `OPENAI_API_KEY`. The generated ConfigMap stores the Secret name and
key only. When the variable is empty, the adapter sends no `Authorization` header. Real benchmark
runners still require a non-empty key so the Secret path is exercised; an open inference server must
accept that bearer token or ignore it.

Built-in provider keys are `fake`, `openai-agents`, and `openai-compatible`. The development KEDA
overlay caps `openai-compatible` at 8 concurrent attempts and 60 requests per minute, matching
`openai-agents`.

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
completed model call; `408`, `5xx`, redirects, timeouts, and connection failures are ambiguous and
leave `model_call_count` null; a malformed `200` counts the call and is ambiguous. Token totals
stay unknown when usage is absent. They are never stored as zero.

## Endpoint examples

These are operator-pinned examples, not defaults and not measured results:

| Deployment | Base URL shape |
| --- | --- |
| DashScope legacy compatible mode | `https://dashscope.aliyuncs.com/compatible-mode/v1` |
| DashScope workspace domain | `https://{WorkspaceId}.{region}.maas.aliyuncs.com/compatible-mode/v1` |
| vLLM or SGLang | `http://host:8000/v1` |

The default worker NetworkPolicy allows egress to TCP 443 and DNS. A self-hosted server on another
port needs an explicit policy. Do not widen worker egress to arbitrary ports in the default profile.
The Kubernetes benchmark runner rejects loopback hosts (`localhost`, `127.0.0.1`, `::1`) because a
Worker pod cannot treat the operator laptop as the endpoint.

## What this provider is not

It does not replace `openai-agents`. It does not deploy vLLM, probe GPUs, or call `/v1/models`.
SSH is a way to reach an HTTP server, not an adapter. Codex and ChatGPT sessions are not benchmark
endpoints. The published `v0.6.0` Worker digest predates this provider; a Kubernetes run needs a
digest-pinned image built from a revision that contains it.
