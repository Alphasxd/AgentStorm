# SRE Agent benchmark

This benchmark measures tool use and instruction following along a fixed path. It does not measure
diagnostic reasoning. Each of the 32 synthetic incidents requires the model to call `get_incident`,
then `query_metrics` and `search_logs`, then `lookup_runbook` with the `error_signature` that only
`search_logs` returns. The diagnosis values (`root_cause`, `severity`, `recommended_action`,
`evidence`, and `runbook`) come from `lookup_runbook`. The instructions tell the model to copy them
verbatim, and `evidence` is a JSON array of strings. `query_metrics` is required by the path
assertion even though those diagnosis values do not depend on its numbers. A wrong path or a skipped
tool cannot pass `validate_tool_usage`. The tools are deterministic, read-only fixtures shipped in
the Worker image.

## No-cost validation

```bash
make benchmark-fake
```

The fake run executes the same 12-run capacity matrix (384 Agent executions) plus the 32-case
reliability run. It validates tool lifecycle, assertions, retry/error reporting, aggregation, and
artifact checksums, but it is not a model or KEDA performance result.

The committed [`v0.6.0-fake-validation`](results/v0.6.0-fake-validation/) evidence contains all 416
case histories and is marked `canonical=false`. Timing, throughput, cost, speedup, efficiency, and
KEDA timing fields are deliberately removed so the artifact cannot be mistaken for performance
evidence.

## Optional post-release OpenAI run

The optional runner can use the released Worker digest, `gpt-5.6-luna`, explicit reasoning effort
`none`, parallel tool calls, KEDA worker gradients `1,4,16,32`, and the three fixed orders documented
in `manifest.json`. It is not an M6 release gate. Run it only in an isolated
OpenAI project with a platform hard spend limit of USD 10. `OPENAI_API_KEY` is read only by the
manual runner; it must never be put in a manifest, artifact, command argument, or CI secret used by
default workflows. The runner stops launching new runs once recorded cost reaches USD 8. A
ChatGPT Pro subscription is not an API credential.

A result is publishable only when all 384 capacity executions and the separate 32-case reliability
run are complete. Do not shrink the dataset or change the model to manufacture a complete report.

With a KEDA-enabled AgentStorm release already installed, the Result API reachable locally, and
the `agentstorm-controller` / `agentstorm-result-api` Deployments pinned to released digests:

```bash
export OPENAI_API_KEY='...'
export AGENTSTORM_RESULT_READ_TOKEN='...'
python3 hack/benchmark/run_sre_keda.py \
  --kube-context kind-agentstorm \
  --namespace agentstorm-system \
  --worker-image ghcr.io/alphasxd/agentstorm-worker@sha256:... \
  --result-api-url http://127.0.0.1:8080 \
  --input-price '<current explicit price>' \
  --output-price '<current explicit price>' \
  --output .out/benchmark-real
```

The read token and API key are accepted only from environment variables. The runner always deletes
its temporary API-key Secret, scans every artifact for both secret values, and exits non-zero for a
partial matrix or a recorded-cost stop. Before spending begins it also starts a no-secret metadata
pod from the released Worker digest to record the exact Python, Worker, and OpenAI Agents SDK
versions. A publishable run requires the 32-Worker tier to reach 32 live Workers and all capacity
cases to pass their schema and dependent-tool assertions.

## OpenAI-compatible real-model protocol

Domestic cloud endpoints and self-hosted OpenAI-compatible servers (vLLM, SGLang, and others) use
`provider: openai-compatible` and `agentstorm_worker.benchmarks.sre:create_adapter`. The optional
Agents SDK runner above is unchanged. SSH only transports a Worker-reachable HTTP server and is not
an adapter. Codex and ChatGPT sessions are neither endpoints nor credentials.

The contract is [OpenAI-compatible provider](../../docs/openai-compatible.md). The SRE factory pins
`temperature` 0 and `max_tokens` from `target.maxTokens`, default 512, and records both in the
manifest. `lookup_runbook` returns the diagnosis fields the schema requires, including `evidence`
as an array of strings. Real modes also require a decimal price snapshot and reject model ids
`latest`, `auto`, and `default`. `domestic-cloud` and `hosted-api` base URLs must be `https`.
Example roots an operator may pin, none of which are defaults or measured results:

```text
https://dashscope.aliyuncs.com/compatible-mode/v1
https://{WorkspaceId}.{region}.maas.aliyuncs.com/compatible-mode/v1
http://host:8000/v1
```

The default worker NetworkPolicy allows TCP 443 and DNS. A server on another port needs its own
policy. The Kubernetes runner rejects loopback and unspecified addresses, including a trailing DNS
dot, `0.0.0.0`, `::`, and IPv4-mapped forms such as `::ffff:127.0.0.1`.

### No-cost mock

```bash
make benchmark-compatible-mock
make sre-compatible-mock
```

`benchmark-compatible-mock` replays the 12-run capacity matrix and the 32-case reliability run
against a local mock server. `sre-compatible-mock` is one 32-case smoke pass. Both pop
`OPENAI_API_KEY` for the duration of the run, and the mock rejects an `Authorization` header. The
mock still applies a price snapshot so cost plumbing is exercised, then strips timing, throughput,
cost, speedup, and efficiency from the artifacts. `canonical` is false. These commands are the CI
path. They support no domestic-cloud, self-hosted, or Kubernetes claim.

### In-process real endpoint

This calls a billed or self-hosted server. It does not start unless spend is explicitly allowed:

```bash
AGENTSTORM_ALLOW_PROVIDER_SPEND=1 \
OPENAI_API_KEY='...' \
MODEL='exact-model-id' \
BASE_URL='https://example.invalid/compatible-mode/v1' \
ENDPOINT_CLASS=domestic-cloud \
INPUT_PRICE='<decimal USD per million input tokens>' \
OUTPUT_PRICE='<decimal USD per million output tokens>' \
make benchmark-compatible-local
```

Replace the model, base URL, and prices with the endpoint you will call. `INPUT_PRICE` and
`OUTPUT_PRICE` are decimal USD per million tokens. For `ENDPOINT_CLASS=self-hosted`, also set
`INFERENCE_SERVER`, `QUANTIZATION`, `HARDWARE`, and `TOPOLOGY`. `MODEL_VERSION` defaults to `MODEL`.
Before each run the runner estimates spend for the cases in that run, 8 model calls, and the
attempt count (1 for capacity, 2 for reliability). Output tokens use the configured `max_tokens`
limit, 512 unless `--max-tokens` is set. Input tokens use the larger of 8192 and the assistant
history those calls can resend at that limit. The endpoint does not enforce that allowance. The
runner refuses to start when recorded spend plus the estimate would pass USD 8. A price of `0`
makes the estimate `0`. Pass `--price-is-zero-confirmed` only after checking the provider bill;
that flag is recorded on the manifest and disables the USD 8 stop. A non-zero price still
reserves spend. A timeout, `5xx`, or connection failure prices that one call at the same
allowance and keeps earlier tokens, so the
case cost stays populated. `model_call_count` in the run artifacts stays at the number of completed
calls. The adapter response records the failed call as `agentstorm.usage.unknown_calls` because the
provider may already have accepted it. The case timeout is 2100 seconds, above two attempts times
eight calls times the 120 second urllib socket timeout. That timeout applies to each socket
operation, not to the whole HTTP request. A completed local matrix can report in-process duration
and token cost for that pinned endpoint. It is `canonical=false` and is
not a KEDA or speedup result. An incomplete matrix writes a report with no performance table and
exits non-zero.

`--shape smoke` runs four incidents, `INC-001`, `INC-009`, `INC-017`, and `INC-025`, one from each
root-cause family. `--cases N` with smoke keeps the first N incidents instead. Smoke defaults to
`--max-concurrency 1`. The same cap applies to a full matrix: a cap below 32 is not the fixed
protocol. Smoke and a capped full run set `complete=false`, omit performance claims, and exit 0
when the harness itself finishes. Pass provider JSON with `--request-fields`, for example
`'{"thinking":{"type":"disabled"}}'`. `make benchmark-compatible-local` forwards `SHAPE`, `CASES`,
`MAX_CONCURRENCY`, `MAX_TOKENS`, `HTTP_RETRY_ATTEMPTS`, `HTTP_RETRY_MAX_WAIT`, and
`PRICE_IS_ZERO_CONFIRMED=1`. Pass `--request-fields` on the Python command; Make does not quote
that JSON. `--http-retry-attempts` (1 through 16, default 3) and `--http-retry-max-wait` (1
through 300 seconds, default 8) set the in-call HTTP 429 retry. HTTP 5xx is not retried inside
that call.

```bash
AGENTSTORM_ALLOW_PROVIDER_SPEND=1 \
OPENAI_API_KEY='...' \
python3 hack/benchmark/run_sre_compatible.py --mode local --shape smoke \
  --model exact-model-id \
  --base-url 'https://example.invalid/v1' \
  --endpoint-class hosted-api \
  --input-price '0' --output-price '0' --price-is-zero-confirmed \
  --max-concurrency 1 --max-tokens 2048 \
  --request-fields '{"thinking":{"type":"disabled"}}' \
  --output .out/benchmark-compatible-smoke
```

### Z.ai glm-4.7-flash

This is a non-publishable smoke against the hosted API. It is not a measured result. Put the key
in `OPENAI_API_KEY` in the environment. Do not write it into a file, a manifest, or this document.

| Field | Value |
| --- | --- |
| Model | `glm-4.7-flash` |
| Base URL | `https://api.z.ai/api/paas/v4` |
| Endpoint class | `hosted-api` |
| Concurrency | `1` |

```bash
AGENTSTORM_ALLOW_PROVIDER_SPEND=1 \
OPENAI_API_KEY='...' \
python3 hack/benchmark/run_sre_compatible.py --mode local --shape smoke \
  --model glm-4.7-flash \
  --base-url 'https://api.z.ai/api/paas/v4' \
  --endpoint-class hosted-api \
  --input-price '0' --output-price '0' --price-is-zero-confirmed \
  --max-concurrency 1 --max-tokens 2048 \
  --request-fields '{"thinking":{"type":"disabled"}}' \
  --http-retry-attempts 8 --http-retry-max-wait 120 \
  --output .out/benchmark-zai-smoke
```

Check the Z.ai bill before `--price-is-zero-confirmed`. A confirmed zero price disables the USD 8
stop. `python -m agentstorm_worker run` also requires `AGENTSTORM_ALLOW_PROVIDER_SPEND=1` and has
no USD stop of its own. HTTP 429 responses may carry Z.ai code `1305` (overloaded) or `1302`
(concurrency). The adapter keeps that code on the local attempt and retries only that 429,
honoring `Retry-After` up to `httpRetryMaxWaitSeconds`, before the runner can restart the case.
`1305` is overload, including at concurrency 1, and a free-tier overload can last minutes, so the
default of 3 attempts with an 8 second cap is too short. HTTP 5xx stays single-shot and is priced
at the unknown-call allowance. This smoke is `complete=false` and is not a publishable benchmark.

### Kubernetes real endpoint

Requires a KEDA-enabled AgentStorm install, a local Result API, and a digest-pinned Worker image
built from a revision that contains this provider. The published `v0.6.0` Worker digest does not.
The runner installs the API key as a Secret, deletes that Secret when it finishes, and scans
artifacts for the key and the read token.

```bash
AGENTSTORM_ALLOW_PROVIDER_SPEND=1 \
OPENAI_API_KEY='...' \
AGENTSTORM_RESULT_READ_TOKEN='...' \
WORKER_IMAGE='ghcr.io/alphasxd/agentstorm-worker@sha256:...' \
RESULT_API_URL='http://127.0.0.1:8080' \
MODEL='exact-model-id' \
BASE_URL='https://example.invalid/compatible-mode/v1' \
ENDPOINT_CLASS=domestic-cloud \
INPUT_PRICE='<decimal USD per million input tokens>' \
OUTPUT_PRICE='<decimal USD per million output tokens>' \
KUBE_CONTEXT=kind-agentstorm \
make benchmark-compatible-keda
```

Self-hosted runs add the same attested metadata as the local command. `canonical=true` only when all
384 capacity executions and the 32-case reliability run finish, the price snapshot is present, the
endpoint stayed pinned, and the KEDA evidence checks pass. The Kubernetes runner uses the same
spend estimate and the same 2100 second case timeout. Its wait must be longer than that
case timeout. A partial matrix, a budget stop, or a changed dataset or model exits non-zero and
omits the performance table. Hardware, quantization,
inference server, and topology in a self-hosted manifest are operator-supplied; AgentStorm does not
probe GPUs.

No real result is stored in this directory. Do not commit one until the manifest says `canonical=true`
and the report's claims match that manifest.
