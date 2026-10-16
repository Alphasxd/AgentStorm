# SRE Agent benchmark

This benchmark measures a complete Agent execution, not a single prompt completion. Each of the
32 synthetic incidents requires a four-tool evidence chain and a structured diagnosis. The tools
are deterministic, read-only fixtures shipped in the Worker image.

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
`temperature` 0 and `max_tokens` 512 and records both in the manifest. Real modes also require a
decimal price snapshot and reject model ids `latest`, `auto`, and `default`. Domestic-cloud base
URLs must be `https`. Example roots an operator may pin, none of which are defaults or measured
results:

```text
https://dashscope.aliyuncs.com/compatible-mode/v1
https://{WorkspaceId}.{region}.maas.aliyuncs.com/compatible-mode/v1
http://host:8000/v1
```

The default worker NetworkPolicy allows TCP 443 and DNS. A server on another port needs its own
policy. The Kubernetes runner rejects loopback hosts.

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
The runner stops before the next run once recorded cost reaches USD 8. A completed local matrix can
report in-process duration and token cost for that pinned endpoint. It is `canonical=false` and is
not a KEDA or speedup result. An incomplete matrix writes a report with no performance table and
exits non-zero.

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
endpoint stayed pinned, and the KEDA evidence checks pass. A partial matrix, a budget stop, or a
changed dataset or model exits non-zero and omits the performance table. Hardware, quantization,
inference server, and topology in a self-hosted manifest are operator-supplied; AgentStorm does not
probe GPUs.

No real result is stored in this directory. Do not commit one until the manifest says `canonical=true`
and the report's claims match that manifest.
