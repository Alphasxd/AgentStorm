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

## Domestic cloud and self-hosted Providers

The next benchmark should prefer an exact, versioned OpenAI-compatible domestic cloud model. Set
`target.baseURL` for a compatible endpoint; use a trusted image-bundled `adapterEntrypoint` when a
Provider requires another HTTP/SDK protocol. A self-hosted service may be reached through an SSH
tunnel, but SSH only transports a stable Worker-accessible inference API and is not itself an
Adapter.

Every real run must record the exact model version and network topology. Self-hosted evidence must
also record quantization, GPU, inference server, and cluster configuration. Codex and ChatGPT
interactive sessions are neither benchmark endpoints nor substitutes for Provider credentials.
