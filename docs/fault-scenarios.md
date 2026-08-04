# FaultScenario contract

M4 fault injection uses a JSON document stored under one key in a namespace-local ConfigMap. It is not a CRD and is never mounted directly into a Worker. Before creating the Job, the Controller strictly validates the document, normalizes it, computes its SHA-256 digest, and copies that immutable snapshot into the generated run ConfigMap.

The source build accepts this run fragment:

```yaml
spec:
  reliability:
    seed: 42
    scenarioRef:
      name: agentstorm-fault-scenario
      key: scenario.json
    retry:
      maxAttempts: 1
      initialBackoffMs: 100
      maxBackoffMs: 2000
      maxCumulativeBackoffMs: 5000
      jitterRatio: 0.2
      allowAmbiguousRetries: false
```

See [`config/samples/fault-scenario.yaml`](../config/samples/fault-scenario.yaml) for a complete document. Rules are evaluated in declaration order and at most the first selected rule applies to an attempt. Supported faults are `latency`, `timeout`, `http_error`, `malformed_response`, `rate_limit`, and `tool_error`.

Selection is deterministic. AgentStorm hashes the NUL-delimited UTF-8 values `scenarioDigest`, `seed`, `caseID`, `iteration`, `attempt`, and `ruleName`, then interprets the first 64 bits as a probability draw. Run ID, shard index, concurrency, and scheduling order are deliberately excluded.

The document is limited to 64 KiB and 100 uniquely named rules. Unknown fields, invalid selectors, unsupported fault parameters, and missing referenced ConfigMaps keep the run Pending with `ScenarioReady=False`; no Provider call is made. Scenario documents cannot contain scripts, templates, credentials, inline Python, or arbitrary HTTP bodies.

## Retry safety

`maxAttempts` includes the initial full Agent execution and defaults to `1`; existing Runs never gain a hidden retry. A retry repeats the entire Agent and can therefore repeat model charges and tool side effects. By default, AgentStorm retries only rate limits, explicitly safe pre-submit failures, and safe injected failures. Timeout, HTTP 5xx, malformed responses, and unknown connection failures require `allowAmbiguousRetries: true`. Evaluation, harness, tool, circuit-open, and cancellation outcomes are never retried.

Backoff is exponential, capped by `maxBackoffMs` and `maxCumulativeBackoffMs`, and bounded by the case timeout. Jitter is deterministically derived from the configured seed, case ID, iteration, and attempt. If no seed is configured because no scenario is referenced, the deterministic retry seed is zero.

Every attempt records its outcome, stable failure category/code, retry decision, actual backoff, known Token usage, injected fault, and circuit events. Token totals include every attempt. If any Provider attempt may have incurred unknown usage, `usage_complete` is false and priced cost remains `null`.

## Circuit breaker

The optional breaker is scoped to one Worker process and Provider adapter. Consecutive terminal Provider failures open it; evaluation, tool, harness, and cancellation outcomes do not increment the counter. While open, calls fail locally with `provider/circuit_open`. After `openDurationMs`, exactly one concurrent half-open probe is admitted. A Provider success closes and resets the breaker; a Provider failure opens it again.

Because breaker state observes concurrent completion order, the exact rejected cases are only reproducible with `concurrencyPerWorker: 1`. Deterministic fault selection itself remains independent of concurrency and sharding.

## Durable reporting and cancellation

The Result API registration stores the normalized scenario, source ConfigMap name/key, digest,
retry budget, and circuit settings. Case pages expose ordered attempts with stable category/code,
retry decision, backoff, Token usage completeness, selected rule/fault, and circuit events. Run
summaries and comparisons separate evaluation failures as quality failures from Provider, tool, and
harness infrastructure failures. They also report attempts, retries, retry successes, injected
faults, and circuit rejections.

Setting `spec.cancel: true` makes the Controller best-effort persist a `cancelled` terminal record
before deleting the Job. A terminating Worker stops scheduling cases and retries, cancels its
in-flight Adapter task, flushes completed cases within ten seconds, and idempotently records
cancellation. The Result API keeps those cases readable with `partial: true`; incomplete or terminal
Runs cannot be used as Promptfoo or comparison baselines. Kubernetes cancellation still wins when
the result sink is unavailable, and the CR exposes that persistence failure.

Run the entire no-cost source validation with:

```bash
CLUSTER_PROVIDER=kind ENABLE_RELIABILITY_E2E=true make e2e-results-local
```

It uses the fake Provider and validates all supported fault types, same-seed selection across
different shard/concurrency layouts, conservative versus opted-in ambiguous retry, Token/cost
completeness, every circuit transition, report separation, and partial cancellation.

The public manifests pin the verified `v0.6.0` Controller, Worker, and Result API
image-index digests. Keep all three components on the same release when enabling reliability.
