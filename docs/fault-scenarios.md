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

Retry and circuit-breaker behavior is being completed as part of M4. Until the `v0.4.0-alpha.1` images are published, use Controller and Worker images built from the same source revision when exercising this contract.
