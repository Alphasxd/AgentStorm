# AgentStorm SRE benchmark

> Fake validation only: this report is not a real-model performance claim.

Complete fixed execution shape: **True**; capacity executions: **384**; reliability executions: **32**.

## Capacity execution shape

All 384 executions succeeded across the fixed 12-run concurrency order.
They recorded 1920 simulated model calls and
1536 deterministic tool calls.

## Reliability execution shape

The 32-case reliability run recorded 30 successes,
2 expected infrastructure failures,
1 safe retry success, and
3 injected faults.

Timing and cost fields are intentionally omitted because in-process fake execution is not a Provider or Kubernetes performance measurement.

A future real-Provider report must be produced from released images and a fixed, separately approved benchmark protocol.
