# ADR 0002: Worker Adapter middleware for reliability faults

Status: Accepted

Fault selection, retry, and Worker-local circuit breaking wrap the complete `AgentAdapter.run`
inside the Worker. A network sidecar cannot reliably inject tool lifecycle failures or preserve
Agent-level attempt semantics, and it would add deployment/network variability to capacity results.
Deterministic selection is derived from scenario digest, seed, case, iteration, attempt, and rule.
