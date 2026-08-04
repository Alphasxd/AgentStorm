# ADR 0001: Kubernetes-native control plane

Status: Accepted

AgentStorm represents each immutable load test as an `AgentTestRun` and delegates placement,
garbage collection, quota, and pod lifecycle to Kubernetes. Indexed Jobs serve fixed capacity;
KEDA ScaledJobs consume a durable shard queue for elastic capacity. The alternative—a standalone
orchestrator duplicating scheduler behavior—would weaken the project’s Kubernetes-native thesis and
create another control plane to operate.
