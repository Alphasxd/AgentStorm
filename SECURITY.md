# Security policy

AgentStorm is an early-stage project and is not yet intended for untrusted multi-tenant clusters.

Report suspected vulnerabilities privately to the repository maintainer. Do not include live API keys,
private prompts, model outputs, or cluster credentials in an issue.

The default security model is:

- Provider keys come from Kubernetes Secrets.
- Secret readiness uses uncached single-object reads; runtime RBAC does not list or watch Secrets.
- Generated run ConfigMaps contain no credentials.
- Controller logs contain lifecycle metadata only.
- Worker Pods do not mount ServiceAccount tokens, run as non-root, drop Linux capabilities, use
  RuntimeDefault seccomp, and write only to a dedicated ephemeral output volume.
- The namespace-scoped profile combines Role/RoleBinding permissions with controller and worker
  NetworkPolicies.
- Arbitrary Agent tools are disabled until a sandbox policy is implemented.
- Raw Agent traces are considered sensitive and must be opt-in.
