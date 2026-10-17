# ADR 0005: One OpenAI-compatible provider for pinned real-model runs

Status: Accepted

Domestic cloud chat endpoints and self-hosted inference servers (vLLM, SGLang, and other
OpenAI-compatible HTTP servers) share one Worker provider, `openai-compatible`. The difference
between those deployments is benchmark metadata and URL policy, not a second adapter. The existing
`openai-agents` provider stays the optional Agents SDK path and is not the transport for this
benchmark.

The adapter speaks `POST {baseURL}/chat/completions` with the Python standard library. It does not
import a vendor SDK, probe `/v1/models`, or open SSH. SSH may carry an HTTP server to a
Worker-reachable address; it is transport, not an adapter. API keys stay in `OPENAI_API_KEY`,
injected from `spec.target.apiKeySecretRef`, and never enter generated ConfigMaps. An empty key
omits the `Authorization` header so an open local server can still be called.

Real benchmark runners refuse to start unless `AGENTSTORM_ALLOW_PROVIDER_SPEND=1` is set, the model
id is pinned, and a decimal price snapshot is present. Domestic-cloud endpoints must be `https`.
Self-hosted hardware, quantization, inference server, and topology are operator-attested fields in
the manifest; AgentStorm does not measure them. A result is canonical only when the Kubernetes
runner finishes the unchanged 384-capacity plus 32-reliability SRE protocol. Mock and in-process
runs are `canonical=false` and do not support a performance claim. No real-endpoint result is
committed with this decision.
