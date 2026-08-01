# Open-source reference designs

AgentStorm borrows architectural patterns, not product identities or copied implementations. Review
the license of any code before adapting it and keep third-party notices when required.

## OpenAI Agents SDK

Reference: <https://github.com/openai/openai-agents-python>

Borrow:

- Adapter boundary around Agent execution.
- Tool, handoff, guardrail, session, and tracing vocabulary.
- Separation between workflow orchestration and model transport.

Do not copy:

- Its Agent loop into the controller.
- Provider-specific trace payloads into the CRD.
- Raw sensitive prompt/tool content into default telemetry.

## LangGraph

Reference: <https://github.com/langchain-ai/langgraph>

Borrow later:

- Durable checkpoints, resumable state, interrupts, and human approval concepts.
- Clear separation of API, queue, execution worker, and persistence roles.

Do not add it to the alpha worker alongside the Agents SDK. First stabilize AgentStorm's adapter and
result contracts; then add LangGraph as a second adapter to prove the boundary.

## Kubebuilder and controller-runtime

References:

- <https://github.com/kubernetes-sigs/kubebuilder>
- <https://book.kubebuilder.io/cronjob-tutorial/controller-implementation.html>

Borrow:

- Level-based reconciliation, owner references, status conditions, health probes, and RBAC markers.
- Unit tests for pure construction logic and envtest for API behavior.

Avoid action-style controllers that assume every event is delivered once. Reconciliation must remain
safe after controller restarts and duplicate watch events.

## Grafana k6 Operator

Reference: <https://github.com/grafana/k6-operator>

Borrow:

- A test-run custom resource as the user-facing execution contract.
- Kubernetes Jobs as distributed runners.
- Separation between initialization, execution, and aggregation responsibilities.

Do not wrap k6 or copy `TestRun`. Agent workloads need different result semantics: model/tool traces,
token usage, nondeterministic quality checks, and expensive-call retry boundaries.

## Kubernetes SIGs Agent Sandbox

Reference: <https://github.com/kubernetes-sigs/agent-sandbox>

Borrow later:

- Declarative lifecycle and isolation boundaries for stateful Agent workspaces.
- Template and warm-pool concepts when tool-using test cases need disposable sandboxes.

Do not turn AgentStorm into another sandbox controller. AgentStorm should schedule and evaluate test
runs; a sandbox project should own isolated, stateful execution environments. Integrate through a
runner adapter after arbitrary tools enter scope.

## kagent

Reference: <https://github.com/kagent-dev/kagent>

Borrow:

- Provider abstraction and Kubernetes-native Agent configuration vocabulary.
- Realistic multi-provider Agent deployments as future black-box test targets.

Keep the product boundary explicit: kagent builds and operates Agents, while AgentStorm drives
repeatable workloads and evaluates reliability. AgentStorm should not duplicate Agent authoring or
tool-catalog features.

## Promptfoo

Reference: <https://github.com/promptfoo/promptfoo>

Borrow:

- Declarative datasets, assertions, provider comparison, CI exit codes, and red-team regression cases.
- Deterministic assertions before expensive LLM-based grading.

Promptfoo should remain an optional integration. AgentStorm owns distributed execution and Kubernetes
lifecycle; it should not reimplement Promptfoo's complete assertion catalog.

## KEDA

Reference: <https://github.com/kedacore/keda>

Borrow in M5:

- Event-driven scale-to-zero and ScaledJob concepts.
- Authentication references and external-metric boundaries.

Do not introduce KEDA before a durable pending-shard queue exists. Scaling an in-memory queue would
produce attractive diagrams but incorrect failure behavior.

## OpenTelemetry semantic conventions

Reference: <https://opentelemetry.io/docs/specs/semconv/registry/attributes/gen-ai/>

Borrow:

- Standard names for Agent, provider, model, token usage, conversation, and tool calls.
- Trace hierarchy that correlates run, case, model generation, and tool execution.

Treat semantic-convention stability explicitly. AgentStorm's persisted result schema must not be a
blind copy of experimental trace attributes.
