# AgentStorm Worker

The worker reads one run configuration and a JSONL dataset, executes its assigned shard,
evaluates deterministic assertions, and writes `results.jsonl` plus `summary.json`.

The default `fake` provider has no third-party dependency. Install the optional OpenAI
adapter with `pip install -e '.[openai]'`.

The `openai-agents` adapter accepts an explicit model and optional OpenAI-compatible `baseURL`.
Credentials are read from `OPENAI_API_KEY`; in Kubernetes the controller injects that variable
directly from `spec.target.apiKeySecretRef` without writing it to a ConfigMap.
