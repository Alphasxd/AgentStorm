# Trusted Adapter plugins

Adapter plugins let a Worker image add a domain Agent or Provider integration without putting SDK or
prompt/tool logic in the Kubernetes Controller.

## Contract

Set the immutable run target:

```yaml
target:
  provider: openai-agents
  model: gpt-5.6-luna
  adapterEntrypoint: my_package.adapter:create_adapter
```

The entrypoint is a maximum 256-byte `module:function` reference. Code must be installed in
`runner.image`; AgentStorm does not execute inline Python, download remote code, or mount scripts
from ConfigMaps. The factory is synchronous:

```python
from agentstorm_worker.adapters import AdapterFactoryContext

def create_adapter(context: AdapterFactoryContext):
    ...
```

`AdapterFactoryContext` exposes `provider`, `model`, `base_url`, `max_output_tokens`, and
`request_fields`. Credentials remain in the Worker process environment for the Provider SDK and
are never passed through the factory context. Pricing and Kubernetes Secret objects are also
absent. A trusted plugin still runs in that same process and can read environment variables, so
the context restriction is not a sandbox boundary. `max_output_tokens` is the optional completion
cap. `request_fields` is the validated provider JSON from `target.requestFields`.

The returned object must implement async `run(case, lifecycle=None) -> AdapterResponse`. Emit public
tool/handoff lifecycle events so tracing, `tool_path`, fault injection, and tool-call accounting stay
provider-independent. Set `model_call_count` only from public SDK usage data; use `None` if it cannot
be proven.

## Failure and trust boundary

Import errors, factory exceptions, and invalid return objects fail before the first model call as
`harness/adapter_plugin_invalid`. Error messages are sanitized. Plugin code is trusted code in the
Worker image: there is no process sandbox, so review it like any other dependency.

## Built-in OpenAI-compatible provider

`provider: openai-compatible` is a Worker adapter, not a plugin download. It posts to
`{baseURL}/chat/completions` with the standard library and reads `OPENAI_API_KEY` from the process
environment. Domain tools, including the SRE incident tools, stay in an image-bundled
`adapterEntrypoint` such as `agentstorm_worker.benchmarks.sre:create_adapter`. A run with no
entrypoint is plain chat and sends no tools. The optional `openai-agents` provider remains a
separate SDK path. See [OpenAI-compatible provider](openai-compatible.md).

## Contribution checklist

- Keep the Controller free of Provider imports and domain prompts.
- Add a no-cost fake implementation of the same model/tool lifecycle.
- Test Secret-free factory context, import/factory failures, tool lifecycle, and unknown usage.
- Use deterministic public fixtures; never commit real prompts, credentials, or production traces.
- Update the CRD, sample, documentation, and tests together if the public run contract changes.
