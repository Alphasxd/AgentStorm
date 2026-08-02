# Promptfoo durable replay

This optional integration evaluates a completed AgentStorm run with Promptfoo without issuing a
new model request. The generator reads the original JSONL dataset and the paginated Result API,
then writes a Promptfoo configuration. The Python provider retrieves the saved output, Token usage,
cost, and tool metadata by case ID and iteration at evaluation time.

Promptfoo can re-evaluate `exact`, `contains`, `regex`, and `json_schema` assertions. AgentStorm
remains authoritative for latency, tool-path, and trusted Python assertions; their saved outcomes
are attached as test and provider metadata instead of being re-executed.

## Requirements

- A run whose Result API status is `complete`.
- Sensitive durable results enabled before the run, so every case has a saved `output`.
- Python 3.11 or newer.
- Promptfoo 0.121.19 on Node.js 24 for the tested path.

The read token is accepted only through `AGENTSTORM_RESULT_READ_TOKEN`. It is never accepted as a
command-line option and is not written to the generated config. The config also contains no saved
model output.

## Generate and evaluate

```bash
export AGENTSTORM_RESULT_READ_TOKEN='<read token>'

python3 integrations/promptfoo/generate.py \
  --result-api-url http://127.0.0.1:18080 \
  --run-id '<completed run ID>' \
  --dataset examples/datasets/basic.jsonl \
  --output .out/promptfoo.json

PROMPTFOO_DISABLE_TELEMETRY=true \
PROMPTFOO_PYTHON=python3 \
npx --yes promptfoo@0.121.19 eval \
  --config .out/promptfoo.json \
  --no-cache
```

Generation fails before writing a config when the run is incomplete, the case pages are malformed,
or any output is absent. The provider caches one completed run in its Python process, so multiple
Promptfoo test rows do not repeatedly page the Result API.

See the upstream documentation for the [Python provider response contract](https://www.promptfoo.dev/docs/providers/python/)
and [declarative assertions](https://www.promptfoo.dev/docs/configuration/expected-outputs/).
