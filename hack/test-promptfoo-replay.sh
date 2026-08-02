#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
test_root="$(mktemp -d)"
stub_pid=""
export PYTHONDONTWRITEBYTECODE="1"

cleanup() {
  if [[ -n "${stub_pid}" ]]; then
    kill "${stub_pid}" 2>/dev/null || true
    wait "${stub_pid}" 2>/dev/null || true
  fi
  rm -rf "${test_root}"
}
trap cleanup EXIT

port_file="${test_root}/port"
request_log="${test_root}/requests.log"
config_file="${test_root}/promptfoo.json"
result_file="${test_root}/results.json"

python3 "${repo_root}/integrations/promptfoo/tests/stub_result_api.py" \
  --port-file "${port_file}" \
  --request-log "${request_log}" &
stub_pid="$!"

for _ in {1..50}; do
  if [[ -s "${port_file}" ]]; then
    break
  fi
  sleep 0.1
done
if [[ ! -s "${port_file}" ]]; then
  echo "Promptfoo stub Result API did not start" >&2
  exit 1
fi

port="$(<"${port_file}")"
export AGENTSTORM_RESULT_READ_TOKEN="promptfoo-ci-read-token"
python3 "${repo_root}/integrations/promptfoo/generate.py" \
  --result-api-url "http://127.0.0.1:${port}" \
  --run-id replay-run \
  --dataset "${repo_root}/integrations/promptfoo/tests/fixtures/cases.jsonl" \
  --output "${config_file}"

CONFIG_FILE="${config_file}" python3 - <<'PY'
import json
import os
from pathlib import Path

path = Path(os.environ["CONFIG_FILE"])
rendered = path.read_text(encoding="utf-8")
if "promptfoo-ci-read-token" in rendered:
    raise SystemExit("generated config leaked the read token")
if "UNMAPPED_MODEL_OUTPUT_CANARY" in rendered:
    raise SystemExit("generated config leaked a durable model output")
config = json.loads(rendered)
if len(config.get("tests", [])) != 5:
    raise SystemExit("generated config did not include every paginated case")
PY

export PROMPTFOO_DISABLE_TELEMETRY="true"
export PROMPTFOO_PYTHON="python3"
unset OPENAI_API_KEY ANTHROPIC_API_KEY GOOGLE_API_KEY AZURE_OPENAI_API_KEY
npx --yes promptfoo@0.121.19 eval \
  --config "${config_file}" \
  --no-cache \
  --output "${result_file}"

RESULT_FILE="${result_file}" REQUEST_LOG="${request_log}" python3 - <<'PY'
import json
import os
from pathlib import Path

payload = json.loads(Path(os.environ["RESULT_FILE"]).read_text(encoding="utf-8"))
results = payload.get("results", {}).get("results", payload.get("results", []))
if not isinstance(results, list) or len(results) != 5:
    raise SystemExit("Promptfoo did not evaluate all five replay cases")
if any(not item.get("success") for item in results):
    raise SystemExit("Promptfoo replay assertions did not all pass")
requests = Path(os.environ["REQUEST_LOG"]).read_text(encoding="utf-8").splitlines()
if not any("cursor=second-page" in request for request in requests):
    raise SystemExit("Promptfoo replay did not exercise case pagination")
if any(not request.startswith("/v1/runs/replay-run") for request in requests):
    raise SystemExit("Promptfoo replay made a non-Result-API request")
PY
