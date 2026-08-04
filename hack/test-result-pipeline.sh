#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
database_url="${AGENTSTORM_TEST_DATABASE_URL:?AGENTSTORM_TEST_DATABASE_URL is required}"
s3_endpoint="${AGENTSTORM_TEST_S3_ENDPOINT:?AGENTSTORM_TEST_S3_ENDPOINT is required}"
s3_access_key="${AGENTSTORM_TEST_S3_ACCESS_KEY:?AGENTSTORM_TEST_S3_ACCESS_KEY is required}"
s3_secret_key="${AGENTSTORM_TEST_S3_SECRET_KEY:?AGENTSTORM_TEST_S3_SECRET_KEY is required}"
python_bin="${PYTHON:-python3}"
listen_address="127.0.0.1:18080"
base_url="http://$listen_address"
write_token="pipeline-write-test-token"
read_token="pipeline-read-test-token"
run_id="pipeline-$RANDOM-$$"
test_dir="$(mktemp -d)"
api_pid=""

cleanup() {
  if [[ -n "$api_pid" ]]; then
    kill "$api_pid" >/dev/null 2>&1 || true
    wait "$api_pid" >/dev/null 2>&1 || true
  fi
  if [[ -n "$test_dir" && -d "$test_dir" ]]; then
    rm -rf -- "$test_dir"
  fi
}
trap cleanup EXIT

go build -C "$repo_root" -o "$test_dir/result-api" ./cmd/result-api

AGENTSTORM_LISTEN_ADDR="$listen_address" \
AGENTSTORM_DATABASE_URL="$database_url" \
AGENTSTORM_S3_ENDPOINT="$s3_endpoint" \
AGENTSTORM_S3_ACCESS_KEY="$s3_access_key" \
AGENTSTORM_S3_SECRET_KEY="$s3_secret_key" \
AGENTSTORM_RESULT_WRITE_TOKEN="$write_token" \
AGENTSTORM_RESULT_READ_TOKEN="$read_token" \
  "$test_dir/result-api" >"$test_dir/result-api.log" 2>&1 &
api_pid=$!

for attempt in {1..30}; do
  if curl --fail --silent "$base_url/readyz" >/dev/null; then
    break
  fi
  if [[ "$attempt" == "30" ]]; then
    sed -n '1,120p' "$test_dir/result-api.log" >&2
    exit 1
  fi
  sleep 1
done

cat >"$test_dir/run.json" <<JSON
{
  "run_id": "$run_id",
  "source": {"namespace": "integration", "name": "pipeline"},
  "dataset": {"name": "pipeline-dataset", "key": "cases.jsonl"},
  "target": {"provider": "fake"},
  "workload": {"concurrency": 1, "iterations": 1, "timeout_seconds": 30},
  "evaluation": {"min_success_rate": 1, "max_error_rate": 0}
}
JSON

cat >"$test_dir/cases.jsonl" <<'JSONL'
{"id":"case-a","input":"alpha","expected_contains":"alpha"}
{"id":"case-b","input":"beta","expected_contains":"beta"}
JSONL

run_shard() {
  shard_index="$1"
  AGENTSTORM_RESULT_API_URL="$base_url" \
  AGENTSTORM_RESULT_WRITE_TOKEN="$write_token" \
  AGENTSTORM_SHARD_COUNT=2 \
  AGENTSTORM_SHARD_INDEX="$shard_index" \
  PYTHONPATH="$repo_root/worker/src" \
    "$python_bin" -m agentstorm_worker run \
      --config "$test_dir/run.json" \
      --dataset "$test_dir/cases.jsonl" \
      --output "$test_dir/output-$shard_index"
}

run_shard 0
curl --fail --silent \
  --header "Authorization: Bearer $read_token" \
  "$base_url/v1/runs/$run_id" >"$test_dir/collecting.json"
"$python_bin" - "$test_dir/collecting.json" <<'PY'
import json
import sys

run = json.load(open(sys.argv[1], encoding="utf-8"))
assert run["status"] == "collecting", run
assert run["received_shards"] == 1, run
assert run["partial"] is True, run
PY

run_shard 1
curl --fail --silent \
  --header "Authorization: Bearer $read_token" \
  "$base_url/v1/runs/$run_id" >"$test_dir/complete.json"
curl --fail --silent \
  --header "Authorization: Bearer $read_token" \
  "$base_url/v1/runs/$run_id/cases?limit=10" >"$test_dir/cases-response.json"
"$python_bin" - "$test_dir/complete.json" "$test_dir/cases-response.json" <<'PY'
import json
import sys

run = json.load(open(sys.argv[1], encoding="utf-8"))
page = json.load(open(sys.argv[2], encoding="utf-8"))
assert run["status"] == "complete", run
assert run["received_shards"] == 2, run
assert run["partial"] is False, run
assert run["summary"]["total"] == 2, run
assert run["summary"]["succeeded"] == 2, run
assert run["summary"]["usage_complete"] is True, run
assert run["summary"]["model_call_count"] == 2, run
assert run["summary"]["tool_call_count"] == 2, run
assert run["summary"]["model_calls_per_successful_agent"] == 1, run
assert run["summary"]["tool_calls_per_successful_agent"] == 1, run
assert len(page["cases"]) == 2, page
for case in page["cases"]:
    assert "output" not in case, case
    assert "error" not in case, case
    assert case["usage_complete"] is True, case
    assert case["model_call_count"] == 1, case
    assert case["tool_call_count"] == 1, case
    assert len(case["attempts"]) == 1, case
    assert case["attempts"][0]["outcome"] == "succeeded", case
    assert case["attempts"][0]["model_call_count"] == 1, case
    assert case["attempts"][0]["tool_call_count"] == 1, case
PY

echo "Result pipeline integration passed: run_id=$run_id"
