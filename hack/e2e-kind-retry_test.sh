#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=hack/e2e-kind-retry.sh
source "$repo_root/hack/e2e-kind-retry.sh"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

attempt_log="$tmpdir/attempt.log"
controller_log="$tmpdir/controller.log"
job_status="$tmpdir/jobs.txt"

write_signature() {
  printf '%s\n' 'error: timed out waiting for the condition on agenttestruns/agentstorm-demo' >"$attempt_log"
  printf '%s\n' 'E1017 02:00:52.776785 leaderelection.go:436] error retrieving resource lock agentstorm-system/agentstorm-controller.agentstorm.io: context deadline exceeded' >"$controller_log"
  printf '%s\n' 'agentstorm-demo-worker Complete=True' >"$job_status"
}

expect_retry() {
  local label="$1"
  if ! e2e_should_retry_kind_lock_flake kind 1 "$attempt_log" "$controller_log" "$job_status"; then
    echo "expected retry: $label" >&2
    exit 1
  fi
}

expect_stop() {
  local label="$1"
  if e2e_should_retry_kind_lock_flake kind 1 "$attempt_log" "$controller_log" "$job_status"; then
    echo "expected fail-fast: $label" >&2
    exit 1
  fi
}

write_signature
expect_retry "wait timeout, resource lock error, and Complete worker Job"

if e2e_should_retry_kind_lock_flake kind 2 "$attempt_log" "$controller_log" "$job_status"; then
  echo "second attempt must not retry" >&2
  exit 1
fi
if e2e_should_retry_kind_lock_flake orbstack 1 "$attempt_log" "$controller_log" "$job_status"; then
  echo "non-kind clusters must not recreate a cluster" >&2
  exit 1
fi

write_signature
printf '%s\n' 'agentstorm-demo-worker Complete=' >"$job_status"
expect_stop "running worker Job"

write_signature
printf '%s\n' 'agentstorm-demo-worker Complete=False' >"$job_status"
expect_stop "worker Job not Complete"

write_signature
printf '%s\n' 'agentstorm-secret-gate-worker Complete=' 'agentstorm-demo-worker Complete=True' >"$job_status"
expect_retry "one Complete worker Job among others"

write_signature
printf '%s\n' 'unexpected successful run status: job=agentstorm-demo-worker succeeded=1' >"$attempt_log"
expect_stop "assertion failure without a wait timeout"

write_signature
printf '%s\n' \
  'Waiting for deployment "agentstorm-controller" rollout to finish: 0 of 1 updated replicas are available...' \
  "$e2e_rollout_status_timeout_line" >"$attempt_log"
printf '' >"$job_status"
expect_retry "rollout status timeout and resource lock error without a worker Job"

write_signature
printf '%s\n' "$e2e_rollout_status_timeout_line" >"$attempt_log"
printf '%s\n' 'agentstorm-demo-worker Complete=' >"$job_status"
expect_retry "rollout status timeout while the worker Job is still running"

write_signature
printf '%s\n' 'error: timed out waiting for the condition on agenttestruns/agentstorm-demo' >"$attempt_log"
printf '' >"$job_status"
expect_stop "AgentTestRun wait timeout without a Complete worker Job"

write_signature
printf '%s\n' "$e2e_rollout_status_timeout_line" >"$attempt_log"
printf '' >"$job_status"
if e2e_should_retry_kind_lock_flake kind 2 "$attempt_log" "$controller_log" "$job_status"; then
  echo "second attempt must not retry a rollout status timeout" >&2
  exit 1
fi
if e2e_should_retry_kind_lock_flake orbstack 1 "$attempt_log" "$controller_log" "$job_status"; then
  echo "non-kind clusters must not retry a rollout status timeout" >&2
  exit 1
fi

write_signature
printf '%s\n' "$e2e_rollout_status_timeout_line" >"$attempt_log"
printf '%s\n' 'leader election lost' >"$controller_log"
expect_stop "rollout status timeout without the resource lock error"

write_signature
rm -f "$controller_log"
if e2e_should_retry_kind_lock_flake kind 1 "$attempt_log" "$controller_log" "$job_status"; then
  echo "missing controller log must fail fast" >&2
  exit 1
fi

echo "kind lock retry signature tests passed"
