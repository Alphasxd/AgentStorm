#!/usr/bin/env bash
# Decision helper for the one-shot kind E2E retry. Sourced by hack/e2e-local.sh.

# e2e_should_retry_kind_lock_flake reports whether hack/e2e-local.sh should
# delete the kind cluster and run once more.
#
# Retry only on the first attempt, and only for a kind cluster, when all of
# the following are true:
#   - the attempt log shows that a kubectl wait timed out
#   - the controller log contains "error retrieving resource lock"
#   - a worker Job is already Complete (a line containing Complete=True)
# Every other failure returns non-zero so the caller stops immediately.
e2e_should_retry_kind_lock_flake() {
  local provider="$1"
  local attempt="$2"
  local attempt_log="$3"
  local controller_log="$4"
  local job_status="$5"

  [[ "$provider" == "kind" ]] || return 1
  [[ "$attempt" == "1" ]] || return 1
  [[ -f "$attempt_log" && -f "$controller_log" && -f "$job_status" ]] || return 1
  grep -F -q 'timed out waiting for the condition' "$attempt_log" || return 1
  grep -F -q 'error retrieving resource lock' "$controller_log" || return 1
  grep -E -q '(^|[[:space:]])Complete=True([[:space:]]|$)' "$job_status" || return 1
  return 0
}
