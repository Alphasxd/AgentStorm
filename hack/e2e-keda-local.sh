#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cluster_provider="${CLUSTER_PROVIDER:-auto}"
kind_cluster_name="${KIND_CLUSTER_NAME:-agentstorm}"
kube_context="${KUBE_CONTEXT:-}"
e2e_namespace="${E2E_NAMESPACE:-agentstorm-system}"
keep_resources="${KEEP_E2E_RESOURCES:-true}"
controller_image="${CONTROLLER_IMAGE:-agentstorm-controller:dev}"
worker_image="${WORKER_IMAGE:-agentstorm-worker:dev}"
result_api_image="${RESULT_API_IMAGE:-agentstorm-result-api:dev}"
wait_timeout="${E2E_TIMEOUT:-600s}"
kubectl_bin="${KUBECTL:-kubectl}"
keda_manifest_url="${KEDA_MANIFEST_URL:-https://github.com/kedacore/keda/releases/download/v2.20.0/keda-2.20.0.yaml}"

case "$keep_resources" in true|false) ;; *) echo "KEEP_E2E_RESOURCES must be true or false" >&2; exit 1 ;; esac

CLUSTER_PROVIDER="$cluster_provider" KIND_CLUSTER_NAME="$kind_cluster_name" KUBE_CONTEXT="$kube_context" \
KEEP_E2E_RESOURCES=true ENABLE_TELEMETRY_E2E=false ENABLE_RELIABILITY_E2E=false \
  EXPECT_COST_ACCOUNTING=false "$repo_root/hack/e2e-results-local.sh"

if [[ -z "$kube_context" ]]; then
  kube_context="$($kubectl_bin config current-context)"
fi
kubectl_cmd=("$kubectl_bin" --context "$kube_context")

if ! "${kubectl_cmd[@]}" get crd scaledjobs.keda.sh >/dev/null 2>&1; then
  "${kubectl_cmd[@]}" apply --server-side -f "$keda_manifest_url" >/dev/null
fi
"${kubectl_cmd[@]}" wait --for=condition=Established crd/scaledjobs.keda.sh --timeout="$wait_timeout"
"${kubectl_cmd[@]}" rollout status deployment/keda-operator -n keda --timeout="$wait_timeout"
"${kubectl_cmd[@]}" rollout status deployment/keda-metrics-apiserver -n keda --timeout="$wait_timeout"
"${kubectl_cmd[@]}" rollout status deployment/keda-admission -n keda --timeout="$wait_timeout"

cleanup() {
  if [[ "$keep_resources" == "true" ]]; then
    return
  fi
  for name in agentstorm-keda-e2e agentstorm-keda-quota-rejected; do
    "${kubectl_cmd[@]}" delete agenttestrun "$name" -n "$e2e_namespace" --ignore-not-found --wait=true >/dev/null || true
    "${kubectl_cmd[@]}" delete scaledjob "$name-worker" -n "$e2e_namespace" --ignore-not-found --wait=true >/dev/null || true
  done
  "${kubectl_cmd[@]}" delete configmap agentstorm-keda-e2e-dataset agentstorm-keda-e2e-scenario \
    -n "$e2e_namespace" --ignore-not-found >/dev/null || true
}
trap cleanup EXIT

for name in agentstorm-keda-e2e agentstorm-keda-quota-rejected; do
  "${kubectl_cmd[@]}" delete agenttestrun "$name" -n "$e2e_namespace" --ignore-not-found --wait=true >/dev/null
  "${kubectl_cmd[@]}" delete scaledjob "$name-worker" -n "$e2e_namespace" --ignore-not-found --wait=true >/dev/null
done

"${kubectl_cmd[@]}" apply -k "$repo_root/config/dev/keda" >/dev/null
"${kubectl_cmd[@]}" set image deployment/agentstorm-controller -n "$e2e_namespace" "manager=$controller_image" >/dev/null
"${kubectl_cmd[@]}" set image deployment/agentstorm-result-api -n "$e2e_namespace" "result-api=$result_api_image" >/dev/null
"${kubectl_cmd[@]}" set env deployment/agentstorm-result-api -n "$e2e_namespace" \
  AGENTSTORM_GLOBAL_MAX_CONCURRENCY=1 \
  AGENTSTORM_GLOBAL_REQUESTS_PER_MINUTE=600 \
  'AGENTSTORM_PROVIDER_LIMITS_JSON={"fake":{"max_concurrency":1,"requests_per_minute":600}}' >/dev/null
"${kubectl_cmd[@]}" rollout restart deployment/agentstorm-controller deployment/agentstorm-result-api -n "$e2e_namespace" >/dev/null
"${kubectl_cmd[@]}" rollout status deployment/agentstorm-result-api -n "$e2e_namespace" --timeout="$wait_timeout"
"${kubectl_cmd[@]}" rollout status deployment/agentstorm-controller -n "$e2e_namespace" --timeout="$wait_timeout"

"${kubectl_cmd[@]}" apply -f - <<YAML
apiVersion: v1
kind: ConfigMap
metadata:
  name: agentstorm-keda-e2e-dataset
  namespace: $e2e_namespace
data:
  cases.jsonl: |
    {"id":"scale-1","input":"one","expected_contains":"one"}
    {"id":"scale-2","input":"two","expected_contains":"two"}
    {"id":"scale-3","input":"three","expected_contains":"three"}
    {"id":"scale-4","input":"four","expected_contains":"four"}
    {"id":"scale-5","input":"five","expected_contains":"five"}
    {"id":"scale-6","input":"six","expected_contains":"six"}
    {"id":"scale-7","input":"seven","expected_contains":"seven"}
    {"id":"scale-8","input":"eight","expected_contains":"eight"}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: agentstorm-keda-e2e-scenario
  namespace: $e2e_namespace
data:
  scenario.json: |
    {"apiVersion":"agentstorm.io/v1alpha1","kind":"FaultScenario","rules":[{"name":"hold-provider-permit","fault":"latency","probability":1,"delayMs":500}]}
YAML

initial_jobs="$("${kubectl_cmd[@]}" get jobs -n "$e2e_namespace" -l agentstorm.io/run=agentstorm-keda-e2e -o name)"
if [[ -n "$initial_jobs" ]]; then
  echo "queued run did not start from zero Worker Jobs" >&2
  exit 1
fi

"${kubectl_cmd[@]}" apply -f - <<YAML
apiVersion: agentstorm.io/v1alpha1
kind: AgentTestRun
metadata:
  name: agentstorm-keda-e2e
  namespace: $e2e_namespace
spec:
  target:
    provider: fake
  workload:
    datasetRef:
      name: agentstorm-keda-e2e-dataset
      key: cases.jsonl
    parallelism: 8
    concurrencyPerWorker: 1
    iterations: 1
    timeoutSeconds: 240
  reliability:
    seed: 42
    scenarioRef:
      name: agentstorm-keda-e2e-scenario
      key: scenario.json
  evaluation:
    minSuccessRate: 1
    maxErrorRate: 0
  scheduling:
    strategy: keda
    maxWorkers: 4
    resourceProfile: small
  runner:
    image: "$worker_image"
    imagePullPolicy: IfNotPresent
YAML

"${kubectl_cmd[@]}" wait --for=create scaledjob/agentstorm-keda-e2e-worker -n "$e2e_namespace" --timeout="$wait_timeout"
run_uid="$("${kubectl_cmd[@]}" get agenttestrun agentstorm-keda-e2e -n "$e2e_namespace" -o jsonpath='{.metadata.uid}')"
queue_json="$("${kubectl_cmd[@]}" get --raw "/api/v1/namespaces/$e2e_namespace/services/http:agentstorm-result-api:8080/proxy/v1/runs/$run_uid/queue")"
python3 -c 'import json,sys; value=json.loads(sys.argv[1]); assert value["expected"] == 8 and value["pending"] + value["leased"] + value["completed"] == 8' "$queue_json"

"${kubectl_cmd[@]}" wait --for=jsonpath='{.status.phase}'=Succeeded agenttestrun/agentstorm-keda-e2e -n "$e2e_namespace" --timeout="$wait_timeout"
queue_json="$("${kubectl_cmd[@]}" get --raw "/api/v1/namespaces/$e2e_namespace/services/http:agentstorm-result-api:8080/proxy/v1/runs/$run_uid/queue")"
python3 -c 'import json,sys; value=json.loads(sys.argv[1]); assert value["completed"] == 8 and value["available"] == 0 and value["run_status"] == "complete"' "$queue_json"

for attempt in {1..60}; do
  active_jobs="$("${kubectl_cmd[@]}" get jobs -n "$e2e_namespace" -l agentstorm.io/run=agentstorm-keda-e2e -o json | python3 -c 'import json,sys; print(sum(item.get("status",{}).get("active",0) for item in json.load(sys.stdin)["items"]))')"
  if [[ "$active_jobs" == "0" ]]; then
    break
  fi
  if [[ "$attempt" == "60" ]]; then
    echo "KEDA Workers did not return to zero active Jobs" >&2
    exit 1
  fi
  sleep 2
done
metrics="$("${kubectl_cmd[@]}" get --raw "/api/v1/namespaces/$e2e_namespace/services/http:agentstorm-result-api:8080/proxy/metrics")"
if ! grep -Eq 'agentstorm_result_api_queue_claims_total\{outcome="granted"\} [1-9]' <<<"$metrics"; then
  echo "queue claim metric was not recorded" >&2
  exit 1
fi
if ! grep -Eq 'agentstorm_result_api_scheduler_permits_total\{outcome="limited"\} [1-9]' <<<"$metrics"; then
  echo "distributed concurrency limit was not exercised while Workers scaled" >&2
  exit 1
fi

"${kubectl_cmd[@]}" apply -f - <<YAML
apiVersion: agentstorm.io/v1alpha1
kind: AgentTestRun
metadata:
  name: agentstorm-keda-quota-rejected
  namespace: $e2e_namespace
spec:
  target:
    provider: fake
  workload:
    datasetRef:
      name: agentstorm-keda-e2e-dataset
      key: cases.jsonl
    parallelism: 60
    concurrencyPerWorker: 1
    timeoutSeconds: 120
  scheduling:
    strategy: keda
    maxWorkers: 60
    resourceProfile: small
  runner:
    image: "$worker_image"
YAML
"${kubectl_cmd[@]}" wait --for=jsonpath='{.status.phase}'=Pending agenttestrun/agentstorm-keda-quota-rejected -n "$e2e_namespace" --timeout="$wait_timeout"
quota_reason="$("${kubectl_cmd[@]}" get agenttestrun agentstorm-keda-quota-rejected -n "$e2e_namespace" -o jsonpath='{.status.conditions[?(@.type=="SchedulingReady")].reason}')"
if [[ "$quota_reason" != "QuotaExceeded" ]]; then
  echo "quota violation reason = '$quota_reason', want QuotaExceeded" >&2
  exit 1
fi
if "${kubectl_cmd[@]}" get scaledjob agentstorm-keda-quota-rejected-worker -n "$e2e_namespace" >/dev/null 2>&1; then
  echo "ScaledJob was created before quota admission succeeded" >&2
  exit 1
fi

echo "AgentStorm KEDA queue E2E passed on context $kube_context"
