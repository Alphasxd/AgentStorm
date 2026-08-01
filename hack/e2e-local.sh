#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cluster_provider="${CLUSTER_PROVIDER:-auto}"
kind_cluster_name="${KIND_CLUSTER_NAME:-agentstorm}"
kube_context="${KUBE_CONTEXT:-}"
deployment_profile="${DEPLOYMENT_PROFILE:-cluster}"
e2e_namespace="${E2E_NAMESPACE:-}"
keep_resources="${KEEP_E2E_RESOURCES:-true}"
skip_image_build="${SKIP_IMAGE_BUILD:-false}"
load_local_images="${LOAD_LOCAL_IMAGES:-true}"
controller_image="${CONTROLLER_IMAGE:-agentstorm-controller:dev}"
worker_image="${WORKER_IMAGE:-agentstorm-worker:dev}"
wait_timeout="${E2E_TIMEOUT:-120s}"
kubectl_bin="${KUBECTL:-kubectl}"
docker_bin="${DOCKER:-docker}"
kind_bin="${KIND:-kind}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 1
  fi
}

require_boolean() {
  case "$2" in
    true|false) ;;
    *)
      echo "$1 must be true or false, got: $2" >&2
      exit 1
      ;;
  esac
}

require_boolean KEEP_E2E_RESOURCES "$keep_resources"
require_boolean SKIP_IMAGE_BUILD "$skip_image_build"
require_boolean LOAD_LOCAL_IMAGES "$load_local_images"
require_command "$kubectl_bin"
require_command "$docker_bin"

if [[ "$cluster_provider" == "auto" ]]; then
  if [[ -z "$kube_context" ]]; then
    kube_context="$($kubectl_bin config current-context 2>/dev/null || true)"
  fi
  case "$kube_context" in
    orbstack)
      cluster_provider="$kube_context"
      ;;
    kind-*)
      cluster_provider="kind"
      kind_cluster_name="${kube_context#kind-}"
      ;;
    *)
      echo "refusing to use non-local Kubernetes context '${kube_context:-<none>}'" >&2
      echo "set CLUSTER_PROVIDER=kind or switch to the orbstack context" >&2
      exit 1
      ;;
  esac
fi

case "$cluster_provider" in
  kind)
    require_command "$kind_bin"
    kube_context="kind-$kind_cluster_name"
    kind_exists="false"
    while IFS= read -r cluster_name; do
      if [[ "$cluster_name" == "$kind_cluster_name" ]]; then
        kind_exists="true"
        break
      fi
    done < <("$kind_bin" get clusters 2>/dev/null)
    if [[ "$kind_exists" == "false" ]]; then
      "$kind_bin" create cluster --name "$kind_cluster_name" --wait "$wait_timeout"
    fi
    ;;
  orbstack)
    if [[ -z "$kube_context" ]]; then
      kube_context="$cluster_provider"
    fi
    if [[ "$kube_context" != "$cluster_provider" ]]; then
      echo "KUBE_CONTEXT=$kube_context does not match CLUSTER_PROVIDER=$cluster_provider" >&2
      exit 1
    fi
    docker_context="$($docker_bin context show)"
    if [[ "$docker_context" != "$cluster_provider" ]]; then
      echo "Docker context '$docker_context' must match Kubernetes context '$cluster_provider'" >&2
      exit 1
    fi
    ;;
  *)
    echo "unsupported CLUSTER_PROVIDER: $cluster_provider" >&2
    exit 1
    ;;
esac

case "$deployment_profile" in
  cluster)
    kustomize_dir="$repo_root/config/default"
    if [[ -z "$e2e_namespace" ]]; then
      e2e_namespace="default"
    fi
    ;;
  namespace)
    kustomize_dir="$repo_root/config/namespace-scoped"
    if [[ -z "$e2e_namespace" ]]; then
      e2e_namespace="agentstorm-system"
    fi
    if [[ "$e2e_namespace" != "agentstorm-system" ]]; then
      echo "namespace profile currently requires E2E_NAMESPACE=agentstorm-system" >&2
      exit 1
    fi
    ;;
  *)
    echo "unsupported DEPLOYMENT_PROFILE: $deployment_profile" >&2
    exit 1
    ;;
esac

kubectl_cmd=("$kubectl_bin" --context "$kube_context")

diagnostics() {
  exit_code=$?
  trap - ERR
  echo "AgentStorm E2E failed; collecting non-secret diagnostics" >&2
  "${kubectl_cmd[@]}" get deployment,pods -n agentstorm-system -o wide >&2 || true
  "${kubectl_cmd[@]}" get agenttestruns,jobs,pods -n "$e2e_namespace" -o wide >&2 || true
  "${kubectl_cmd[@]}" logs -n agentstorm-system deployment/agentstorm-controller --tail=200 >&2 || true
  exit "$exit_code"
}
trap diagnostics ERR

"${kubectl_cmd[@]}" cluster-info >/dev/null

if [[ "$skip_image_build" == "false" ]]; then
  make -C "$repo_root" docker-build CONTROLLER_IMAGE="$controller_image" WORKER_IMAGE="$worker_image"
fi

if [[ "$cluster_provider" == "kind" && "$load_local_images" == "true" ]]; then
  "$kind_bin" load docker-image "$controller_image" "$worker_image" --name "$kind_cluster_name"
fi

"${kubectl_cmd[@]}" apply -k "$kustomize_dir"
if [[ "$deployment_profile" == "namespace" ]]; then
  "${kubectl_cmd[@]}" delete clusterrolebinding agentstorm-controller --ignore-not-found
  "${kubectl_cmd[@]}" delete clusterrole agentstorm-controller --ignore-not-found
else
  "${kubectl_cmd[@]}" delete rolebinding agentstorm-controller -n agentstorm-system --ignore-not-found
  "${kubectl_cmd[@]}" delete role agentstorm-controller -n agentstorm-system --ignore-not-found
  "${kubectl_cmd[@]}" delete networkpolicy agentstorm-worker -n agentstorm-system --ignore-not-found
fi
"${kubectl_cmd[@]}" set image deployment/agentstorm-controller -n agentstorm-system "manager=$controller_image"
"${kubectl_cmd[@]}" rollout restart deployment/agentstorm-controller -n agentstorm-system
"${kubectl_cmd[@]}" rollout status deployment/agentstorm-controller -n agentstorm-system --timeout="$wait_timeout"

"${kubectl_cmd[@]}" apply --server-side --dry-run=server -n "$e2e_namespace" \
  -f "$repo_root/config/samples/agentstorm_v1alpha1_agenttestrun.yaml" >/dev/null

"${kubectl_cmd[@]}" apply -f - <<YAML
apiVersion: v1
kind: ConfigMap
metadata:
  name: agentstorm-demo-dataset
  namespace: $e2e_namespace
data:
  cases.jsonl: |
    {"id":"greeting-1","input":"hello","expected_contains":"hello"}
    {"id":"greeting-2","input":"agent reliability","expected_contains":"agent reliability"}
YAML

service_account="system:serviceaccount:agentstorm-system:agentstorm-controller"
can_get_secrets="$("${kubectl_cmd[@]}" auth can-i get secrets --as="$service_account" -n "$e2e_namespace" || true)"
can_list_secrets="$("${kubectl_cmd[@]}" auth can-i list secrets --as="$service_account" -n "$e2e_namespace" || true)"
if [[ "$can_get_secrets" != "yes" || "$can_list_secrets" != "no" ]]; then
  echo "unexpected Secret RBAC: get=$can_get_secrets list=$can_list_secrets" >&2
  exit 1
fi
if [[ "$deployment_profile" == "namespace" ]]; then
  can_watch_all_runs="$("${kubectl_cmd[@]}" auth can-i watch agenttestruns.agentstorm.io --as="$service_account" --all-namespaces || true)"
  if [[ "$can_watch_all_runs" != "no" ]]; then
    echo "namespace-scoped ServiceAccount can unexpectedly watch all AgentTestRuns" >&2
    exit 1
  fi
fi

delete_run() {
  run_name="$1"
  "${kubectl_cmd[@]}" delete agenttestrun "$run_name" -n "$e2e_namespace" --ignore-not-found --wait=true
  if "${kubectl_cmd[@]}" get job "$run_name-worker" -n "$e2e_namespace" >/dev/null 2>&1; then
    "${kubectl_cmd[@]}" wait --for=delete "job/$run_name-worker" -n "$e2e_namespace" --timeout="$wait_timeout"
  fi
  if "${kubectl_cmd[@]}" get configmap "$run_name-config" -n "$e2e_namespace" >/dev/null 2>&1; then
    "${kubectl_cmd[@]}" wait --for=delete "configmap/$run_name-config" -n "$e2e_namespace" --timeout="$wait_timeout"
  fi
}

run_success_case() {
  delete_run agentstorm-demo
  "${kubectl_cmd[@]}" apply -f - <<YAML
apiVersion: agentstorm.io/v1alpha1
kind: AgentTestRun
metadata:
  name: agentstorm-demo
  namespace: $e2e_namespace
spec:
  target:
    provider: fake
  workload:
    datasetRef:
      name: agentstorm-demo-dataset
      key: cases.jsonl
    parallelism: 2
    concurrencyPerWorker: 4
    iterations: 1
    timeoutSeconds: 300
  evaluation:
    minSuccessRate: 1
    maxErrorRate: 0
    maxP95LatencyMs: 1000
  runner:
    image: "$worker_image"
    imagePullPolicy: IfNotPresent
YAML
  "${kubectl_cmd[@]}" wait --for=jsonpath='{.status.phase}'=Succeeded agenttestrun/agentstorm-demo -n "$e2e_namespace" --timeout="$wait_timeout"

  job_name="$("${kubectl_cmd[@]}" get agenttestrun agentstorm-demo -n "$e2e_namespace" -o jsonpath='{.status.jobName}')"
  succeeded="$("${kubectl_cmd[@]}" get agenttestrun agentstorm-demo -n "$e2e_namespace" -o jsonpath='{.status.succeeded}')"
  if [[ "$job_name" != "agentstorm-demo-worker" || "$succeeded" != "2" ]]; then
    echo "unexpected successful run status: job=$job_name succeeded=$succeeded" >&2
    return 1
  fi
  "${kubectl_cmd[@]}" wait --for=condition=complete "job/$job_name" -n "$e2e_namespace" --timeout="$wait_timeout"
  "${kubectl_cmd[@]}" logs -n "$e2e_namespace" -l agentstorm.io/run=agentstorm-demo --all-containers=true --prefix=true
}

run_success_case

delete_run agentstorm-demo
if ! "${kubectl_cmd[@]}" get configmap agentstorm-demo-dataset -n "$e2e_namespace" >/dev/null; then
  echo "dataset ConfigMap was incorrectly garbage-collected" >&2
  exit 1
fi

delete_run agentstorm-secret-gate
"${kubectl_cmd[@]}" delete secret agentstorm-e2e-credentials -n "$e2e_namespace" --ignore-not-found
"${kubectl_cmd[@]}" apply -f - <<YAML
apiVersion: agentstorm.io/v1alpha1
kind: AgentTestRun
metadata:
  name: agentstorm-secret-gate
  namespace: $e2e_namespace
spec:
  target:
    provider: fake
    apiKeySecretRef:
      name: agentstorm-e2e-credentials
      key: api-key
  workload:
    datasetRef:
      name: agentstorm-demo-dataset
      key: cases.jsonl
    parallelism: 1
    concurrencyPerWorker: 1
    iterations: 1
    timeoutSeconds: 120
  evaluation:
    minSuccessRate: 1
    maxErrorRate: 0
    maxP95LatencyMs: 1000
  runner:
    image: "$worker_image"
    imagePullPolicy: IfNotPresent
YAML
"${kubectl_cmd[@]}" wait --for=jsonpath='{.status.phase}'=Pending agenttestrun/agentstorm-secret-gate -n "$e2e_namespace" --timeout="$wait_timeout"
credentials_reason="$("${kubectl_cmd[@]}" get agenttestrun agentstorm-secret-gate -n "$e2e_namespace" -o jsonpath='{.status.conditions[?(@.type=="CredentialsReady")].reason}')"
if [[ "$credentials_reason" != "SecretMissing" ]]; then
  echo "missing Secret condition reason = '$credentials_reason', want SecretMissing" >&2
  exit 1
fi
if "${kubectl_cmd[@]}" get job agentstorm-secret-gate-worker -n "$e2e_namespace" >/dev/null 2>&1; then
  echo "worker Job was created before the required Secret became available" >&2
  exit 1
fi
"${kubectl_cmd[@]}" create secret generic agentstorm-e2e-credentials -n "$e2e_namespace" --from-literal=api-key=test-only-value
"${kubectl_cmd[@]}" wait --for=jsonpath='{.status.phase}'=Succeeded agenttestrun/agentstorm-secret-gate -n "$e2e_namespace" --timeout="$wait_timeout"
worker_config="$("${kubectl_cmd[@]}" get configmap agentstorm-secret-gate-config -n "$e2e_namespace" -o jsonpath='{.data.run\.json}')"
case "$worker_config" in
  *agentstorm-e2e-credentials*|*api-key*|*test-only-value*)
    echo "generated worker ConfigMap contains provider Secret material" >&2
    exit 1
    ;;
esac
delete_run agentstorm-secret-gate
"${kubectl_cmd[@]}" delete secret agentstorm-e2e-credentials -n "$e2e_namespace" --ignore-not-found

"${kubectl_cmd[@]}" apply -f - <<YAML
apiVersion: agentstorm.io/v1alpha1
kind: AgentTestRun
metadata:
  name: agentstorm-cancel
  namespace: $e2e_namespace
spec:
  target:
    provider: fake
  workload:
    datasetRef:
      name: agentstorm-demo-dataset
      key: cases.jsonl
    parallelism: 1
    concurrencyPerWorker: 1
    iterations: 10000
    timeoutSeconds: 120
  evaluation:
    minSuccessRate: 1
    maxErrorRate: 0
    maxP95LatencyMs: 1000
  runner:
    image: "$worker_image"
    imagePullPolicy: IfNotPresent
YAML
"${kubectl_cmd[@]}" wait --for=jsonpath='{.status.phase}'=Running agenttestrun/agentstorm-cancel -n "$e2e_namespace" --timeout="$wait_timeout"
"${kubectl_cmd[@]}" patch agenttestrun agentstorm-cancel -n "$e2e_namespace" --type=merge -p '{"spec":{"cancel":true}}'
"${kubectl_cmd[@]}" wait --for=jsonpath='{.status.phase}'=Cancelled agenttestrun/agentstorm-cancel -n "$e2e_namespace" --timeout="$wait_timeout"
if "${kubectl_cmd[@]}" get job agentstorm-cancel-worker -n "$e2e_namespace" >/dev/null 2>&1; then
  "${kubectl_cmd[@]}" wait --for=delete job/agentstorm-cancel-worker -n "$e2e_namespace" --timeout="$wait_timeout"
fi
sleep 6
if "${kubectl_cmd[@]}" get job agentstorm-cancel-worker -n "$e2e_namespace" >/dev/null 2>&1; then
  echo "cancelled worker Job was recreated" >&2
  exit 1
fi
delete_run agentstorm-cancel

if [[ "$keep_resources" == "true" ]]; then
  run_success_case
else
  delete_run agentstorm-demo
  "${kubectl_cmd[@]}" delete configmap agentstorm-demo-dataset -n "$e2e_namespace" --ignore-not-found
fi

echo "AgentStorm E2E passed: context=$kube_context profile=$deployment_profile namespace=$e2e_namespace controller_image=$controller_image worker_image=$worker_image"
