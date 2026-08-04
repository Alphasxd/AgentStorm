#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cluster_provider="${CLUSTER_PROVIDER:-auto}"
kind_cluster_name="${KIND_CLUSTER_NAME:-agentstorm}"
kube_context="${KUBE_CONTEXT:-}"
e2e_namespace="${E2E_NAMESPACE:-agentstorm-system}"
keep_resources="${KEEP_E2E_RESOURCES:-true}"
skip_image_build="${SKIP_IMAGE_BUILD:-false}"
load_local_images="${LOAD_LOCAL_IMAGES:-true}"
telemetry_e2e="${ENABLE_TELEMETRY_E2E:-}"
expect_cost_accounting="${EXPECT_COST_ACCOUNTING:-}"
reliability_e2e="${ENABLE_RELIABILITY_E2E:-}"
controller_image="${CONTROLLER_IMAGE:-agentstorm-controller:dev}"
worker_image="${WORKER_IMAGE:-agentstorm-worker:dev}"
result_api_image="${RESULT_API_IMAGE:-agentstorm-result-api:dev}"
wait_timeout="${E2E_TIMEOUT:-180s}"
local_port="${RESULT_API_LOCAL_PORT:-18080}"
tempo_local_port="${TEMPO_LOCAL_PORT:-13200}"
prometheus_local_port="${PROMETHEUS_LOCAL_PORT:-19090}"
grafana_local_port="${GRAFANA_LOCAL_PORT:-13000}"
kubectl_bin="${KUBECTL:-kubectl}"
docker_bin="${DOCKER:-docker}"
kind_bin="${KIND:-kind}"
test_dir=""
port_forward_pids=()
write_token=""
read_token=""
postgres_password=""
s3_secret_key=""

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
if [[ -z "$telemetry_e2e" ]]; then
  if [[ "$skip_image_build" == "false" ]]; then
    telemetry_e2e="true"
  else
    telemetry_e2e="false"
  fi
fi
if [[ -z "$expect_cost_accounting" ]]; then
  if [[ "$skip_image_build" == "false" ]]; then
    expect_cost_accounting="true"
  else
    expect_cost_accounting="false"
  fi
fi
if [[ -z "$reliability_e2e" ]]; then
  if [[ "$skip_image_build" == "false" ]]; then
    reliability_e2e="true"
  else
    reliability_e2e="false"
  fi
fi
require_boolean ENABLE_TELEMETRY_E2E "$telemetry_e2e"
require_boolean EXPECT_COST_ACCOUNTING "$expect_cost_accounting"
require_boolean ENABLE_RELIABILITY_E2E "$reliability_e2e"
require_command "$kubectl_bin"
require_command "$docker_bin"
require_command curl
require_command python3

random_secret() {
  python3 -c 'import secrets; print(secrets.token_hex(24))'
}

write_token="$(random_secret)"
read_token="$(random_secret)"
postgres_password="$(random_secret)"
s3_secret_key="$(random_secret)"

if [[ "$e2e_namespace" != "agentstorm-system" ]]; then
  echo "the result-stack overlay currently requires E2E_NAMESPACE=agentstorm-system" >&2
  exit 1
fi

# Exercise the controller lifecycle, RBAC, Secret, cancellation, and garbage-collection paths first.
# It also creates or validates the requested local cluster.
KEEP_E2E_RESOURCES=false DEPLOYMENT_PROFILE=namespace DISABLE_LOCAL_NETWORK_POLICIES=true \
  "$repo_root/hack/e2e-local.sh"

if [[ "$cluster_provider" == "auto" ]]; then
  if [[ -z "$kube_context" ]]; then
    kube_context="$($kubectl_bin config current-context 2>/dev/null || true)"
  fi
  case "$kube_context" in
    orbstack)
      cluster_provider="orbstack"
      ;;
    kind-*)
      cluster_provider="kind"
      kind_cluster_name="${kube_context#kind-}"
      ;;
    *)
      echo "refusing to use non-local Kubernetes context '${kube_context:-<none>}'" >&2
      exit 1
      ;;
  esac
elif [[ -z "$kube_context" ]]; then
  case "$cluster_provider" in
    kind) kube_context="kind-$kind_cluster_name" ;;
    orbstack) kube_context="orbstack" ;;
  esac
fi

case "$cluster_provider" in
  kind)
    require_command "$kind_bin"
    ;;
  orbstack) ;;
  *)
    echo "unsupported CLUSTER_PROVIDER: $cluster_provider" >&2
    exit 1
    ;;
esac

kubectl_cmd=("$kubectl_bin" --context "$kube_context")
test_dir="$(mktemp -d)"

stop_port_forwards() {
  if (( ${#port_forward_pids[@]} > 0 )); then
    for pid in "${port_forward_pids[@]}"; do
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    done
  fi
  port_forward_pids=()
}

start_port_forward() {
  service_name="$1"
  local_service_port="$2"
  remote_service_port="$3"
  log_name="$4"
  "${kubectl_cmd[@]}" port-forward -n "$e2e_namespace" "service/$service_name" \
    "$local_service_port:$remote_service_port" >"$test_dir/$log_name-port-forward.log" 2>&1 &
  port_forward_pids+=("$!")
}

delete_run() {
  run_name="$1"
  "${kubectl_cmd[@]}" delete agenttestrun "$run_name" -n "$e2e_namespace" --ignore-not-found --wait=true >/dev/null
  "${kubectl_cmd[@]}" delete job "$run_name-worker" -n "$e2e_namespace" --ignore-not-found --wait=true >/dev/null
  "${kubectl_cmd[@]}" delete configmap "$run_name-config" -n "$e2e_namespace" --ignore-not-found --wait=true >/dev/null
}

delete_managed_secret() {
  secret_name="$1"
  if ! "${kubectl_cmd[@]}" get secret "$secret_name" -n "$e2e_namespace" >/dev/null 2>&1; then
    return
  fi
  manager="$("${kubectl_cmd[@]}" get secret "$secret_name" -n "$e2e_namespace" -o jsonpath='{.metadata.labels.app\.kubernetes\.io/managed-by}')"
  if [[ "$manager" != "agentstorm-e2e" ]]; then
    echo "refusing to replace unmanaged Secret $e2e_namespace/$secret_name" >&2
    exit 1
  fi
  "${kubectl_cmd[@]}" delete secret "$secret_name" -n "$e2e_namespace" --wait=true >/dev/null
}

assert_secret_is_managed() {
  secret_name="$1"
  if ! "${kubectl_cmd[@]}" get secret "$secret_name" -n "$e2e_namespace" >/dev/null 2>&1; then
    return
  fi
  manager="$("${kubectl_cmd[@]}" get secret "$secret_name" -n "$e2e_namespace" -o jsonpath='{.metadata.labels.app\.kubernetes\.io/managed-by}')"
  if [[ "$manager" != "agentstorm-e2e" ]]; then
    echo "refusing to reset result stack backed by unmanaged Secret $e2e_namespace/$secret_name" >&2
    exit 1
  fi
}

create_auth_secret() {
  "${kubectl_cmd[@]}" create secret generic agentstorm-result-auth -n "$e2e_namespace" \
    --from-literal=write-token="$write_token" \
    --from-literal=read-token="$read_token" >/dev/null
  "${kubectl_cmd[@]}" label secret agentstorm-result-auth -n "$e2e_namespace" \
    app.kubernetes.io/managed-by=agentstorm-e2e \
    app.kubernetes.io/part-of=agentstorm-results >/dev/null
}

cleanup_stack() {
  stop_port_forwards
  rm -rf -- "$test_dir"
  if [[ "$keep_resources" == "true" ]]; then
    return
  fi

  for run_name in \
    agentstorm-results-secret-gate \
    agentstorm-results-baseline \
    agentstorm-results-candidate \
    agentstorm-results-failure \
    agentstorm-results-cancel \
    agentstorm-results-reliability-faults \
    agentstorm-results-retry-default \
    agentstorm-results-retry-ambiguous \
    agentstorm-results-deterministic-a \
    agentstorm-results-deterministic-b \
    agentstorm-results-circuit \
    agentstorm-results-redacted \
    agentstorm-results-reference; do
    delete_run "$run_name" || true
  done
  "${kubectl_cmd[@]}" delete configmap agentstorm-results-dataset agentstorm-results-cancel-scenario \
    agentstorm-results-reliability-scenarios -n "$e2e_namespace" --ignore-not-found >/dev/null || true
  "${kubectl_cmd[@]}" delete deployment agentstorm-result-api -n "$e2e_namespace" --ignore-not-found >/dev/null || true
  "${kubectl_cmd[@]}" delete deployment agentstorm-otel-collector agentstorm-tempo agentstorm-prometheus agentstorm-grafana \
    -n "$e2e_namespace" --ignore-not-found >/dev/null || true
  "${kubectl_cmd[@]}" delete service agentstorm-otel-collector agentstorm-tempo agentstorm-prometheus agentstorm-grafana \
    -n "$e2e_namespace" --ignore-not-found >/dev/null || true
  "${kubectl_cmd[@]}" delete configmap agentstorm-otel-collector agentstorm-tempo agentstorm-prometheus \
    agentstorm-grafana-provisioning agentstorm-grafana-dashboard -n "$e2e_namespace" --ignore-not-found >/dev/null || true
  "${kubectl_cmd[@]}" delete pvc agentstorm-tempo agentstorm-prometheus agentstorm-grafana \
    -n "$e2e_namespace" --ignore-not-found >/dev/null || true
  "${kubectl_cmd[@]}" delete statefulset agentstorm-postgres agentstorm-minio -n "$e2e_namespace" --ignore-not-found >/dev/null || true
  "${kubectl_cmd[@]}" delete service agentstorm-result-api agentstorm-postgres agentstorm-minio -n "$e2e_namespace" --ignore-not-found >/dev/null || true
  "${kubectl_cmd[@]}" delete networkpolicy agentstorm-result-api agentstorm-postgres agentstorm-minio -n "$e2e_namespace" --ignore-not-found >/dev/null || true
  "${kubectl_cmd[@]}" delete pvc -n "$e2e_namespace" -l app.kubernetes.io/part-of=agentstorm-results --ignore-not-found >/dev/null || true
  "${kubectl_cmd[@]}" delete secret -n "$e2e_namespace" -l app.kubernetes.io/managed-by=agentstorm-e2e,app.kubernetes.io/part-of=agentstorm-results --ignore-not-found >/dev/null || true
  "${kubectl_cmd[@]}" apply -k "$repo_root/config/namespace-scoped" >/dev/null || true
  "${kubectl_cmd[@]}" delete networkpolicy agentstorm-controller agentstorm-worker \
    -n "$e2e_namespace" --ignore-not-found >/dev/null || true
  "${kubectl_cmd[@]}" set image deployment/agentstorm-controller -n "$e2e_namespace" "manager=$controller_image" >/dev/null || true
}

diagnostics() {
  exit_code=$?
  trap - ERR
  echo "AgentStorm result-stack E2E failed; collecting non-secret diagnostics" >&2
  "${kubectl_cmd[@]}" get deployment,statefulset,pods -n "$e2e_namespace" -o wide >&2 || true
  "${kubectl_cmd[@]}" get agenttestruns,jobs -n "$e2e_namespace" -o wide >&2 || true
  "${kubectl_cmd[@]}" logs -n "$e2e_namespace" deployment/agentstorm-controller --tail=200 >&2 || true
  "${kubectl_cmd[@]}" logs -n "$e2e_namespace" deployment/agentstorm-result-api --tail=200 >&2 || true
  "${kubectl_cmd[@]}" logs -n "$e2e_namespace" deployment/agentstorm-otel-collector --tail=200 >&2 || true
  "${kubectl_cmd[@]}" logs -n "$e2e_namespace" deployment/agentstorm-tempo --tail=200 >&2 || true
  "${kubectl_cmd[@]}" logs -n "$e2e_namespace" deployment/agentstorm-prometheus --tail=200 >&2 || true
  "${kubectl_cmd[@]}" logs -n "$e2e_namespace" deployment/agentstorm-grafana --tail=200 >&2 || true
  exit "$exit_code"
}
trap diagnostics ERR
trap cleanup_stack EXIT

if [[ "$skip_image_build" == "false" ]]; then
  make -C "$repo_root" docker-build-result-api RESULT_API_IMAGE="$result_api_image"
fi
if [[ "$cluster_provider" == "kind" && "$load_local_images" == "true" ]]; then
  "$kind_bin" load docker-image "$result_api_image" --name "$kind_cluster_name"
fi

for run_name in \
  agentstorm-results-secret-gate \
  agentstorm-results-baseline \
  agentstorm-results-candidate \
  agentstorm-results-failure \
  agentstorm-results-cancel \
  agentstorm-results-reliability-faults \
  agentstorm-results-retry-default \
  agentstorm-results-retry-ambiguous \
  agentstorm-results-deterministic-a \
  agentstorm-results-deterministic-b \
  agentstorm-results-circuit \
  agentstorm-results-redacted \
  agentstorm-results-reference; do
  delete_run "$run_name"
done
"${kubectl_cmd[@]}" delete configmap agentstorm-results-dataset agentstorm-results-cancel-scenario \
  agentstorm-results-reliability-scenarios -n "$e2e_namespace" --ignore-not-found >/dev/null

# Persistent database credentials are initialized only once. Reset the complete test-owned stack so
# a retained PVC can never be paired with a newly generated password on a later E2E invocation.
assert_secret_is_managed agentstorm-result-storage
assert_secret_is_managed agentstorm-result-auth
"${kubectl_cmd[@]}" delete deployment agentstorm-result-api -n "$e2e_namespace" --ignore-not-found --wait=true >/dev/null
"${kubectl_cmd[@]}" delete deployment agentstorm-otel-collector agentstorm-tempo agentstorm-prometheus agentstorm-grafana \
  -n "$e2e_namespace" --ignore-not-found --wait=true >/dev/null
"${kubectl_cmd[@]}" delete service agentstorm-otel-collector agentstorm-tempo agentstorm-prometheus agentstorm-grafana \
  -n "$e2e_namespace" --ignore-not-found >/dev/null
"${kubectl_cmd[@]}" delete configmap agentstorm-otel-collector agentstorm-tempo agentstorm-prometheus \
  agentstorm-grafana-provisioning agentstorm-grafana-dashboard -n "$e2e_namespace" --ignore-not-found >/dev/null
"${kubectl_cmd[@]}" delete statefulset agentstorm-postgres agentstorm-minio -n "$e2e_namespace" --ignore-not-found --wait=true >/dev/null
"${kubectl_cmd[@]}" delete service agentstorm-result-api agentstorm-postgres agentstorm-minio -n "$e2e_namespace" --ignore-not-found >/dev/null
"${kubectl_cmd[@]}" delete networkpolicy agentstorm-result-api agentstorm-postgres agentstorm-minio -n "$e2e_namespace" --ignore-not-found >/dev/null
"${kubectl_cmd[@]}" delete pvc -n "$e2e_namespace" -l app.kubernetes.io/part-of=agentstorm-results --ignore-not-found --wait=true >/dev/null
delete_managed_secret agentstorm-result-storage
delete_managed_secret agentstorm-result-auth
"${kubectl_cmd[@]}" create secret generic agentstorm-result-storage -n "$e2e_namespace" \
  --from-literal=database-url="postgres://agentstorm:$postgres_password@agentstorm-postgres:5432/agentstorm?sslmode=disable" \
  --from-literal=postgres-password="$postgres_password" \
  --from-literal=s3-access-key='agentstorm' \
  --from-literal=s3-secret-key="$s3_secret_key" >/dev/null
"${kubectl_cmd[@]}" label secret agentstorm-result-storage -n "$e2e_namespace" \
  app.kubernetes.io/managed-by=agentstorm-e2e \
  app.kubernetes.io/part-of=agentstorm-results >/dev/null
create_auth_secret

result_overlay="$repo_root/config/dev/results"
if [[ "$telemetry_e2e" == "true" ]]; then
  result_overlay="$repo_root/config/dev/telemetry"
fi
"${kubectl_cmd[@]}" apply -k "$result_overlay" >/dev/null
"${kubectl_cmd[@]}" set image deployment/agentstorm-controller -n "$e2e_namespace" "manager=$controller_image" >/dev/null
"${kubectl_cmd[@]}" set image deployment/agentstorm-result-api -n "$e2e_namespace" "result-api=$result_api_image" >/dev/null
"${kubectl_cmd[@]}" rollout restart deployment/agentstorm-controller -n "$e2e_namespace" >/dev/null
"${kubectl_cmd[@]}" rollout restart deployment/agentstorm-result-api -n "$e2e_namespace" >/dev/null
if [[ "$telemetry_e2e" == "true" ]]; then
  "${kubectl_cmd[@]}" rollout restart deployment/agentstorm-otel-collector -n "$e2e_namespace" >/dev/null
  "${kubectl_cmd[@]}" rollout restart deployment/agentstorm-tempo -n "$e2e_namespace" >/dev/null
  "${kubectl_cmd[@]}" rollout restart deployment/agentstorm-prometheus -n "$e2e_namespace" >/dev/null
  "${kubectl_cmd[@]}" rollout restart deployment/agentstorm-grafana -n "$e2e_namespace" >/dev/null
fi
"${kubectl_cmd[@]}" rollout status statefulset/agentstorm-postgres -n "$e2e_namespace" --timeout="$wait_timeout"
"${kubectl_cmd[@]}" rollout status statefulset/agentstorm-minio -n "$e2e_namespace" --timeout="$wait_timeout"
"${kubectl_cmd[@]}" rollout status deployment/agentstorm-result-api -n "$e2e_namespace" --timeout="$wait_timeout"
if [[ "$telemetry_e2e" == "true" ]]; then
  "${kubectl_cmd[@]}" rollout status deployment/agentstorm-tempo -n "$e2e_namespace" --timeout="$wait_timeout"
  "${kubectl_cmd[@]}" rollout status deployment/agentstorm-prometheus -n "$e2e_namespace" --timeout="$wait_timeout"
  "${kubectl_cmd[@]}" rollout status deployment/agentstorm-grafana -n "$e2e_namespace" --timeout="$wait_timeout"
  "${kubectl_cmd[@]}" rollout status deployment/agentstorm-otel-collector -n "$e2e_namespace" --timeout="$wait_timeout"
fi
"${kubectl_cmd[@]}" rollout status deployment/agentstorm-controller -n "$e2e_namespace" --timeout="$wait_timeout"

"${kubectl_cmd[@]}" apply -f - <<YAML
apiVersion: v1
kind: ConfigMap
metadata:
  name: agentstorm-results-dataset
  namespace: $e2e_namespace
data:
  cases.jsonl: |
    {"id":"case-a","input":"alpha","expected_contains":"alpha"}
    {"id":"case-b","input":"beta","expected_contains":"beta"}
  failed.jsonl: |
    {"id":"case-failure","input":"gamma","expected_contains":"never-redacted-value"}
  redacted.jsonl: |
    {"id":"case-redacted","input":"contact customer-42@example.com","metadata":{"tenant":{"name":"team-blue","api_key":"metadata-sensitive-canary"},"internal":"internal-canary"}}
  reliability.jsonl: |
    {"id":"fault-latency","input":"latency","expected_contains":"latency"}
    {"id":"fault-timeout","input":"timeout","expected_contains":"timeout"}
    {"id":"fault-http","input":"http","expected_contains":"http"}
    {"id":"fault-malformed","input":"malformed","expected_contains":"malformed"}
    {"id":"fault-rate-limit","input":"rate-limit","expected_contains":"rate-limit"}
    {"id":"fault-tool","input":"tool","expected_contains":"tool"}
    {"id":"fault-quality","input":"quality","expected_contains":"not-present"}
  retry.jsonl: |
    {"id":"retry-rate-limit","input":"rate-limit","expected_contains":"rate-limit"}
    {"id":"retry-timeout","input":"timeout","expected_contains":"timeout"}
    {"id":"retry-http","input":"http","expected_contains":"http"}
    {"id":"retry-malformed","input":"malformed","expected_contains":"malformed"}
  deterministic.jsonl: |
    {"id":"deterministic-01","input":"one","expected_contains":"one"}
    {"id":"deterministic-02","input":"two","expected_contains":"two"}
    {"id":"deterministic-03","input":"three","expected_contains":"three"}
    {"id":"deterministic-04","input":"four","expected_contains":"four"}
    {"id":"deterministic-05","input":"five","expected_contains":"five"}
    {"id":"deterministic-06","input":"six","expected_contains":"six"}
    {"id":"deterministic-07","input":"seven","expected_contains":"seven"}
    {"id":"deterministic-08","input":"eight","expected_contains":"eight"}
    {"id":"deterministic-09","input":"nine","expected_contains":"nine"}
    {"id":"deterministic-10","input":"ten","expected_contains":"ten"}
  circuit.jsonl: |
    {"id":"circuit-a","input":"a","expected_contains":"a"}
    {"id":"circuit-b","input":"b","expected_contains":"b"}
    {"id":"circuit-c","input":"c","expected_contains":"c"}
    {"id":"circuit-d","input":"d","expected_contains":"d"}
YAML

if [[ "$reliability_e2e" == "true" ]]; then
  "${kubectl_cmd[@]}" apply -f - <<YAML
apiVersion: v1
kind: ConfigMap
metadata:
  name: agentstorm-results-cancel-scenario
  namespace: $e2e_namespace
data:
  scenario.json: |
    {"apiVersion":"agentstorm.io/v1alpha1","kind":"FaultScenario","rules":[{"name":"hold-first","fault":"latency","probability":1,"caseIDs":["case-a"],"delayMs":30000}]}
YAML

  "${kubectl_cmd[@]}" apply -f - <<YAML
apiVersion: v1
kind: ConfigMap
metadata:
  name: agentstorm-results-reliability-scenarios
  namespace: $e2e_namespace
data:
  faults.json: |
    {"apiVersion":"agentstorm.io/v1alpha1","kind":"FaultScenario","rules":[{"name":"latency","fault":"latency","probability":1,"caseIDs":["fault-latency"],"delayMs":1},{"name":"timeout","fault":"timeout","probability":1,"caseIDs":["fault-timeout"],"delayMs":1},{"name":"http-500","fault":"http_error","probability":1,"caseIDs":["fault-http"],"statusCode":500},{"name":"malformed","fault":"malformed_response","probability":1,"caseIDs":["fault-malformed"]},{"name":"rate-limit","fault":"rate_limit","probability":1,"caseIDs":["fault-rate-limit"]},{"name":"tool","fault":"tool_error","probability":1,"caseIDs":["fault-tool"],"toolName":"fake.echo"}]}
  retry.json: |
    {"apiVersion":"agentstorm.io/v1alpha1","kind":"FaultScenario","rules":[{"name":"retry-rate-limit","fault":"rate_limit","probability":1,"caseIDs":["retry-rate-limit"],"attempts":[1]},{"name":"retry-timeout","fault":"timeout","probability":1,"caseIDs":["retry-timeout"],"attempts":[1],"delayMs":1},{"name":"retry-http","fault":"http_error","probability":1,"caseIDs":["retry-http"],"attempts":[1],"statusCode":500},{"name":"retry-malformed","fault":"malformed_response","probability":1,"caseIDs":["retry-malformed"],"attempts":[1]}]}
  deterministic.json: |
    {"apiVersion":"agentstorm.io/v1alpha1","kind":"FaultScenario","rules":[{"name":"stable-selection","fault":"latency","probability":0.5,"delayMs":1}]}
  circuit.json: |
    {"apiVersion":"agentstorm.io/v1alpha1","kind":"FaultScenario","rules":[{"name":"open-on-first","fault":"rate_limit","probability":1,"caseIDs":["circuit-a"],"iterations":[0],"attempts":[1]}]}
YAML
fi

# The running API keeps its already-resolved credentials, while the controller must gate new Jobs
# until the per-namespace worker-write Secret exists again.
delete_managed_secret agentstorm-result-auth
"${kubectl_cmd[@]}" apply -f - <<YAML
apiVersion: agentstorm.io/v1alpha1
kind: AgentTestRun
metadata:
  name: agentstorm-results-secret-gate
  namespace: $e2e_namespace
spec:
  target:
    provider: fake
  workload:
    datasetRef:
      name: agentstorm-results-dataset
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
"${kubectl_cmd[@]}" wait --for=jsonpath='{.status.phase}'=Pending agenttestrun/agentstorm-results-secret-gate -n "$e2e_namespace" --timeout="$wait_timeout"
result_sink_reason="$("${kubectl_cmd[@]}" get agenttestrun agentstorm-results-secret-gate -n "$e2e_namespace" -o jsonpath='{.status.conditions[?(@.type=="ResultSinkReady")].reason}')"
if [[ "$result_sink_reason" != "SecretMissing" ]]; then
  echo "missing Result API Secret condition reason = '$result_sink_reason', want SecretMissing" >&2
  exit 1
fi
if "${kubectl_cmd[@]}" get job agentstorm-results-secret-gate-worker -n "$e2e_namespace" >/dev/null 2>&1; then
  echo "worker Job was created before the Result API write Secret became available" >&2
  exit 1
fi
create_auth_secret
"${kubectl_cmd[@]}" wait --for=jsonpath='{.status.phase}'=Succeeded agenttestrun/agentstorm-results-secret-gate -n "$e2e_namespace" --timeout="$wait_timeout"

create_run() {
  run_name="$1"
  input_price="$2"
  output_price="$3"
  "${kubectl_cmd[@]}" apply -f - <<YAML
apiVersion: agentstorm.io/v1alpha1
kind: AgentTestRun
metadata:
  name: $run_name
  namespace: $e2e_namespace
spec:
  target:
    provider: fake
    pricing:
      inputUSDPerMillionTokens: "$input_price"
      outputUSDPerMillionTokens: "$output_price"
  workload:
    datasetRef:
      name: agentstorm-results-dataset
      key: cases.jsonl
    parallelism: 2
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
}

create_failure_run() {
  "${kubectl_cmd[@]}" apply -f - <<YAML
apiVersion: agentstorm.io/v1alpha1
kind: AgentTestRun
metadata:
  name: agentstorm-results-failure
  namespace: $e2e_namespace
spec:
  target:
    provider: fake
  workload:
    datasetRef:
      name: agentstorm-results-dataset
      key: failed.jsonl
    parallelism: 1
    concurrencyPerWorker: 1
    iterations: 1
    timeoutSeconds: 120
  evaluation:
    minSuccessRate: 0
    maxErrorRate: 1
    maxP95LatencyMs: 1000
  runner:
    image: "$worker_image"
    imagePullPolicy: IfNotPresent
YAML
}

create_redacted_run() {
  "${kubectl_cmd[@]}" apply -f - <<YAML
apiVersion: agentstorm.io/v1alpha1
kind: AgentTestRun
metadata:
  name: agentstorm-results-redacted
  namespace: $e2e_namespace
spec:
  target:
    provider: fake
  workload:
    datasetRef:
      name: agentstorm-results-dataset
      key: redacted.jsonl
    parallelism: 1
    concurrencyPerWorker: 1
    iterations: 1
    timeoutSeconds: 120
  evaluation:
    minSuccessRate: 1
    maxErrorRate: 0
    maxP95LatencyMs: 1000
  telemetry:
    contentMode: redacted
    redaction:
      patterns: ['customer-[0-9]+@example[.]com']
      metadataKeys: [tenant]
  runner:
    image: "$worker_image"
    imagePullPolicy: IfNotPresent
YAML
}

create_cancel_run() {
  "${kubectl_cmd[@]}" apply -f - <<YAML
apiVersion: agentstorm.io/v1alpha1
kind: AgentTestRun
metadata:
  name: agentstorm-results-cancel
  namespace: $e2e_namespace
spec:
  target:
    provider: fake
  workload:
    datasetRef:
      name: agentstorm-results-dataset
      key: cases.jsonl
    parallelism: 1
    concurrencyPerWorker: 1
    iterations: 1
    timeoutSeconds: 120
  reliability:
    seed: 42
    scenarioRef:
      name: agentstorm-results-cancel-scenario
      key: scenario.json
    retry:
      maxAttempts: 1
  evaluation:
    minSuccessRate: 1
    maxErrorRate: 0
  runner:
    image: "$worker_image"
    imagePullPolicy: IfNotPresent
YAML
}

create_reliability_fault_run() {
  "${kubectl_cmd[@]}" apply -f - <<YAML
apiVersion: agentstorm.io/v1alpha1
kind: AgentTestRun
metadata:
  name: agentstorm-results-reliability-faults
  namespace: $e2e_namespace
spec:
  target:
    provider: fake
    pricing:
      inputUSDPerMillionTokens: "2"
      outputUSDPerMillionTokens: "4"
  workload:
    datasetRef:
      name: agentstorm-results-dataset
      key: reliability.jsonl
    parallelism: 2
    concurrencyPerWorker: 2
    iterations: 1
    timeoutSeconds: 120
  reliability:
    seed: 42
    scenarioRef:
      name: agentstorm-results-reliability-scenarios
      key: faults.json
    retry:
      maxAttempts: 1
  evaluation:
    minSuccessRate: 0
    maxErrorRate: 1
  runner:
    image: "$worker_image"
    imagePullPolicy: IfNotPresent
YAML
}

create_retry_run() {
  run_name="$1"
  allow_ambiguous="$2"
  "${kubectl_cmd[@]}" apply -f - <<YAML
apiVersion: agentstorm.io/v1alpha1
kind: AgentTestRun
metadata:
  name: $run_name
  namespace: $e2e_namespace
spec:
  target:
    provider: fake
    pricing:
      inputUSDPerMillionTokens: "2"
      outputUSDPerMillionTokens: "4"
  workload:
    datasetRef:
      name: agentstorm-results-dataset
      key: retry.jsonl
    parallelism: 1
    concurrencyPerWorker: 1
    iterations: 1
    timeoutSeconds: 120
  reliability:
    seed: 42
    scenarioRef:
      name: agentstorm-results-reliability-scenarios
      key: retry.json
    retry:
      maxAttempts: 2
      initialBackoffMs: 1
      maxBackoffMs: 1
      maxCumulativeBackoffMs: 10
      jitterRatio: 0
      allowAmbiguousRetries: $allow_ambiguous
  evaluation:
    minSuccessRate: 0
    maxErrorRate: 1
  runner:
    image: "$worker_image"
    imagePullPolicy: IfNotPresent
YAML
}

create_deterministic_run() {
  run_name="$1"
  parallelism="$2"
  concurrency="$3"
  "${kubectl_cmd[@]}" apply -f - <<YAML
apiVersion: agentstorm.io/v1alpha1
kind: AgentTestRun
metadata:
  name: $run_name
  namespace: $e2e_namespace
spec:
  target:
    provider: fake
  workload:
    datasetRef:
      name: agentstorm-results-dataset
      key: deterministic.jsonl
    parallelism: $parallelism
    concurrencyPerWorker: $concurrency
    iterations: 1
    timeoutSeconds: 120
  reliability:
    seed: 777
    scenarioRef:
      name: agentstorm-results-reliability-scenarios
      key: deterministic.json
    retry:
      maxAttempts: 1
  evaluation:
    minSuccessRate: 1
    maxErrorRate: 0
  runner:
    image: "$worker_image"
    imagePullPolicy: IfNotPresent
YAML
}

create_circuit_run() {
  "${kubectl_cmd[@]}" apply -f - <<YAML
apiVersion: agentstorm.io/v1alpha1
kind: AgentTestRun
metadata:
  name: agentstorm-results-circuit
  namespace: $e2e_namespace
spec:
  target:
    provider: fake
  workload:
    datasetRef:
      name: agentstorm-results-dataset
      key: circuit.jsonl
    parallelism: 1
    concurrencyPerWorker: 1
    iterations: 100
    timeoutSeconds: 120
  reliability:
    seed: 42
    scenarioRef:
      name: agentstorm-results-reliability-scenarios
      key: circuit.json
    retry:
      maxAttempts: 1
    circuitBreaker:
      failureThreshold: 1
      openDurationMs: 30000
  evaluation:
    minSuccessRate: 0
    maxErrorRate: 1
  runner:
    image: "$worker_image"
    imagePullPolicy: IfNotPresent
YAML
}

create_run agentstorm-results-baseline 2.5 10
create_run agentstorm-results-candidate 5 20
create_failure_run
if [[ "$telemetry_e2e" == "true" ]]; then
  create_redacted_run
fi
if [[ "$reliability_e2e" == "true" ]]; then
  create_reliability_fault_run
  create_retry_run agentstorm-results-retry-default false
  create_retry_run agentstorm-results-retry-ambiguous true
  create_deterministic_run agentstorm-results-deterministic-a 1 1
  create_deterministic_run agentstorm-results-deterministic-b 2 4
  create_circuit_run
  create_cancel_run
  "${kubectl_cmd[@]}" wait --for=create job/agentstorm-results-cancel-worker -n "$e2e_namespace" --timeout="$wait_timeout"
  "${kubectl_cmd[@]}" patch agenttestrun agentstorm-results-cancel -n "$e2e_namespace" --type=merge -p '{"spec":{"cancel":true}}'
  "${kubectl_cmd[@]}" wait --for=jsonpath='{.status.phase}'=Cancelled agenttestrun/agentstorm-results-cancel -n "$e2e_namespace" --timeout="$wait_timeout"
fi
"${kubectl_cmd[@]}" wait --for=jsonpath='{.status.phase}'=Succeeded agenttestrun/agentstorm-results-baseline -n "$e2e_namespace" --timeout="$wait_timeout"
"${kubectl_cmd[@]}" wait --for=jsonpath='{.status.phase}'=Succeeded agenttestrun/agentstorm-results-candidate -n "$e2e_namespace" --timeout="$wait_timeout"
"${kubectl_cmd[@]}" wait --for=jsonpath='{.status.phase}'=Succeeded agenttestrun/agentstorm-results-failure -n "$e2e_namespace" --timeout="$wait_timeout"
if [[ "$telemetry_e2e" == "true" ]]; then
  "${kubectl_cmd[@]}" wait --for=jsonpath='{.status.phase}'=Succeeded agenttestrun/agentstorm-results-redacted -n "$e2e_namespace" --timeout="$wait_timeout"
fi
if [[ "$reliability_e2e" == "true" ]]; then
  for run_name in \
    agentstorm-results-reliability-faults \
    agentstorm-results-retry-default \
    agentstorm-results-retry-ambiguous \
    agentstorm-results-deterministic-a \
    agentstorm-results-deterministic-b \
    agentstorm-results-circuit; do
    "${kubectl_cmd[@]}" wait --for=jsonpath='{.status.phase}'=Succeeded \
      "agenttestrun/$run_name" -n "$e2e_namespace" --timeout="$wait_timeout"
  done
fi

baseline_id="$("${kubectl_cmd[@]}" get agenttestrun agentstorm-results-baseline -n "$e2e_namespace" -o jsonpath='{.metadata.uid}')"
candidate_id="$("${kubectl_cmd[@]}" get agenttestrun agentstorm-results-candidate -n "$e2e_namespace" -o jsonpath='{.metadata.uid}')"
failure_id="$("${kubectl_cmd[@]}" get agenttestrun agentstorm-results-failure -n "$e2e_namespace" -o jsonpath='{.metadata.uid}')"
cancel_id=""
faults_id=""
retry_default_id=""
retry_ambiguous_id=""
deterministic_a_id=""
deterministic_b_id=""
circuit_id=""
if [[ "$reliability_e2e" == "true" ]]; then
  cancel_id="$("${kubectl_cmd[@]}" get agenttestrun agentstorm-results-cancel -n "$e2e_namespace" -o jsonpath='{.metadata.uid}')"
  faults_id="$("${kubectl_cmd[@]}" get agenttestrun agentstorm-results-reliability-faults -n "$e2e_namespace" -o jsonpath='{.metadata.uid}')"
  retry_default_id="$("${kubectl_cmd[@]}" get agenttestrun agentstorm-results-retry-default -n "$e2e_namespace" -o jsonpath='{.metadata.uid}')"
  retry_ambiguous_id="$("${kubectl_cmd[@]}" get agenttestrun agentstorm-results-retry-ambiguous -n "$e2e_namespace" -o jsonpath='{.metadata.uid}')"
  deterministic_a_id="$("${kubectl_cmd[@]}" get agenttestrun agentstorm-results-deterministic-a -n "$e2e_namespace" -o jsonpath='{.metadata.uid}')"
  deterministic_b_id="$("${kubectl_cmd[@]}" get agenttestrun agentstorm-results-deterministic-b -n "$e2e_namespace" -o jsonpath='{.metadata.uid}')"
  circuit_id="$("${kubectl_cmd[@]}" get agenttestrun agentstorm-results-circuit -n "$e2e_namespace" -o jsonpath='{.metadata.uid}')"
fi
redacted_id=""
if [[ "$telemetry_e2e" == "true" ]]; then
  redacted_id="$("${kubectl_cmd[@]}" get agenttestrun agentstorm-results-redacted -n "$e2e_namespace" -o jsonpath='{.metadata.uid}')"
fi
if [[ -z "$baseline_id" || -z "$candidate_id" || -z "$failure_id" || "$baseline_id" == "$candidate_id" ]]; then
  echo "invalid durable run identifiers" >&2
  exit 1
fi
if [[ "$reliability_e2e" == "true" ]]; then
  for run_id in "$cancel_id" "$faults_id" "$retry_default_id" "$retry_ambiguous_id" \
    "$deterministic_a_id" "$deterministic_b_id" "$circuit_id"; do
    if [[ -z "$run_id" ]]; then
      echo "invalid durable reliability run identifier" >&2
      exit 1
    fi
  done
fi

worker_config="$("${kubectl_cmd[@]}" get configmap agentstorm-results-baseline-config -n "$e2e_namespace" -o jsonpath='{.data.run\.json}')"
case "$worker_config" in
  *agentstorm-result-auth*|*write-token*|*"$write_token"*)
    echo "generated worker ConfigMap contains Result API Secret material" >&2
    exit 1
    ;;
esac

start_port_forward agentstorm-result-api "$local_port" 8080 result-api
base_url="http://127.0.0.1:$local_port"
for attempt in {1..30}; do
  if curl --fail --silent "$base_url/readyz" >/dev/null; then
    break
  fi
  if [[ "$attempt" == "30" ]]; then
    sed -n '1,80p' "$test_dir/result-api-port-forward.log" >&2
    exit 1
  fi
  sleep 1
done

curl --fail --silent --header "Authorization: Bearer $read_token" \
  "$base_url/v1/runs/$baseline_id" >"$test_dir/baseline.json"
curl --fail --silent --header "Authorization: Bearer $read_token" \
  "$base_url/v1/runs/$candidate_id" >"$test_dir/candidate.json"
curl --fail --silent --header "Authorization: Bearer $read_token" \
  "$base_url/v1/runs/$failure_id" >"$test_dir/failure.json"
if [[ "$reliability_e2e" == "true" ]]; then
  curl --fail --silent --header "Authorization: Bearer $read_token" \
    "$base_url/v1/runs/$cancel_id" >"$test_dir/cancel.json"
  for item in \
    "faults:$faults_id" \
    "retry-default:$retry_default_id" \
    "retry-ambiguous:$retry_ambiguous_id" \
    "deterministic-a:$deterministic_a_id" \
    "deterministic-b:$deterministic_b_id" \
    "circuit:$circuit_id"; do
    name="${item%%:*}"
    run_id="${item#*:}"
    curl --fail --silent --header "Authorization: Bearer $read_token" \
      "$base_url/v1/runs/$run_id" >"$test_dir/$name.json"
    curl --fail --silent --header "Authorization: Bearer $read_token" \
      "$base_url/v1/runs/$run_id/cases?limit=500" >"$test_dir/$name-cases.json"
  done
fi
curl --fail --silent --header "Authorization: Bearer $read_token" \
  "$base_url/v1/runs/$baseline_id/cases?limit=10" >"$test_dir/baseline-cases.json"
curl --fail --silent --header "Authorization: Bearer $read_token" \
  "$base_url/v1/runs/$failure_id/cases?failed=true&limit=10" >"$test_dir/failure-cases.json"
curl --fail --silent --header "Authorization: Bearer $read_token" \
  "$base_url/v1/comparisons?baseline=$baseline_id&candidate=$candidate_id" >"$test_dir/comparison.json"
if [[ "$telemetry_e2e" == "true" ]]; then
  curl --fail --silent "$base_url/metrics" >"$test_dir/metrics.txt"
fi

python3 - "$test_dir/baseline.json" "$test_dir/candidate.json" "$test_dir/failure.json" "$test_dir/cancel.json" "$test_dir/baseline-cases.json" "$test_dir/failure-cases.json" "$test_dir/comparison.json" "$baseline_id" "$candidate_id" "$failure_id" "$cancel_id" "$expect_cost_accounting" "$reliability_e2e" <<'PY'
import json
import sys

baseline = json.load(open(sys.argv[1], encoding="utf-8"))
candidate = json.load(open(sys.argv[2], encoding="utf-8"))
failure = json.load(open(sys.argv[3], encoding="utf-8"))
cases = json.load(open(sys.argv[5], encoding="utf-8"))
failure_cases = json.load(open(sys.argv[6], encoding="utf-8"))
comparison = json.load(open(sys.argv[7], encoding="utf-8"))
baseline_id = sys.argv[8]
candidate_id = sys.argv[9]
failure_id = sys.argv[10]
cancel_id = sys.argv[11]
expect_cost_accounting = sys.argv[12] == "true"
reliability_e2e = sys.argv[13] == "true"
cancel = json.load(open(sys.argv[4], encoding="utf-8")) if reliability_e2e else None

for run, run_id in ((baseline, baseline_id), (candidate, candidate_id)):
    assert run["id"] == run_id, run
    assert run["status"] == "complete", run
    assert run["received_shards"] == 2, run
    assert run["registration"]["expected_shards"] == 2, run
    assert run["summary"]["total"] == 2, run
    assert run["summary"]["succeeded"] == 2, run
    assert run["summary"]["failed"] == 0, run
if expect_cost_accounting:
    assert baseline["summary"]["cost_usd"] == "0.000195000000", baseline
    assert candidate["summary"]["cost_usd"] == "0.000390000000", candidate

assert failure["id"] == failure_id, failure
assert failure["status"] == "complete", failure
assert failure["summary"]["total"] == 1, failure
assert failure["summary"]["succeeded"] == 0, failure
assert failure["summary"]["failed"] == 1, failure

if reliability_e2e:
    assert cancel["id"] == cancel_id, cancel
    assert cancel["status"] == "cancelled", cancel
    assert cancel["partial"] is True, cancel
    assert cancel["terminal_reason"]["reason_code"] == "cancellation_requested", cancel

assert len(cases["cases"]) == 2, cases
for case in cases["cases"]:
    assert "output" not in case, case
    assert "error" not in case, case
    if expect_cost_accounting:
        assert case["cost_usd"] is not None, case

assert len(failure_cases["cases"]) == 1, failure_cases
assert failure_cases["cases"][0]["failure_kind"] == "assertion", failure_cases
assert "output" not in failure_cases["cases"][0], failure_cases
assert "error" not in failure_cases["cases"][0], failure_cases

assert comparison["baseline_id"] == baseline_id, comparison
assert comparison["candidate_id"] == candidate_id, comparison
expected_deltas = {
    "success_rate", "failure_rate", "p50_ms", "p50_percent", "p95_ms",
    "p95_percent", "p99_ms", "p99_percent", "input_tokens", "output_tokens",
}
if reliability_e2e:
    expected_deltas |= {
        "quality_failures", "quality_failure_rate", "infrastructure_failures",
        "infrastructure_failure_rate", "attempt_count", "retry_count", "retried_cases",
        "retry_successes", "retry_success_rate", "injected_faults", "circuit_rejections",
    }
assert expected_deltas <= comparison["delta"].keys(), comparison
if expect_cost_accounting:
    assert comparison["delta"]["cost_usd"] == "0.000195000000", comparison
    assert comparison["delta"]["cost_percent"] == 100, comparison
PY

if [[ "$reliability_e2e" == "true" ]]; then
  python3 - \
    "$test_dir/faults.json" "$test_dir/faults-cases.json" \
    "$test_dir/retry-default.json" "$test_dir/retry-default-cases.json" \
    "$test_dir/retry-ambiguous.json" "$test_dir/retry-ambiguous-cases.json" \
    "$test_dir/deterministic-a.json" "$test_dir/deterministic-a-cases.json" \
    "$test_dir/deterministic-b.json" "$test_dir/deterministic-b-cases.json" \
    "$test_dir/circuit.json" "$test_dir/circuit-cases.json" <<'PY'
import json
import sys


def load(index):
    return json.load(open(sys.argv[index], encoding="utf-8"))


def by_case(page):
    return {item["case_id"]: item for item in page["cases"]}


faults, fault_page = load(1), load(2)
retry_default, retry_default_page = load(3), load(4)
retry_ambiguous, retry_ambiguous_page = load(5), load(6)
deterministic_a, deterministic_a_page = load(7), load(8)
deterministic_b, deterministic_b_page = load(9), load(10)
circuit, circuit_page = load(11), load(12)

for run in (faults, retry_default, retry_ambiguous, deterministic_a, deterministic_b, circuit):
    assert run["status"] == "complete", run
    assert run["partial"] is False, run
    scenario = run["registration"]["reliability"]["scenario"]
    assert scenario["digest"].startswith("sha256:") and len(scenario["digest"]) == 71, scenario
    assert scenario["source"]["name"] == "agentstorm-results-reliability-scenarios", scenario

assert faults["summary"]["total"] == 7, faults
assert faults["summary"]["succeeded"] == 1, faults
assert faults["summary"]["quality_failures"] == 1, faults
assert faults["summary"]["infrastructure_failures"] == 5, faults
assert faults["summary"]["attempt_count"] == 7, faults
assert faults["summary"]["retry_count"] == 0, faults
assert faults["summary"]["injected_faults"] == 6, faults
assert faults["summary"]["usage_complete"] is False, faults
assert faults["summary"]["cost_usd"] is None, faults
fault_cases = by_case(fault_page)
expected_faults = {
    "fault-latency": (True, "", "", "latency"),
    "fault-timeout": (False, "provider", "timeout", "timeout"),
    "fault-http": (False, "provider", "http_5xx", "http_error"),
    "fault-malformed": (False, "provider", "malformed_response", "malformed_response"),
    "fault-rate-limit": (False, "provider", "rate_limited", "rate_limit"),
    "fault-tool": (False, "tool", "injected_tool_error", "tool_error"),
    "fault-quality": (False, "evaluation", "assertion_failed", ""),
}
assert set(fault_cases) == set(expected_faults), fault_page
for case_id, expected in expected_faults.items():
    case = fault_cases[case_id]
    actual = (
        case["success"],
        case.get("failure_category", ""),
        case.get("error_code", ""),
        case["attempts"][0].get("injected_fault", ""),
    )
    assert actual == expected, case
malformed = fault_cases["fault-malformed"]
assert (malformed["input_tokens"], malformed["output_tokens"]) == (11, 7), malformed
assert malformed["cost_usd"] == "0.000050000000", malformed

default_cases = by_case(retry_default_page)
assert retry_default["summary"]["total"] == 4, retry_default
assert retry_default["summary"]["succeeded"] == 1, retry_default
assert retry_default["summary"]["infrastructure_failures"] == 3, retry_default
assert retry_default["summary"]["attempt_count"] == 5, retry_default
assert retry_default["summary"]["retry_count"] == 1, retry_default
assert retry_default["summary"]["retried_cases"] == 1, retry_default
assert retry_default["summary"]["retry_successes"] == 1, retry_default
assert retry_default["summary"]["retry_success_rate"] == 1, retry_default
assert len(default_cases["retry-rate-limit"]["attempts"]) == 2, default_cases
assert default_cases["retry-rate-limit"]["attempts"][0]["retry_decision"] == "retry_safe", default_cases
for case_id in ("retry-timeout", "retry-http", "retry-malformed"):
    case = default_cases[case_id]
    assert len(case["attempts"]) == 1, case
    assert case["attempts"][0]["retry_decision"] == "ambiguous_blocked", case

ambiguous_cases = by_case(retry_ambiguous_page)
assert retry_ambiguous["summary"]["total"] == 4, retry_ambiguous
assert retry_ambiguous["summary"]["succeeded"] == 4, retry_ambiguous
assert retry_ambiguous["summary"]["attempt_count"] == 8, retry_ambiguous
assert retry_ambiguous["summary"]["retry_count"] == 4, retry_ambiguous
assert retry_ambiguous["summary"]["retried_cases"] == 4, retry_ambiguous
assert retry_ambiguous["summary"]["retry_successes"] == 4, retry_ambiguous
assert retry_ambiguous["summary"]["retry_success_rate"] == 1, retry_ambiguous
assert (retry_ambiguous["summary"]["input_tokens"], retry_ambiguous["summary"]["output_tokens"]) == (55, 35), retry_ambiguous
assert retry_ambiguous["summary"]["cost_usd"] == "0.000250000000", retry_ambiguous
for case in ambiguous_cases.values():
    assert len(case["attempts"]) == 2, case
assert ambiguous_cases["retry-malformed"]["cost_usd"] == "0.000100000000", ambiguous_cases


def injection_map(page):
    return {
        (item["case_id"], item["iteration"]): item["attempts"][0].get("injected_fault", "")
        for item in page["cases"]
    }


selected_a = injection_map(deterministic_a_page)
selected_b = injection_map(deterministic_b_page)
assert selected_a == selected_b, (selected_a, selected_b)
assert 0 < sum(bool(value) for value in selected_a.values()) < len(selected_a), selected_a
assert deterministic_a["registration"]["reliability"]["scenario"]["digest"] == deterministic_b["registration"]["reliability"]["scenario"]["digest"], (deterministic_a, deterministic_b)

assert circuit["summary"]["total"] == 400, circuit
assert circuit["summary"]["attempt_count"] == 400, circuit
assert circuit["summary"]["injected_faults"] == 1, circuit
assert circuit["summary"]["circuit_rejections"] > 0, circuit
assert len(circuit_page["cases"]) == 400, circuit_page
circuit_events = {
    event
    for case in circuit_page["cases"]
    for attempt in case["attempts"]
    for event in attempt.get("circuit_events", [])
}
assert {"open", "reject"} <= circuit_events, circuit_events
PY
fi

if [[ "$telemetry_e2e" == "true" ]]; then
  python3 - "$test_dir/metrics.txt" "$baseline_id" "$candidate_id" "$write_token" "$read_token" <<'PY'
import pathlib
import sys

metrics = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
for name in (
    "agentstorm_result_api_http_requests_total",
    "agentstorm_result_api_http_request_duration_seconds",
    "agentstorm_result_api_run_registrations_total",
    "agentstorm_result_api_shard_uploads_total",
    "agentstorm_result_api_cases_total",
    "agentstorm_result_api_tokens_total",
    "agentstorm_result_api_cost_usd_total",
    "agentstorm_result_api_case_failures_total",
    "agentstorm_result_api_attempts_total",
    "agentstorm_result_api_retry_decisions_total",
    "agentstorm_result_api_retries_total",
    "agentstorm_result_api_injected_faults_total",
    "agentstorm_result_api_circuit_events_total",
):
    assert name in metrics, name
for forbidden in sys.argv[2:]:
    assert forbidden not in metrics, forbidden
assert "case-a" not in metrics
assert "case-b" not in metrics
PY

  collector_logs=""
  for attempt in {1..30}; do
    collector_logs="$("${kubectl_cmd[@]}" logs -n "$e2e_namespace" deployment/agentstorm-otel-collector)"
    if [[ "$collector_logs" == *agentstorm.run* && "$collector_logs" == *gen_ai.invoke_agent* && "$collector_logs" == *'[REDACTED]'* && "$collector_logs" == *team-blue* ]]; then
      break
    fi
    if [[ "$attempt" == "30" ]]; then
      echo "timed out waiting for exported worker spans" >&2
      exit 1
    fi
    sleep 1
  done
  for span_name in \
    agentstorm.run \
    agentstorm.case \
    gen_ai.invoke_agent \
    gen_ai.execute_tool \
    agentstorm.handoff \
    agentstorm.evaluate; do
    if [[ "$collector_logs" != *"$span_name"* ]]; then
      echo "collector logs do not contain span $span_name" >&2
      exit 1
    fi
  done
  for sensitive_value in alpha beta gamma never-redacted-value; do
    if [[ "$collector_logs" == *"$sensitive_value"* ]]; then
      echo "collector logs contain default-redacted dataset content" >&2
      exit 1
    fi
  done
  for sensitive_value in customer-42@example.com internal-canary metadata-sensitive-canary; do
    if [[ "$collector_logs" == *"$sensitive_value"* ]]; then
      echo "collector logs contain unsanitized redacted-mode content" >&2
      exit 1
    fi
  done

  start_port_forward agentstorm-tempo "$tempo_local_port" 3200 tempo
  start_port_forward agentstorm-prometheus "$prometheus_local_port" 9090 prometheus
  start_port_forward agentstorm-grafana "$grafana_local_port" 3000 grafana
  tempo_base_url="http://127.0.0.1:$tempo_local_port"
  prometheus_base_url="http://127.0.0.1:$prometheus_local_port"
  grafana_base_url="http://127.0.0.1:$grafana_local_port"
  for endpoint in \
    "$tempo_base_url/ready" \
    "$prometheus_base_url/-/ready" \
    "$grafana_base_url/api/health"; do
    for attempt in {1..30}; do
      if curl --fail --silent "$endpoint" >/dev/null; then
        break
      fi
      if [[ "$attempt" == "30" ]]; then
        echo "timed out waiting for observability endpoint $endpoint" >&2
        exit 1
      fi
      sleep 1
    done
  done

  for attempt in {1..30}; do
    curl --fail --silent --get \
      --data-urlencode 'query=agentstorm_result_api_cases_total' \
      "$prometheus_base_url/api/v1/query" >"$test_dir/prometheus-query.json"
    if python3 - "$test_dir/prometheus-query.json" <<'PY'
import json
import sys

payload = json.load(open(sys.argv[1], encoding="utf-8"))
raise SystemExit(0 if payload.get("status") == "success" and payload.get("data", {}).get("result") else 1)
PY
    then
      break
    fi
    if [[ "$attempt" == "30" ]]; then
      echo "Prometheus did not scrape Result API metrics" >&2
      exit 1
    fi
    sleep 1
  done
  curl --fail --silent --get --data-urlencode 'type=record' \
    "$prometheus_base_url/api/v1/rules" >"$test_dir/prometheus-rules.json"

  failed_trace_query="{ span.agentstorm.run.id = \"$failure_id\" && name = \"agentstorm.case\" && span.agentstorm.case.success = false }"
  for attempt in {1..60}; do
    curl --fail --silent --get --data-urlencode "q=$failed_trace_query" \
      --data-urlencode 'limit=20' "$tempo_base_url/api/search" >"$test_dir/tempo-search.json"
    if python3 - "$test_dir/tempo-search.json" <<'PY'
import json
import sys

payload = json.load(open(sys.argv[1], encoding="utf-8"))
raise SystemExit(0 if payload.get("traces") else 1)
PY
    then
      break
    fi
    if [[ "$attempt" == "60" ]]; then
      echo "Tempo did not return the failed-case trace" >&2
      exit 1
    fi
    sleep 1
  done
  failed_trace_id="$(python3 - "$test_dir/tempo-search.json" <<'PY'
import json
import sys

payload = json.load(open(sys.argv[1], encoding="utf-8"))
print(payload["traces"][0]["traceID"])
PY
)"
  curl --fail --silent "$tempo_base_url/api/traces/$failed_trace_id" >"$test_dir/tempo-trace.json"
  redacted_trace_query="{ span.agentstorm.run.id = \"$redacted_id\" && name = \"agentstorm.case\" }"
  for attempt in {1..60}; do
    curl --fail --silent --get --data-urlencode "q=$redacted_trace_query" \
      --data-urlencode 'limit=20' "$tempo_base_url/api/search" >"$test_dir/tempo-redacted-search.json"
    if python3 - "$test_dir/tempo-redacted-search.json" <<'PY'
import json
import sys

payload = json.load(open(sys.argv[1], encoding="utf-8"))
raise SystemExit(0 if payload.get("traces") else 1)
PY
    then
      break
    fi
    if [[ "$attempt" == "60" ]]; then
      echo "Tempo did not return the redacted-content trace" >&2
      exit 1
    fi
    sleep 1
  done
  redacted_trace_id="$(python3 - "$test_dir/tempo-redacted-search.json" <<'PY'
import json
import sys

payload = json.load(open(sys.argv[1], encoding="utf-8"))
print(payload["traces"][0]["traceID"])
PY
)"
  curl --fail --silent "$tempo_base_url/api/traces/$redacted_trace_id" >"$test_dir/tempo-redacted-trace.json"
  curl --fail --silent "$grafana_base_url/api/dashboards/uid/agentstorm-observability" >"$test_dir/grafana-dashboard.json"

  python3 - \
    "$test_dir/prometheus-query.json" \
    "$test_dir/prometheus-rules.json" \
    "$test_dir/tempo-trace.json" \
    "$test_dir/tempo-redacted-trace.json" \
    "$test_dir/grafana-dashboard.json" <<'PY'
import json
import pathlib
import sys

prometheus = json.load(open(sys.argv[1], encoding="utf-8"))
assert prometheus["status"] == "success", prometheus
assert prometheus["data"]["result"], prometheus

rules = json.load(open(sys.argv[2], encoding="utf-8"))
assert rules["status"] == "success", rules
recording_rules = {
    rule["name"]: rule["health"]
    for group in rules["data"]["groups"]
    for rule in group["rules"]
}
expected_rules = {
    "agentstorm:result_api_http_requests:rate5m",
    "agentstorm:result_api_http_errors:ratio5m",
    "agentstorm:result_api_http_request_duration:p95_5m",
    "agentstorm:result_api_cost_usd:rate5m",
    "agentstorm:result_api_case_failures:rate5m",
    "agentstorm:result_api_retry_success:ratio5m",
    "agentstorm:result_api_injected_faults:rate5m",
    "agentstorm:result_api_circuit_events:rate5m",
    "agentstorm:result_api_queue_claims:rate5m",
    "agentstorm:result_api_scheduler_permits:rate5m",
    "agentstorm:result_api_scheduler_permits_limited:ratio5m",
}
assert expected_rules <= recording_rules.keys(), recording_rules
assert all(recording_rules[name] == "ok" for name in expected_rules), recording_rules

trace = pathlib.Path(sys.argv[3]).read_text(encoding="utf-8")
for forbidden in ("alpha", "beta", "gamma", "never-redacted-value"):
    assert forbidden not in trace, forbidden

redacted_trace = pathlib.Path(sys.argv[4]).read_text(encoding="utf-8")
assert "[REDACTED]" in redacted_trace, redacted_trace
assert "team-blue" in redacted_trace, redacted_trace
for forbidden in (
    "customer-42@example.com",
    "internal-canary",
    "metadata-sensitive-canary",
):
    assert forbidden not in redacted_trace, forbidden

grafana = json.load(open(sys.argv[5], encoding="utf-8"))["dashboard"]
assert grafana["uid"] == "agentstorm-observability", grafana
assert {item["name"] for item in grafana["templating"]["list"]} == {"run_id", "trace_id"}, grafana
panels = {panel["title"]: panel for panel in grafana["panels"]}
for title in (
    "Failed cases for Run ID",
    "Provider errors for Run ID",
    "Selected trace",
    "Result API P95 latency",
    "Estimated USD cost",
    "Failure categories",
    "Retry effectiveness",
    "Injected vs observed faults",
    "Circuit events",
    "Durable queue claims",
    "Distributed Provider permits",
):
    assert title in panels, title
assert "agentstorm.case.success = false" in panels["Failed cases for Run ID"]["targets"][0]["query"]
assert "status = error" in panels["Provider errors for Run ID"]["targets"][0]["query"]
PY

  stop_port_forwards
  "${kubectl_cmd[@]}" rollout restart deployment/agentstorm-tempo deployment/agentstorm-prometheus -n "$e2e_namespace" >/dev/null
  "${kubectl_cmd[@]}" rollout status deployment/agentstorm-tempo -n "$e2e_namespace" --timeout="$wait_timeout"
  "${kubectl_cmd[@]}" rollout status deployment/agentstorm-prometheus -n "$e2e_namespace" --timeout="$wait_timeout"
  start_port_forward agentstorm-tempo "$tempo_local_port" 3200 tempo-restarted
  start_port_forward agentstorm-prometheus "$prometheus_local_port" 9090 prometheus-restarted
  for attempt in {1..30}; do
    if curl --fail --silent "$tempo_base_url/api/traces/$failed_trace_id" >"$test_dir/tempo-trace-restarted.json" && \
      curl --fail --silent --get --data-urlencode 'query=agentstorm_result_api_cases_total' \
        "$prometheus_base_url/api/v1/query" >"$test_dir/prometheus-query-restarted.json"; then
      break
    fi
    if [[ "$attempt" == "30" ]]; then
      echo "persistent telemetry was unavailable after backend restart" >&2
      exit 1
    fi
    sleep 1
  done
  python3 - "$test_dir/tempo-trace-restarted.json" "$test_dir/prometheus-query-restarted.json" <<'PY'
import json
import sys

trace = json.load(open(sys.argv[1], encoding="utf-8"))
assert trace.get("batches"), trace
prometheus = json.load(open(sys.argv[2], encoding="utf-8"))
assert prometheus.get("status") == "success", prometheus
assert prometheus.get("data", {}).get("result"), prometheus
PY
fi

echo "AgentStorm result-stack E2E passed: context=$kube_context baseline=$baseline_id candidate=$candidate_id failure=$failure_id result_api_image=$result_api_image telemetry=$telemetry_e2e reliability=$reliability_e2e"
