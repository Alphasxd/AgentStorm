# Reproducible v0.6 demo recording for charmbracelet/vhs.
Output demo/agentstorm-v0.6.gif
Set Shell "bash"
Set FontSize 18
Set Width 1200
Set Height 760
Set TypingSpeed 20ms

Type "helm --kube-context kind-agentstorm upgrade --install agentstorm-demo oci://ghcr.io/alphasxd/charts/agentstorm --version 0.6.0 -n agentstorm-system --create-namespace --wait --timeout 5m --set nameOverride=agentstorm-controller --set controller.image.repository=ghcr.io/alphasxd/agentstorm-controller --set-string controller.image.digest=sha256:b37ae7edcbc5da61f5f1680f3aca5d5d6311c6a2b7745ebea088dde3279428ca --set controller.resultSink.enabled=true --set controller.resultSink.url=http://agentstorm-result-api.agentstorm-system.svc.cluster.local:8080 --set controller.resultSink.existingAuthSecret=agentstorm-result-auth --set controller.resultSink.distributedLimits=true --set keda.enabled=true --set keda.metricsApiUrl=http://agentstorm-result-api.agentstorm-system.svc.cluster.local:8080"
Enter
Sleep 3s
Type "kubectl --context kind-agentstorm -n agentstorm-system create configmap sre-demo-dataset --from-file=cases.jsonl=examples/datasets/sre-incidents.jsonl --dry-run=client -o yaml | kubectl --context kind-agentstorm apply -f -"
Enter
Sleep 2s
Type "kubectl --context kind-agentstorm apply -f demo/sre-demo.yaml && kubectl --context kind-agentstorm -n agentstorm-system get scaledjob,pods -l agentstorm.io/run=sre-demo"
Enter
Sleep 3s
Type "kubectl --context kind-agentstorm -n agentstorm-system wait --for=jsonpath='{.status.phase}'=Succeeded agenttestrun/sre-demo --timeout=300s"
Enter
Sleep 2s
Type `RUN_ID=$(kubectl --context kind-agentstorm -n agentstorm-system get agenttestrun/sre-demo -o jsonpath='{.metadata.uid}'); READ_TOKEN=$(kubectl --context kind-agentstorm -n agentstorm-system get secret agentstorm-result-auth -o jsonpath='{.data.read-token}' | base64 -d); curl -sH "Authorization: Bearer $READ_TOKEN" "http://127.0.0.1:18080/v1/runs/$RUN_ID" | jq '{status,summary:{total:.summary.total,succeeded:.summary.succeeded,model_calls:.summary.model_call_count,tool_calls:.summary.tool_call_count}}'`
Enter
Sleep 3s
Type `curl -sH "Authorization: Bearer $READ_TOKEN" "http://127.0.0.1:18080/v1/runs/$RUN_ID/cases?limit=1" | jq '.cases[0] | {case_id,tool_path,model_call_count,tool_call_count}'`
Enter
Sleep 3s
Type "kubectl --context kind-agentstorm -n agentstorm-system get jobs -l agentstorm.io/run=sre-demo -o json | jq '{active_workers: ([.items[].status.active // 0] | add // 0)}'"
Enter
Sleep 2s
