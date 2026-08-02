package controller

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	agentstormv1alpha1 "github.com/Alphasxd/AgentStorm/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReconcileCreatesIndexedWorkerJob(t *testing.T) {
	scheme := testScheme(t)
	run := testRun()
	run.Spec.Target.APIKeySecretRef = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "provider-credentials"},
		Key:                  "api-key",
	}
	dataset := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-dataset", Namespace: "default"},
		Data:       map[string]string{"cases.jsonl": `{"id":"1","input":"hello"}`},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "provider-credentials", Namespace: "default"},
		Data:       map[string][]byte{"api-key": []byte("test-only-value")},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&agentstormv1alpha1.AgentTestRun{}).WithObjects(run, dataset, secret).Build()
	reconciler := &AgentTestRunReconciler{Client: client, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	job := &batchv1.Job{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-worker"}, job); err != nil {
		t.Fatalf("get Job: %v", err)
	}
	if job.Spec.Parallelism == nil || *job.Spec.Parallelism != 2 {
		t.Fatalf("parallelism = %v, want 2", job.Spec.Parallelism)
	}
	if job.Spec.CompletionMode == nil || *job.Spec.CompletionMode != batchv1.IndexedCompletion {
		t.Fatalf("completion mode = %v, want Indexed", job.Spec.CompletionMode)
	}
	if len(job.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(job.Spec.Template.Spec.Containers))
	}
	assertEnv(t, job.Spec.Template.Spec.Containers[0].Env, "AGENTSTORM_SHARD_COUNT", "2")
	assertSecretEnv(t, job.Spec.Template.Spec.Containers[0].Env, "OPENAI_API_KEY", "provider-credentials", "api-key")
	assertWorkerSecurity(t, job.Spec.Template.Spec)

	config := &corev1.ConfigMap{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-config"}, config); err != nil {
		t.Fatalf("get generated ConfigMap: %v", err)
	}
	if config.Data[workerConfigKey] == "" {
		t.Fatal("generated worker config is empty")
	}
	if strings.Contains(config.Data[workerConfigKey], "provider-credentials") || strings.Contains(config.Data[workerConfigKey], "api-key") {
		t.Fatal("generated worker config must not contain Secret references")
	}
}

func TestReconcileWaitsForRequiredSecret(t *testing.T) {
	scheme := testScheme(t)
	run := testRun()
	run.Spec.Target.APIKeySecretRef = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "provider-credentials"},
		Key:                  "api-key",
	}
	dataset := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-dataset", Namespace: "default"},
		Data:       map[string]string{"cases.jsonl": `{"id":"1","input":"hello"}`},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&agentstormv1alpha1.AgentTestRun{}).WithObjects(run, dataset).Build()
	reconciler := &AgentTestRunReconciler{Client: client, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}}

	result, err := reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected a requeue while the provider Secret is missing")
	}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-worker"}, &batchv1.Job{}); err == nil {
		t.Fatal("worker Job should not exist before the provider Secret is available")
	}

	updated := &agentstormv1alpha1.AgentTestRun{}
	if err := client.Get(context.Background(), request.NamespacedName, updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	condition := meta.FindStatusCondition(updated.Status.Conditions, conditionCredentials)
	if updated.Status.Phase != agentstormv1alpha1.AgentTestRunPending || condition == nil || condition.Reason != "SecretMissing" {
		t.Fatalf("status = %#v, want Pending with SecretMissing condition", updated.Status)
	}
}

func TestReconcileConfiguresResultSinkWithoutSerializingSecrets(t *testing.T) {
	scheme := testScheme(t)
	run := testRun()
	run.Spec.Target.Pricing = &agentstormv1alpha1.AgentPricingSpec{
		InputUSDPerMillionTokens:  "2.5",
		OutputUSDPerMillionTokens: "10",
	}
	dataset := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-dataset", Namespace: "default"},
		Data:       map[string]string{"cases.jsonl": `{"id":"1","input":"hello"}`},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "agentstorm-result-auth", Namespace: "default"},
		Data:       map[string][]byte{"write-token": []byte("result-test-only-value")},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&agentstormv1alpha1.AgentTestRun{}).WithObjects(run, dataset, secret).Build()
	resultSink := ResultSinkConfig{
		URL:                  "http://agentstorm-result-api:8080/",
		WriteTokenSecretName: "agentstorm-result-auth",
		WriteTokenSecretKey:  "write-token",
		TimeoutSeconds:       30,
	}
	if err := resultSink.ValidateAndDefault(); err != nil {
		t.Fatalf("validate result sink: %v", err)
	}
	reconciler := &AgentTestRunReconciler{Client: client, Scheme: scheme, ResultSink: resultSink}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	job := &batchv1.Job{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-worker"}, job); err != nil {
		t.Fatalf("get Job: %v", err)
	}
	env := job.Spec.Template.Spec.Containers[0].Env
	assertEnv(t, env, "AGENTSTORM_RESULT_API_URL", "http://agentstorm-result-api:8080")
	assertEnv(t, env, "AGENTSTORM_INCLUDE_SENSITIVE_RESULTS", "false")
	assertEnv(t, env, "AGENTSTORM_RESULT_TIMEOUT_SECONDS", "30")
	assertSecretEnv(t, env, "AGENTSTORM_RESULT_WRITE_TOKEN", "agentstorm-result-auth", "write-token")

	configMap := &corev1.ConfigMap{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-config"}, configMap); err != nil {
		t.Fatalf("get worker config: %v", err)
	}
	workerConfig := map[string]any{}
	if err := json.Unmarshal([]byte(configMap.Data[workerConfigKey]), &workerConfig); err != nil {
		t.Fatalf("decode worker config: %v", err)
	}
	source := workerConfig["source"].(map[string]any)
	datasetConfig := workerConfig["dataset"].(map[string]any)
	targetConfig := workerConfig["target"].(map[string]any)
	pricingConfig := targetConfig["pricing"].(map[string]any)
	if source["namespace"] != "default" || source["name"] != "demo" ||
		datasetConfig["name"] != "demo-dataset" || datasetConfig["key"] != "cases.jsonl" ||
		pricingConfig["inputUSDPerMillionTokens"] != "2.5" ||
		pricingConfig["outputUSDPerMillionTokens"] != "10" {
		t.Fatalf("unexpected source/dataset config: %#v", workerConfig)
	}
	for _, forbidden := range []string{"result-test-only-value", "agentstorm-result-auth", "write-token"} {
		if strings.Contains(configMap.Data[workerConfigKey], forbidden) {
			t.Fatalf("generated worker ConfigMap contains result Secret material %q", forbidden)
		}
	}
}

func TestReconcileWaitsForResultSinkSecret(t *testing.T) {
	scheme := testScheme(t)
	run := testRun()
	dataset := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-dataset", Namespace: "default"},
		Data:       map[string]string{"cases.jsonl": `{"id":"1","input":"hello"}`},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&agentstormv1alpha1.AgentTestRun{}).WithObjects(run, dataset).Build()
	resultSink := ResultSinkConfig{
		URL:                  "http://agentstorm-result-api:8080",
		WriteTokenSecretName: "agentstorm-result-auth",
		WriteTokenSecretKey:  "write-token",
		TimeoutSeconds:       30,
	}
	reconciler := &AgentTestRunReconciler{Client: client, Scheme: scheme, ResultSink: resultSink}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}}

	result, err := reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected a requeue while the result sink Secret is missing")
	}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-worker"}, &batchv1.Job{}); err == nil {
		t.Fatal("worker Job should not exist before the result sink Secret is available")
	}
	updated := &agentstormv1alpha1.AgentTestRun{}
	if err := client.Get(context.Background(), request.NamespacedName, updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	condition := meta.FindStatusCondition(updated.Status.Conditions, conditionResultSink)
	if updated.Status.Phase != agentstormv1alpha1.AgentTestRunPending || condition == nil || condition.Reason != "SecretMissing" {
		t.Fatalf("status = %#v, want Pending with result sink SecretMissing", updated.Status)
	}
}

func TestResultSinkConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  ResultSinkConfig
		wantErr bool
	}{
		{name: "disabled", config: ResultSinkConfig{}},
		{name: "valid", config: ResultSinkConfig{URL: "https://results.example", WriteTokenSecretName: "result-auth", WriteTokenSecretKey: "write_token", TimeoutSeconds: 30}},
		{name: "credentials in URL", config: ResultSinkConfig{URL: "https://user:password@results.example", WriteTokenSecretName: "result-auth", WriteTokenSecretKey: "write-token"}, wantErr: true},
		{name: "invalid Secret name", config: ResultSinkConfig{URL: "https://results.example", WriteTokenSecretName: "INVALID", WriteTokenSecretKey: "write-token"}, wantErr: true},
		{name: "sensitive without sink", config: ResultSinkConfig{IncludeSensitive: true}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.ValidateAndDefault()
			if (err != nil) != test.wantErr {
				t.Fatalf("ValidateAndDefault() error = %v, wantErr=%v", err, test.wantErr)
			}
		})
	}
}

func TestReconcileConfiguresOptionalWorkerTelemetry(t *testing.T) {
	scheme := testScheme(t)
	run := testRun()
	run.Spec.Telemetry = agentstormv1alpha1.AgentTelemetrySpec{
		ContentMode: "redacted",
		Redaction: agentstormv1alpha1.AgentTelemetryRedactionSpec{
			Patterns:     []string{`customer-[0-9]+`},
			MetadataKeys: []string{"tenant"},
		},
	}
	dataset := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-dataset", Namespace: "default"},
		Data:       map[string]string{"cases.jsonl": `{"id":"1","input":"private prompt"}`},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&agentstormv1alpha1.AgentTestRun{}).WithObjects(run, dataset).Build()
	telemetry := TelemetryConfig{
		OTLPEndpoint:  "http://otel-collector:4318/",
		AllowRedacted: true,
	}
	if err := telemetry.ValidateAndDefault(); err != nil {
		t.Fatalf("validate telemetry: %v", err)
	}
	reconciler := &AgentTestRunReconciler{Client: client, Scheme: scheme, Telemetry: telemetry}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	job := &batchv1.Job{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-worker"}, job); err != nil {
		t.Fatalf("get Job: %v", err)
	}
	env := job.Spec.Template.Spec.Containers[0].Env
	assertEnv(t, env, "AGENTSTORM_OTEL_ENABLED", "true")
	assertEnv(t, env, "OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318")
	assertEnv(t, env, "OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	assertEnv(t, env, "OTEL_SERVICE_NAME", "agentstorm-worker")

	configMap := &corev1.ConfigMap{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-config"}, configMap); err != nil {
		t.Fatalf("get worker config: %v", err)
	}
	for _, forbidden := range []string{"private prompt", "otel-collector", "OTEL_EXPORTER_OTLP"} {
		if strings.Contains(configMap.Data[workerConfigKey], forbidden) {
			t.Fatalf("generated worker ConfigMap contains telemetry or dataset content %q", forbidden)
		}
	}
	if !strings.Contains(configMap.Data[workerConfigKey], `"contentMode": "redacted"`) ||
		!strings.Contains(configMap.Data[workerConfigKey], "customer-[0-9]+") ||
		!strings.Contains(configMap.Data[workerConfigKey], `"tenant"`) {
		t.Fatalf("generated worker ConfigMap is missing redaction policy: %s", configMap.Data[workerConfigKey])
	}
	updated := &agentstormv1alpha1.AgentTestRun{}
	if err := client.Get(context.Background(), request.NamespacedName, updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	condition := meta.FindStatusCondition(updated.Status.Conditions, conditionTelemetry)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "Available" {
		t.Fatalf("TelemetryReady condition = %#v, want true/Available", condition)
	}
}

func TestReconcileBlocksRedactedTelemetryWithoutBothControllerGates(t *testing.T) {
	tests := []struct {
		name       string
		telemetry  TelemetryConfig
		wantReason string
	}{
		{
			name:       "missing endpoint",
			telemetry:  TelemetryConfig{AllowRedacted: true},
			wantReason: "EndpointMissing",
		},
		{
			name:       "controller opt in missing",
			telemetry:  TelemetryConfig{OTLPEndpoint: "http://otel-collector:4318"},
			wantReason: "RedactedTelemetryDisabled",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := testScheme(t)
			run := testRun()
			run.Spec.Telemetry.ContentMode = "redacted"
			dataset := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "demo-dataset", Namespace: "default"},
				Data:       map[string]string{"cases.jsonl": `{"id":"1","input":"hello"}`},
			}
			client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&agentstormv1alpha1.AgentTestRun{}).WithObjects(run, dataset).Build()
			reconciler := &AgentTestRunReconciler{Client: client, Scheme: scheme, Telemetry: test.telemetry}
			request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}}

			result, err := reconciler.Reconcile(context.Background(), request)
			if err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if result.RequeueAfter == 0 {
				t.Fatal("expected a requeue while redacted telemetry is gated")
			}
			if err := client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-worker"}, &batchv1.Job{}); err == nil {
				t.Fatal("worker Job should not exist before redacted telemetry is ready")
			}
			updated := &agentstormv1alpha1.AgentTestRun{}
			if err := client.Get(context.Background(), request.NamespacedName, updated); err != nil {
				t.Fatalf("get run: %v", err)
			}
			condition := meta.FindStatusCondition(updated.Status.Conditions, conditionTelemetry)
			if updated.Status.Phase != agentstormv1alpha1.AgentTestRunPending || condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != test.wantReason {
				t.Fatalf("status = %#v, want Pending with %s", updated.Status, test.wantReason)
			}
		})
	}
}

func TestValidateAndDefaultRejectsInvalidRedactionBeforeJobCreation(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
	}{
		{name: "invalid regex", patterns: []string{"("}},
		{name: "too long", patterns: []string{strings.Repeat("x", 257)}},
		{name: "too many", patterns: make([]string, 21)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := testScheme(t)
			run := testRun()
			run.Spec.Telemetry = agentstormv1alpha1.AgentTelemetrySpec{
				ContentMode: "redacted",
				Redaction: agentstormv1alpha1.AgentTelemetryRedactionSpec{
					Patterns: test.patterns,
				},
			}
			dataset := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "demo-dataset", Namespace: "default"},
				Data:       map[string]string{"cases.jsonl": `{"id":"1","input":"hello"}`},
			}
			client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&agentstormv1alpha1.AgentTestRun{}).WithObjects(run, dataset).Build()
			reconciler := &AgentTestRunReconciler{
				Client: client,
				Scheme: scheme,
				Telemetry: TelemetryConfig{
					OTLPEndpoint:  "http://otel-collector:4318",
					AllowRedacted: true,
				},
			}
			request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}}
			if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			updated := &agentstormv1alpha1.AgentTestRun{}
			if err := client.Get(context.Background(), request.NamespacedName, updated); err != nil {
				t.Fatalf("get run: %v", err)
			}
			if updated.Status.Phase != agentstormv1alpha1.AgentTestRunFailed {
				t.Fatalf("phase = %q, want Failed before Job creation", updated.Status.Phase)
			}
			if err := client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-worker"}, &batchv1.Job{}); err == nil {
				t.Fatal("worker Job was created for invalid redaction policy")
			}
		})
	}
}

func TestTelemetryConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  TelemetryConfig
		wantErr bool
	}{
		{name: "disabled", config: TelemetryConfig{}},
		{name: "valid HTTP", config: TelemetryConfig{OTLPEndpoint: "http://collector:4318/"}},
		{name: "valid HTTPS path", config: TelemetryConfig{OTLPEndpoint: "https://telemetry.example/otlp"}},
		{name: "credentials", config: TelemetryConfig{OTLPEndpoint: "https://user:password@telemetry.example"}, wantErr: true},
		{name: "query", config: TelemetryConfig{OTLPEndpoint: "https://telemetry.example?token=value"}, wantErr: true},
		{name: "unsupported scheme", config: TelemetryConfig{OTLPEndpoint: "grpc://collector:4317"}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.ValidateAndDefault()
			if (err != nil) != test.wantErr {
				t.Fatalf("ValidateAndDefault() error = %v, wantErr=%v", err, test.wantErr)
			}
		})
	}
}

func TestReconcileAllowsMissingOptionalSecret(t *testing.T) {
	scheme := testScheme(t)
	run := testRun()
	run.Spec.Target.APIKeySecretRef = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "optional-credentials"},
		Key:                  "api-key",
		Optional:             ptr.To(true),
	}
	dataset := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-dataset", Namespace: "default"},
		Data:       map[string]string{"cases.jsonl": `{"id":"1","input":"hello"}`},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&agentstormv1alpha1.AgentTestRun{}).WithObjects(run, dataset).Build()
	reconciler := &AgentTestRunReconciler{Client: client, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-worker"}, &batchv1.Job{}); err != nil {
		t.Fatalf("optional Secret should not prevent Job creation: %v", err)
	}
}

func TestCredentialsReadyUsesAPIReader(t *testing.T) {
	scheme := testScheme(t)
	run := testRun()
	run.Spec.Target.APIKeySecretRef = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "provider-credentials"},
		Key:                  "api-key",
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "provider-credentials", Namespace: "default"},
		Data:       map[string][]byte{"api-key": []byte("test-only-value")},
	}
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	apiReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	reconciler := &AgentTestRunReconciler{Client: cachedClient, APIReader: apiReader, Scheme: scheme}

	ready, reason, _, err := reconciler.credentialsReady(context.Background(), run)
	if err != nil {
		t.Fatalf("credentialsReady: %v", err)
	}
	if !ready || reason != "Available" {
		t.Fatalf("credentials readiness = %v/%s, want true/Available", ready, reason)
	}
}

func TestReconcileWaitsForDataset(t *testing.T) {
	scheme := testScheme(t)
	run := testRun()
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&agentstormv1alpha1.AgentTestRun{}).WithObjects(run).Build()
	reconciler := &AgentTestRunReconciler{Client: client, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}}

	result, err := reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected a requeue while the dataset is missing")
	}
	job := &batchv1.Job{}
	err = client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-worker"}, job)
	if err == nil {
		t.Fatal("worker Job should not exist before the dataset is available")
	}
}

func TestReconcileWaitsForValidFaultScenario(t *testing.T) {
	tests := []struct {
		name       string
		scenario   *corev1.ConfigMap
		wantReason string
	}{
		{name: "missing ConfigMap", wantReason: "ConfigMapMissing"},
		{
			name: "missing key",
			scenario: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "faults", Namespace: "default"},
				Data:       map[string]string{"other.json": `{}`},
			},
			wantReason: "KeyMissing",
		},
		{
			name: "invalid document",
			scenario: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "faults", Namespace: "default"},
				Data:       map[string]string{"scenario.json": `{"apiVersion":"agentstorm.io/v1alpha1","kind":"FaultScenario","rules":[{"name":"bad","fault":"shell","probability":1}]}`},
			},
			wantReason: "InvalidScenario",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := testScheme(t)
			run := testRunWithScenario()
			dataset := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "demo-dataset", Namespace: "default"},
				Data:       map[string]string{"cases.jsonl": `{"id":"1","input":"hello"}`},
			}
			objects := []client.Object{run, dataset}
			if test.scenario != nil {
				objects = append(objects, test.scenario)
			}
			kubernetesClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&agentstormv1alpha1.AgentTestRun{}).WithObjects(objects...).Build()
			reconciler := &AgentTestRunReconciler{Client: kubernetesClient, Scheme: scheme}
			request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}}

			result, err := reconciler.Reconcile(context.Background(), request)
			if err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if result.RequeueAfter == 0 {
				t.Fatal("expected a requeue while the scenario is unavailable")
			}
			if err := kubernetesClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-worker"}, &batchv1.Job{}); err == nil {
				t.Fatal("worker Job should not exist before the scenario is ready")
			}
			updated := &agentstormv1alpha1.AgentTestRun{}
			if err := kubernetesClient.Get(context.Background(), request.NamespacedName, updated); err != nil {
				t.Fatalf("get run: %v", err)
			}
			condition := meta.FindStatusCondition(updated.Status.Conditions, conditionScenario)
			if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != test.wantReason {
				t.Fatalf("ScenarioReady = %#v, want false/%s", condition, test.wantReason)
			}
		})
	}
}

func TestReconcileSnapshotsFaultScenarioWithoutDrift(t *testing.T) {
	scheme := testScheme(t)
	run := testRunWithScenario()
	dataset := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-dataset", Namespace: "default"},
		Data:       map[string]string{"cases.jsonl": `{"id":"1","input":"hello"}`},
	}
	scenario := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "faults", Namespace: "default"},
		Data: map[string]string{"scenario.json": `{
			"apiVersion":"agentstorm.io/v1alpha1",
			"kind":"FaultScenario",
			"rules":[{"name":"first","fault":"rate_limit","probability":1,"caseIDs":["1"]}]
		}`},
	}
	kubernetesClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&agentstormv1alpha1.AgentTestRun{}).WithObjects(run, dataset, scenario).Build()
	reconciler := &AgentTestRunReconciler{Client: kubernetesClient, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	generated := &corev1.ConfigMap{}
	generatedKey := types.NamespacedName{Namespace: "default", Name: "demo-config"}
	if err := kubernetesClient.Get(context.Background(), generatedKey, generated); err != nil {
		t.Fatalf("get generated config: %v", err)
	}
	originalSnapshot := generated.Data[workerConfigKey]
	var config workerRunConfig
	if err := json.Unmarshal([]byte(originalSnapshot), &config); err != nil {
		t.Fatalf("decode generated config: %v", err)
	}
	if config.Reliability == nil || config.Reliability.Scenario == nil || config.Reliability.Scenario.Digest == "" {
		t.Fatalf("generated reliability snapshot = %#v", config.Reliability)
	}
	if config.Reliability.Scenario.Scenario.Rules[0].Attempts[0] != 1 {
		t.Fatalf("scenario attempt default was not normalized: %#v", config.Reliability.Scenario)
	}

	if err := kubernetesClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "faults"}, scenario); err != nil {
		t.Fatalf("get source scenario: %v", err)
	}
	scenario.Data["scenario.json"] = `{"apiVersion":"agentstorm.io/v1alpha1","kind":"FaultScenario","rules":[]}`
	if err := kubernetesClient.Update(context.Background(), scenario); err != nil {
		t.Fatalf("update source scenario: %v", err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("reconcile after source change: %v", err)
	}
	if err := kubernetesClient.Get(context.Background(), generatedKey, generated); err != nil {
		t.Fatalf("get generated config after source change: %v", err)
	}
	if generated.Data[workerConfigKey] != originalSnapshot {
		t.Fatal("generated fault scenario snapshot drifted after source ConfigMap changed")
	}
}

func TestReconcileCancelsExistingJob(t *testing.T) {
	scheme := testScheme(t)
	run := testRun()
	run.Spec.Cancel = true
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "demo-worker", Namespace: "default"}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&agentstormv1alpha1.AgentTestRun{}).WithObjects(run, job).Build()
	reconciler := &AgentTestRunReconciler{Client: client, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-worker"}, &batchv1.Job{}); err == nil {
		t.Fatal("worker Job still exists after cancellation")
	}
	updated := &agentstormv1alpha1.AgentTestRun{}
	if err := client.Get(context.Background(), request.NamespacedName, updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != agentstormv1alpha1.AgentTestRunCancelled {
		t.Fatalf("phase = %q, want %q", updated.Status.Phase, agentstormv1alpha1.AgentTestRunCancelled)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("reconcile terminal run: %v", err)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-worker"}, &batchv1.Job{}); err == nil {
		t.Fatal("terminal cancelled run recreated its worker Job")
	}
}

func TestApplyJobStatus(t *testing.T) {
	completedAt := metav1.NewTime(time.Unix(1234, 0))
	tests := []struct {
		name       string
		job        *batchv1.Job
		wantPhase  agentstormv1alpha1.AgentTestRunPhase
		wantReason string
		wantReady  metav1.ConditionStatus
	}{
		{
			name: "success",
			job: &batchv1.Job{Status: batchv1.JobStatus{
				Succeeded:      2,
				CompletionTime: &completedAt,
				Conditions: []batchv1.JobCondition{{
					Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
				}},
			}},
			wantPhase:  agentstormv1alpha1.AgentTestRunSucceeded,
			wantReason: "WorkersCompleted",
			wantReady:  metav1.ConditionTrue,
		},
		{
			name: "failure",
			job: &batchv1.Job{Status: batchv1.JobStatus{
				Failed:         1,
				CompletionTime: &completedAt,
				Conditions: []batchv1.JobCondition{{
					Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Message: "worker exited non-zero",
				}},
			}},
			wantPhase:  agentstormv1alpha1.AgentTestRunFailed,
			wantReason: "WorkerJobFailed",
			wantReady:  metav1.ConditionFalse,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := testRun()
			applyJobStatus(run, tt.job)
			if run.Status.Phase != tt.wantPhase {
				t.Fatalf("phase = %q, want %q", run.Status.Phase, tt.wantPhase)
			}
			condition := meta.FindStatusCondition(run.Status.Conditions, conditionReady)
			if condition == nil || condition.Reason != tt.wantReason || condition.Status != tt.wantReady {
				t.Fatalf("ready condition = %#v, want reason %q and status %q", condition, tt.wantReason, tt.wantReady)
			}
			if run.Status.CompletionTime == nil || !run.Status.CompletionTime.Equal(&completedAt) {
				t.Fatalf("completion time = %v, want %v", run.Status.CompletionTime, completedAt)
			}
		})
	}
}

func TestChildNameTruncatesWithStableHash(t *testing.T) {
	if got := childName("demo", "worker"); got != "demo-worker" {
		t.Fatalf("short name = %q, want demo-worker", got)
	}

	prefix := strings.Repeat("a", 62)
	first := childName(prefix+"x", "worker")
	second := childName(prefix+"y", "worker")
	if len(first) > 63 || !strings.HasSuffix(first, "-worker") {
		t.Fatalf("truncated name = %q, want <= 63 chars with worker suffix", first)
	}
	if first == second {
		t.Fatalf("distinct long names collided at %q", first)
	}
	if first != childName(prefix+"x", "worker") {
		t.Fatalf("child name is not deterministic: %q", first)
	}
}

func TestValidateAndDefault(t *testing.T) {
	run := testRun()
	run.Spec.Workload.Iterations = 0
	if err := validateAndDefault(run); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if run.Spec.Workload.Iterations != 1 {
		t.Fatalf("iterations = %d, want 1", run.Spec.Workload.Iterations)
	}

	run = testRun()
	run.Spec.Target.Provider = "openai-agents"
	if err := validateAndDefault(run); err == nil {
		t.Fatal("expected openai-agents without a model to fail validation")
	}

	run = testRun()
	run.Spec.Target.Pricing = &agentstormv1alpha1.AgentPricingSpec{
		InputUSDPerMillionTokens:  "automatic",
		OutputUSDPerMillionTokens: "10",
	}
	if err := validateAndDefault(run); err == nil {
		t.Fatal("expected invalid pricing to fail validation")
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := agentstormv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func testRun() *agentstormv1alpha1.AgentTestRun {
	return &agentstormv1alpha1.AgentTestRun{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: types.UID("run-1")},
		Spec: agentstormv1alpha1.AgentTestRunSpec{
			Target: agentstormv1alpha1.AgentTargetSpec{Provider: "fake"},
			Workload: agentstormv1alpha1.AgentWorkloadSpec{
				DatasetRef:  corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "demo-dataset"}, Key: "cases.jsonl"},
				Parallelism: 2, ConcurrencyPerWorker: 3, TimeoutSeconds: 60,
			},
			Runner: agentstormv1alpha1.AgentRunnerSpec{Image: "agentstorm-worker:test"},
		},
	}
}

func testRunWithScenario() *agentstormv1alpha1.AgentTestRun {
	run := testRun()
	seed := int64(42)
	run.Spec.Reliability = &agentstormv1alpha1.AgentReliabilitySpec{
		Seed: &seed,
		ScenarioRef: &agentstormv1alpha1.AgentFaultScenarioRef{
			Name: "faults",
			Key:  "scenario.json",
		},
	}
	return run
}

func assertEnv(t *testing.T, env []corev1.EnvVar, name, want string) {
	t.Helper()
	for _, item := range env {
		if item.Name == name {
			if item.Value != want {
				t.Fatalf("env %s = %q, want %q", name, item.Value, want)
			}
			return
		}
	}
	t.Fatalf("env %s not found", name)
}

func assertSecretEnv(t *testing.T, env []corev1.EnvVar, name, secretName, secretKey string) {
	t.Helper()
	for _, item := range env {
		if item.Name != name {
			continue
		}
		if item.ValueFrom == nil || item.ValueFrom.SecretKeyRef == nil {
			t.Fatalf("env %s does not use a SecretKeyRef", name)
		}
		ref := item.ValueFrom.SecretKeyRef
		if ref.Name != secretName || ref.Key != secretKey {
			t.Fatalf("env %s SecretKeyRef = %s/%s, want %s/%s", name, ref.Name, ref.Key, secretName, secretKey)
		}
		return
	}
	t.Fatalf("env %s not found", name)
}

func assertWorkerSecurity(t *testing.T, pod corev1.PodSpec) {
	t.Helper()
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
		t.Fatal("worker must not automount a ServiceAccount token")
	}
	if pod.SecurityContext == nil || pod.SecurityContext.RunAsNonRoot == nil || !*pod.SecurityContext.RunAsNonRoot {
		t.Fatal("worker pod must run as non-root")
	}
	if pod.SecurityContext.SeccompProfile == nil || pod.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatal("worker pod must use the RuntimeDefault seccomp profile")
	}
	container := pod.Containers[0]
	if container.SecurityContext == nil || container.SecurityContext.ReadOnlyRootFilesystem == nil || !*container.SecurityContext.ReadOnlyRootFilesystem {
		t.Fatal("worker container must use a read-only root filesystem")
	}
	if container.SecurityContext.AllowPrivilegeEscalation == nil || *container.SecurityContext.AllowPrivilegeEscalation {
		t.Fatal("worker container must disable privilege escalation")
	}
	if container.SecurityContext.Capabilities == nil || len(container.SecurityContext.Capabilities.Drop) != 1 || container.SecurityContext.Capabilities.Drop[0] != "ALL" {
		t.Fatal("worker container must drop all Linux capabilities")
	}
	for _, volume := range pod.Volumes {
		if volume.Name == "output" && volume.EmptyDir != nil {
			return
		}
	}
	t.Fatal("worker output directory must use an EmptyDir volume")
}
