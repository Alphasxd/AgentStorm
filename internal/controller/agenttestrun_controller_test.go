package controller

import (
	"context"
	"strings"
	"testing"

	agentstormv1alpha1 "github.com/Alphasxd/AgentStorm/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
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
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&agentstormv1alpha1.AgentTestRun{}).WithObjects(run, dataset).Build()
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
