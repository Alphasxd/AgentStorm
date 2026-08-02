//go:build envtest

package v1alpha1_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	agentstormv1alpha1 "github.com/Alphasxd/AgentStorm/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestAgentTestRunCRDContract(t *testing.T) {
	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	assetsDirectory := os.Getenv("ENVTEST_ASSETS_DIRECTORY")
	if assetsDirectory == "" {
		assetsDirectory = filepath.Join(repositoryRoot, ".cache", "envtest")
	}
	kubernetesVersion := os.Getenv("ENVTEST_K8S_VERSION")
	if kubernetesVersion == "" {
		kubernetesVersion = "v1.33.0"
	}

	testEnvironment := &envtest.Environment{
		CRDDirectoryPaths:           []string{filepath.Join(repositoryRoot, "config", "crd", "bases")},
		ErrorIfCRDPathMissing:       true,
		DownloadBinaryAssets:        true,
		DownloadBinaryAssetsVersion: kubernetesVersion,
		BinaryAssetsDirectory:       assetsDirectory,
		ControlPlaneStartTimeout:    60 * time.Second,
		ControlPlaneStopTimeout:     60 * time.Second,
	}
	config, err := testEnvironment.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := testEnvironment.Stop(); err != nil {
			t.Errorf("stop envtest: %v", err)
		}
	})

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := agentstormv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kubernetesClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("create Kubernetes client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "agentstorm-envtest"}}
	if err := kubernetesClient.Create(ctx, namespace); err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	run := validEnvtestRun(namespace.Name, "contract")
	run.Spec.Target.Pricing = &agentstormv1alpha1.AgentPricingSpec{
		InputUSDPerMillionTokens:  "2.5",
		OutputUSDPerMillionTokens: "10",
	}
	if err := kubernetesClient.Create(ctx, run); err != nil {
		t.Fatalf("create AgentTestRun: %v", err)
	}
	key := types.NamespacedName{Namespace: namespace.Name, Name: run.Name}
	stored := &agentstormv1alpha1.AgentTestRun{}
	if err := kubernetesClient.Get(ctx, key, stored); err != nil {
		t.Fatalf("get AgentTestRun: %v", err)
	}
	if stored.Spec.Workload.Parallelism != 1 || stored.Spec.Workload.ConcurrencyPerWorker != 1 || stored.Spec.Workload.Iterations != 1 || stored.Spec.Workload.TimeoutSeconds != 900 {
		t.Fatalf("CRD defaults were not applied: %#v", stored.Spec.Workload)
	}

	stored.Spec.Cancel = true
	stored.Status.Phase = agentstormv1alpha1.AgentTestRunFailed
	if err := kubernetesClient.Update(ctx, stored); err != nil {
		t.Fatalf("update mutable cancel field: %v", err)
	}
	if err := kubernetesClient.Get(ctx, key, stored); err != nil {
		t.Fatalf("get updated AgentTestRun: %v", err)
	}
	if !stored.Spec.Cancel {
		t.Fatal("spec.cancel update was not persisted")
	}
	if stored.Status.Phase != "" {
		t.Fatalf("normal update changed status subresource to %q", stored.Status.Phase)
	}

	stored.Status.Phase = agentstormv1alpha1.AgentTestRunRunning
	stored.Status.ObservedGeneration = stored.Generation
	if err := kubernetesClient.Status().Update(ctx, stored); err != nil {
		t.Fatalf("update status subresource: %v", err)
	}
	if err := kubernetesClient.Get(ctx, key, stored); err != nil {
		t.Fatalf("get status-updated AgentTestRun: %v", err)
	}
	if stored.Status.Phase != agentstormv1alpha1.AgentTestRunRunning {
		t.Fatalf("status phase = %q, want Running", stored.Status.Phase)
	}

	stored.Spec.Workload.Parallelism = 2
	err = kubernetesClient.Update(ctx, stored)
	if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "workload is immutable") {
		t.Fatalf("immutable workload update error = %v, want CRD CEL rejection", err)
	}

	invalidRate := 1.1
	invalid := validEnvtestRun(namespace.Name, "invalid-rate")
	invalid.Spec.Evaluation.MinSuccessRate = &invalidRate
	err = kubernetesClient.Create(ctx, invalid)
	if !apierrors.IsInvalid(err) {
		t.Fatalf("out-of-range success rate error = %v, want Invalid", err)
	}

	invalidPricing := validEnvtestRun(namespace.Name, "invalid-pricing")
	invalidPricing.Spec.Target.Pricing = &agentstormv1alpha1.AgentPricingSpec{
		InputUSDPerMillionTokens:  "automatic",
		OutputUSDPerMillionTokens: "10",
	}
	err = kubernetesClient.Create(ctx, invalidPricing)
	if !apierrors.IsInvalid(err) {
		t.Fatalf("invalid pricing error = %v, want Invalid", err)
	}
}

func validEnvtestRun(namespace, name string) *agentstormv1alpha1.AgentTestRun {
	return &agentstormv1alpha1.AgentTestRun{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: agentstormv1alpha1.AgentTestRunSpec{
			Target: agentstormv1alpha1.AgentTargetSpec{Provider: "fake"},
			Workload: agentstormv1alpha1.AgentWorkloadSpec{
				DatasetRef: corev1.ConfigMapKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "dataset"},
					Key:                  "cases.jsonl",
				},
			},
			Runner: agentstormv1alpha1.AgentRunnerSpec{Image: "agentstorm-worker:test"},
		},
	}
}
