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
	if stored.Spec.Telemetry.ContentMode != "omit" {
		t.Fatalf("telemetry content mode = %q, want omit default", stored.Spec.Telemetry.ContentMode)
	}
	if stored.Spec.Scheduling.Strategy != "indexed" || stored.Spec.Scheduling.MaxWorkers != 1 || stored.Spec.Scheduling.ResourceProfile != "small" {
		t.Fatalf("scheduling defaults were not applied: %#v", stored.Spec.Scheduling)
	}

	seed := int64(42)
	reliabilityRun := validEnvtestRun(namespace.Name, "reliability")
	reliabilityRun.Spec.Reliability = &agentstormv1alpha1.AgentReliabilitySpec{
		Seed: &seed,
		ScenarioRef: &agentstormv1alpha1.AgentFaultScenarioRef{
			Name: "faults",
			Key:  "scenario.json",
		},
	}
	if err := kubernetesClient.Create(ctx, reliabilityRun); err != nil {
		t.Fatalf("create reliability AgentTestRun: %v", err)
	}
	if err := kubernetesClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: reliabilityRun.Name}, reliabilityRun); err != nil {
		t.Fatalf("get reliability AgentTestRun: %v", err)
	}
	if reliabilityRun.Spec.Reliability == nil ||
		reliabilityRun.Spec.Reliability.Retry.MaxAttempts != 1 ||
		reliabilityRun.Spec.Reliability.Retry.InitialBackoffMs != 100 ||
		reliabilityRun.Spec.Reliability.Retry.JitterRatio == nil ||
		*reliabilityRun.Spec.Reliability.Retry.JitterRatio != 0.2 {
		t.Fatalf("reliability defaults were not applied: %#v", reliabilityRun.Spec.Reliability)
	}
	reliabilityRun.Spec.Reliability.Retry.MaxAttempts = 2
	err = kubernetesClient.Update(ctx, reliabilityRun)
	if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "reliability is immutable") {
		t.Fatalf("immutable reliability update error = %v, want CRD CEL rejection", err)
	}

	missingSeed := validEnvtestRun(namespace.Name, "missing-seed")
	missingSeed.Spec.Reliability = &agentstormv1alpha1.AgentReliabilitySpec{
		ScenarioRef: &agentstormv1alpha1.AgentFaultScenarioRef{Name: "faults", Key: "scenario.json"},
	}
	err = kubernetesClient.Create(ctx, missingSeed)
	if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "seed") {
		t.Fatalf("missing reliability seed error = %v, want CRD CEL rejection", err)
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

	pluginRun := validEnvtestRun(namespace.Name, "adapter-plugin")
	pluginRun.Spec.Target.AdapterEntrypoint = "agentstorm_worker.benchmarks.sre:create_adapter"
	if err := kubernetesClient.Create(ctx, pluginRun); err != nil {
		t.Fatalf("create Adapter plugin AgentTestRun: %v", err)
	}
	pluginRun.Spec.Target.AdapterEntrypoint = "agentstorm_worker.benchmarks.sre:other_adapter"
	err = kubernetesClient.Update(ctx, pluginRun)
	if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "target is immutable") {
		t.Fatalf("immutable Adapter entrypoint error = %v, want target CEL rejection", err)
	}

	invalidPlugin := validEnvtestRun(namespace.Name, "invalid-adapter-plugin")
	invalidPlugin.Spec.Target.AdapterEntrypoint = "remote://plugin"
	if err := kubernetesClient.Create(ctx, invalidPlugin); !apierrors.IsInvalid(err) {
		t.Fatalf("invalid Adapter entrypoint error = %v, want Invalid", err)
	}

	redacted := validEnvtestRun(namespace.Name, "redacted")
	redacted.Spec.Telemetry = agentstormv1alpha1.AgentTelemetrySpec{
		ContentMode: "redacted",
		Redaction: agentstormv1alpha1.AgentTelemetryRedactionSpec{
			Patterns:     []string{`customer-[0-9]+`},
			MetadataKeys: []string{"tenant"},
		},
	}
	if err := kubernetesClient.Create(ctx, redacted); err != nil {
		t.Fatalf("create redacted AgentTestRun: %v", err)
	}
	redacted.Spec.Telemetry.ContentMode = "omit"
	err = kubernetesClient.Update(ctx, redacted)
	if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "telemetry is immutable") {
		t.Fatalf("immutable telemetry update error = %v, want CRD CEL rejection", err)
	}

	tooManyPatterns := validEnvtestRun(namespace.Name, "too-many-patterns")
	tooManyPatterns.Spec.Telemetry = agentstormv1alpha1.AgentTelemetrySpec{
		ContentMode: "redacted",
		Redaction: agentstormv1alpha1.AgentTelemetryRedactionSpec{
			Patterns: make([]string, 21),
		},
	}
	err = kubernetesClient.Create(ctx, tooManyPatterns)
	if !apierrors.IsInvalid(err) {
		t.Fatalf("too many redaction patterns error = %v, want Invalid", err)
	}

	queued := validEnvtestRun(namespace.Name, "queued")
	queued.Spec.Workload.Parallelism = 64
	queued.Spec.Workload.ConcurrencyPerWorker = 2
	queued.Spec.Scheduling = agentstormv1alpha1.AgentSchedulingSpec{
		Strategy: "keda", MaxWorkers: 8, ResourceProfile: "medium",
	}
	if err := kubernetesClient.Create(ctx, queued); err != nil {
		t.Fatalf("create queued AgentTestRun: %v", err)
	}
	queued.Spec.Scheduling.MaxWorkers = 9
	err = kubernetesClient.Update(ctx, queued)
	if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "scheduling is immutable") {
		t.Fatalf("immutable scheduling update error = %v, want CRD CEL rejection", err)
	}

	unsafe := validEnvtestRun(namespace.Name, "unsafe-scheduling")
	unsafe.Spec.Workload.ConcurrencyPerWorker = 101
	unsafe.Spec.Scheduling = agentstormv1alpha1.AgentSchedulingSpec{
		Strategy: "keda", MaxWorkers: 10, ResourceProfile: "small",
	}
	if err := kubernetesClient.Create(ctx, unsafe); !apierrors.IsInvalid(err) {
		t.Fatalf("unsafe scheduling error = %v, want Invalid", err)
	}

	tooManyShards := validEnvtestRun(namespace.Name, "too-many-shards")
	tooManyShards.Spec.Workload.Parallelism = 10001
	if err := kubernetesClient.Create(ctx, tooManyShards); !apierrors.IsInvalid(err) {
		t.Fatalf("excessive shard count error = %v, want Invalid", err)
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
