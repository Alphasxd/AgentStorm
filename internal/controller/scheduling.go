package controller

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	agentstormv1alpha1 "github.com/Alphasxd/AgentStorm/api/v1alpha1"
	"github.com/Alphasxd/AgentStorm/internal/results"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var scaledJobGVK = schema.GroupVersionKind{Group: "keda.sh", Version: "v1alpha1", Kind: "ScaledJob"}

const (
	scaledJobSuccessfulHistory int64 = 2
	scaledJobFailedHistory     int64 = 2
)

type SchedulerConfig struct {
	EnableKEDA           bool
	MetricsAPIURL        string
	MaxWorkersPerRun     int32
	MaxInFlightCases     int64
	RequireResourceQuota bool
	PollingInterval      int32
}

func (c *SchedulerConfig) ValidateAndDefault() error {
	if c.MaxWorkersPerRun == 0 {
		c.MaxWorkersPerRun = 100
	}
	if c.MaxInFlightCases == 0 {
		c.MaxInFlightCases = 1000
	}
	if c.PollingInterval == 0 {
		c.PollingInterval = 2
	}
	if c.MaxWorkersPerRun < 1 || c.MaxWorkersPerRun > 1000 {
		return fmt.Errorf("max-workers-per-run must be between 1 and 1000")
	}
	if c.MaxInFlightCases < 1 || c.MaxInFlightCases > 100000 {
		return fmt.Errorf("max-in-flight-cases must be between 1 and 100000")
	}
	if c.PollingInterval < 1 || c.PollingInterval > 300 {
		return fmt.Errorf("keda-polling-interval must be between 1 and 300 seconds")
	}
	c.MetricsAPIURL = strings.TrimRight(strings.TrimSpace(c.MetricsAPIURL), "/")
	if !c.EnableKEDA {
		if c.MetricsAPIURL != "" {
			return fmt.Errorf("keda-metrics-api-url requires enable-keda")
		}
		return nil
	}
	parsed, err := url.Parse(c.MetricsAPIURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("keda-metrics-api-url must be an HTTP(S) URL")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("keda-metrics-api-url must not contain credentials, query, or fragment")
	}
	return nil
}

func validateScheduling(run *agentstormv1alpha1.AgentTestRun, config SchedulerConfig) error {
	spec := &run.Spec.Scheduling
	if spec.Strategy == "" {
		spec.Strategy = "indexed"
	}
	if spec.MaxWorkers == 0 {
		spec.MaxWorkers = 1
	}
	if spec.ResourceProfile == "" {
		spec.ResourceProfile = "small"
	}
	if spec.Strategy != "indexed" && spec.Strategy != "keda" {
		return fmt.Errorf("scheduling.strategy must be indexed or keda")
	}
	if spec.MaxWorkers < 1 || spec.MaxWorkers > config.MaxWorkersPerRun {
		return fmt.Errorf("scheduling.maxWorkers exceeds the controller limit of %d", config.MaxWorkersPerRun)
	}
	if _, ok := schedulingResources(spec.ResourceProfile); !ok {
		return fmt.Errorf("scheduling.resourceProfile must be small, medium, or large")
	}
	workers := int64(run.Spec.Workload.Parallelism)
	if spec.Strategy == "keda" {
		workers = int64(spec.MaxWorkers)
	}
	if workers*int64(run.Spec.Workload.ConcurrencyPerWorker) > config.MaxInFlightCases {
		return fmt.Errorf("requested Worker concurrency exceeds the controller safety limit of %d", config.MaxInFlightCases)
	}
	if len(spec.Tolerations) > 16 {
		return fmt.Errorf("scheduling.tolerations must contain at most 16 entries")
	}
	if len(spec.NodeSelector) > 32 {
		return fmt.Errorf("scheduling.nodeSelector must contain at most 32 entries")
	}
	for key, value := range spec.NodeSelector {
		if len(validation.IsQualifiedName(key)) > 0 || len(validation.IsValidLabelValue(value)) > 0 {
			return fmt.Errorf("scheduling.nodeSelector contains an invalid label")
		}
	}
	for _, toleration := range spec.Tolerations {
		if toleration.Key != "" && len(validation.IsQualifiedName(toleration.Key)) > 0 {
			return fmt.Errorf("scheduling.tolerations contains an invalid key")
		}
		if toleration.Operator != "" && toleration.Operator != corev1.TolerationOpEqual && toleration.Operator != corev1.TolerationOpExists {
			return fmt.Errorf("scheduling.tolerations contains an invalid operator")
		}
		if toleration.Operator == corev1.TolerationOpExists && toleration.Value != "" {
			return fmt.Errorf("Exists tolerations cannot set value")
		}
		if toleration.Value != "" && len(validation.IsValidLabelValue(toleration.Value)) > 0 {
			return fmt.Errorf("scheduling.tolerations contains an invalid value")
		}
		if toleration.Effect != "" && toleration.Effect != corev1.TaintEffectNoSchedule && toleration.Effect != corev1.TaintEffectPreferNoSchedule && toleration.Effect != corev1.TaintEffectNoExecute {
			return fmt.Errorf("scheduling.tolerations contains an invalid effect")
		}
		if toleration.TolerationSeconds != nil && toleration.Effect != corev1.TaintEffectNoExecute {
			return fmt.Errorf("tolerationSeconds is only valid with NoExecute")
		}
		if toleration.TolerationSeconds != nil && *toleration.TolerationSeconds < 0 {
			return fmt.Errorf("tolerationSeconds must not be negative")
		}
	}
	return nil
}

func schedulingResources(profile string) (corev1.ResourceRequirements, bool) {
	profiles := map[string]corev1.ResourceRequirements{
		"small": {
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("512Mi")},
		},
		"medium": {
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("512Mi")},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("2Gi")},
		},
		"large": {
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("2Gi")},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: resource.MustParse("8Gi")},
		},
	}
	resources, ok := profiles[profile]
	return resources, ok
}

func applyScheduling(pod *corev1.PodSpec, spec agentstormv1alpha1.AgentSchedulingSpec) {
	resources, _ := schedulingResources(spec.ResourceProfile)
	pod.Containers[0].Resources = resources
	pod.NodeSelector = copyStringMap(spec.NodeSelector)
	pod.Tolerations = append([]corev1.Toleration(nil), spec.Tolerations...)
}

func copyStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func (r *AgentTestRunReconciler) schedulingReady(ctx context.Context, run *agentstormv1alpha1.AgentTestRun) (bool, string, string, error) {
	if run.Spec.Scheduling.Strategy == "keda" {
		if !r.Scheduler.EnableKEDA {
			return false, "KEDADisabled", "queued scheduling is disabled by the controller", nil
		}
		if !r.ResultSink.Enabled() {
			return false, "ResultSinkRequired", "queued scheduling requires the durable Result API", nil
		}
	}
	// Once the execution child exists, its Pod resources are immutable. Later namespace usage
	// must not retroactively invalidate an in-flight run. A ConfigMap alone is not an admission
	// snapshot because Result API registration can still fail before a child is created.
	if run.Spec.Scheduling.Strategy == "keda" {
		snapshot := &unstructured.Unstructured{}
		snapshot.SetGroupVersionKind(scaledJobGVK)
		err := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: childName(run.Name, "worker")}, snapshot)
		if err == nil && metav1.IsControlledBy(snapshot, run) {
			return true, "Admitted", "immutable scheduling admission was already completed", nil
		}
		if err != nil && !apierrors.IsNotFound(err) && !meta.IsNoMatchError(err) && !runtime.IsNotRegisteredError(err) {
			return false, "", "", err
		}
	} else {
		snapshot := &batchv1.Job{}
		err := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: childName(run.Name, "worker")}, snapshot)
		if err == nil && metav1.IsControlledBy(snapshot, run) {
			return true, "Admitted", "immutable scheduling admission was already completed", nil
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return false, "", "", err
		}
	}

	quotas := &corev1.ResourceQuotaList{}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	if err := reader.List(ctx, quotas, client.InNamespace(run.Namespace)); err != nil {
		return false, "", "", err
	}
	if len(quotas.Items) == 0 {
		if r.Scheduler.RequireResourceQuota {
			return false, "ResourceQuotaMissing", "the namespace must define a ResourceQuota before Workers can start", nil
		}
		return true, "BoundedProfile", "Worker resources are bounded by the selected profile", nil
	}
	workers := int64(run.Spec.Workload.Parallelism)
	if run.Spec.Scheduling.Strategy == "keda" {
		workers = int64(run.Spec.Scheduling.MaxWorkers)
	}
	resources, _ := schedulingResources(run.Spec.Scheduling.ResourceProfile)
	for _, quota := range quotas.Items {
		for resourceName, perWorker := range map[corev1.ResourceName]resource.Quantity{
			corev1.ResourceRequestsCPU:    resources.Requests[corev1.ResourceCPU],
			corev1.ResourceRequestsMemory: resources.Requests[corev1.ResourceMemory],
			corev1.ResourceLimitsCPU:      resources.Limits[corev1.ResourceCPU],
			corev1.ResourceLimitsMemory:   resources.Limits[corev1.ResourceMemory],
		} {
			if !quotaHasCapacity(quota, resourceName, perWorker, workers) {
				return false, "QuotaExceeded", fmt.Sprintf("ResourceQuota %s cannot admit the requested %s Worker profile", quota.Name, run.Spec.Scheduling.ResourceProfile), nil
			}
		}
		one := *resource.NewQuantity(1, resource.DecimalSI)
		if !quotaHasCapacity(quota, corev1.ResourcePods, one, workers) {
			return false, "QuotaExceeded", fmt.Sprintf("ResourceQuota %s cannot admit %d Worker Pods", quota.Name, workers), nil
		}
		jobs := int64(1)
		if run.Spec.Scheduling.Strategy == "keda" {
			jobs = workers + scaledJobSuccessfulHistory + scaledJobFailedHistory
			if !quotaHasCapacity(quota, corev1.ResourceName("count/scaledjobs.keda.sh"), one, 1) {
				return false, "QuotaExceeded", fmt.Sprintf("ResourceQuota %s cannot admit a ScaledJob", quota.Name), nil
			}
		}
		if !quotaHasCapacity(quota, corev1.ResourceName("count/jobs.batch"), one, jobs) {
			return false, "QuotaExceeded", fmt.Sprintf("ResourceQuota %s cannot admit %d Worker Jobs", quota.Name, jobs), nil
		}
	}
	return true, "QuotaAvailable", "namespace ResourceQuota can admit the bounded Worker profile", nil
}

func quotaHasCapacity(quota corev1.ResourceQuota, name corev1.ResourceName, perWorker resource.Quantity, workers int64) bool {
	hard, exists := quota.Status.Hard[name]
	if !exists {
		hard, exists = quota.Spec.Hard[name]
	}
	if !exists {
		return true
	}
	used := quota.Status.Used[name]
	remaining := hard.DeepCopy()
	remaining.Sub(used)
	required := perWorker.DeepCopy()
	if name == corev1.ResourceCPU || name == corev1.ResourceRequestsCPU || name == corev1.ResourceLimitsCPU {
		required = *resource.NewMilliQuantity(perWorker.MilliValue()*workers, resource.DecimalSI)
	} else {
		required = *resource.NewQuantity(perWorker.Value()*workers, perWorker.Format)
	}
	return remaining.Cmp(required) >= 0
}

func (r *AgentTestRunReconciler) ensureScaledJob(ctx context.Context, run *agentstormv1alpha1.AgentTestRun) (*unstructured.Unstructured, bool, error) {
	name := childName(run.Name, "worker")
	key := types.NamespacedName{Namespace: run.Namespace, Name: name}
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(scaledJobGVK)
	if err := r.Get(ctx, key, existing); err == nil {
		return existing, false, nil
	} else if !apierrors.IsNotFound(err) {
		if meta.IsNoMatchError(err) {
			return nil, false, fmt.Errorf("KEDA ScaledJob CRD is not installed")
		}
		return nil, false, err
	}
	desired, err := buildScaledJob(run, r.ResultSink, r.Telemetry, r.Scheduler)
	if err != nil {
		return nil, false, err
	}
	if err := controllerutil.SetControllerReference(run, desired, r.Scheme); err != nil {
		return nil, false, err
	}
	if err := r.Create(ctx, desired); err != nil {
		return nil, false, err
	}
	return desired, true, nil
}

func buildScaledJob(run *agentstormv1alpha1.AgentTestRun, resultSink ResultSinkConfig, telemetry TelemetryConfig, scheduler SchedulerConfig) (*unstructured.Unstructured, error) {
	job := buildJob(run, resultSink, telemetry)
	one := int32(1)
	job.Spec.Parallelism = &one
	job.Spec.Completions = &one
	job.Spec.CompletionMode = nil
	job.Spec.Template.Spec.Containers[0].Env = queueWorkerEnvironment(
		job.Spec.Template.Spec.Containers[0].Env,
	)
	jobSpec, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&job.Spec)
	if err != nil {
		return nil, fmt.Errorf("convert KEDA job target: %w", err)
	}
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "keda.sh/v1alpha1",
		"kind":       "ScaledJob",
		"metadata": map[string]any{
			"name":      childName(run.Name, "worker"),
			"namespace": run.Namespace,
			"labels":    stringMapToAny(job.Labels),
		},
		"spec": map[string]any{
			"pollingInterval":            int64(scheduler.PollingInterval),
			"minReplicaCount":            int64(0),
			"maxReplicaCount":            int64(run.Spec.Scheduling.MaxWorkers),
			"successfulJobsHistoryLimit": scaledJobSuccessfulHistory,
			"failedJobsHistoryLimit":     scaledJobFailedHistory,
			"rollout":                    map[string]any{"strategy": "gradual"},
			"scalingStrategy":            map[string]any{"strategy": "accurate"},
			"jobTargetRef":               jobSpec,
			"triggers": []any{map[string]any{
				"type": "metrics-api",
				"metadata": map[string]any{
					"url":                   scheduler.MetricsAPIURL + "/v1/runs/" + url.PathEscape(string(run.UID)) + "/queue",
					"format":                "json",
					"valueLocation":         "available",
					"targetValue":           "1",
					"activationTargetValue": "0",
					"timeout":               "3000",
				},
			}},
		},
	}}
	object.SetGroupVersionKind(scaledJobGVK)
	return object, nil
}

func queueWorkerEnvironment(input []corev1.EnvVar) []corev1.EnvVar {
	output := make([]corev1.EnvVar, 0, len(input)+1)
	for _, item := range input {
		if item.Name != "AGENTSTORM_SHARD_INDEX" && item.Name != "AGENTSTORM_QUEUE_MODE" {
			output = append(output, item)
		}
	}
	output = append(output, corev1.EnvVar{Name: "AGENTSTORM_QUEUE_MODE", Value: "true"})
	return output
}

func stringMapToAny(input map[string]string) map[string]any {
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func (r *AgentTestRunReconciler) deleteScaledJob(ctx context.Context, run *agentstormv1alpha1.AgentTestRun) error {
	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(scaledJobGVK)
	object.SetName(childName(run.Name, "worker"))
	object.SetNamespace(run.Namespace)
	if err := r.Delete(ctx, object, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil && !apierrors.IsNotFound(err) && !meta.IsNoMatchError(err) {
		return err
	}
	jobs := &batchv1.JobList{}
	selector := labels.SelectorFromSet(map[string]string{"agentstorm.io/run": run.Name})
	if err := r.List(ctx, jobs, client.InNamespace(run.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return err
	}
	for index := range jobs.Items {
		if err := r.Delete(ctx, &jobs.Items[index], client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func applyQueueStatus(run *agentstormv1alpha1.AgentTestRun, status results.QueueStatus) {
	run.Status.Active = int32(status.Leased)
	run.Status.Succeeded = int32(status.Completed)
	run.Status.Failed = 0
	switch status.RunStatus {
	case results.RunCancelled:
		setTerminalStatus(run, agentstormv1alpha1.AgentTestRunCancelled, "Cancelled", "queued run was cancelled")
	case results.RunHarnessFailed:
		setTerminalStatus(run, agentstormv1alpha1.AgentTestRunFailed, "HarnessFailed", "queued Worker reported a harness failure")
	case results.RunComplete:
		if status.ThresholdsPassed != nil && *status.ThresholdsPassed {
			setTerminalStatus(run, agentstormv1alpha1.AgentTestRunSucceeded, "Completed", "all queued shards completed")
			meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
				Type: conditionReady, Status: metav1.ConditionTrue, Reason: "Completed",
				Message: "all queued shards completed", ObservedGeneration: run.Generation,
			})
		} else {
			setTerminalStatus(run, agentstormv1alpha1.AgentTestRunFailed, "ThresholdsFailed", "queued run did not pass evaluation thresholds")
		}
	default:
		run.Status.Phase = agentstormv1alpha1.AgentTestRunRunning
	}
}
