package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	agentstormv1alpha1 "github.com/Alphasxd/AgentStorm/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	conditionReady       = "Ready"
	conditionDataset     = "DatasetReady"
	conditionCredentials = "CredentialsReady"
	conditionResultSink  = "ResultSinkReady"
	workerConfigKey      = "run.json"
)

type ResultSinkConfig struct {
	URL                  string
	WriteTokenSecretName string
	WriteTokenSecretKey  string
	IncludeSensitive     bool
	TimeoutSeconds       int
}

func (c *ResultSinkConfig) ValidateAndDefault() error {
	c.URL = strings.TrimRight(strings.TrimSpace(c.URL), "/")
	if c.URL == "" {
		if c.IncludeSensitive {
			return fmt.Errorf("include-sensitive-results requires result-api-url")
		}
		return nil
	}
	parsed, err := url.Parse(c.URL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("result-api-url must be an HTTP(S) URL")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("result-api-url must not contain credentials, query, or fragment")
	}
	if c.WriteTokenSecretName == "" || len(utilvalidation.IsDNS1123Subdomain(c.WriteTokenSecretName)) > 0 {
		return fmt.Errorf("result-write-token-secret-name must be a valid Secret name")
	}
	if c.WriteTokenSecretKey == "" || len(utilvalidation.IsConfigMapKey(c.WriteTokenSecretKey)) > 0 {
		return fmt.Errorf("result-write-token-secret-key must be a valid Secret data key")
	}
	if c.TimeoutSeconds == 0 {
		c.TimeoutSeconds = 30
	}
	if c.TimeoutSeconds < 1 || c.TimeoutSeconds > 300 {
		return fmt.Errorf("result-upload-timeout must be between 1 and 300 seconds")
	}
	return nil
}

func (c ResultSinkConfig) Enabled() bool {
	return c.URL != ""
}

// AgentTestRunReconciler turns an AgentTestRun into an indexed Kubernetes Job.
type AgentTestRunReconciler struct {
	client.Client
	APIReader  client.Reader
	Scheme     *runtime.Scheme
	ResultSink ResultSinkConfig
}

// +kubebuilder:rbac:groups=agentstorm.io,resources=agenttestruns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=agentstorm.io,resources=agenttestruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=agentstorm.io,resources=agenttestruns/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *AgentTestRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	run := &agentstormv1alpha1.AgentTestRun{}
	if err := r.Get(ctx, req.NamespacedName, run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if terminalPhase(run.Status.Phase) {
		return ctrl.Result{}, nil
	}

	before := run.DeepCopy()
	if err := validateAndDefault(run); err != nil {
		setTerminalStatus(run, agentstormv1alpha1.AgentTestRunFailed, "InvalidSpec", err.Error())
		return ctrl.Result{}, r.patchStatus(ctx, before, run)
	}

	jobName := childName(run.Name, "worker")
	if run.Spec.Cancel {
		if err := r.cancelJob(ctx, run.Namespace, jobName); err != nil {
			return ctrl.Result{}, err
		}
		setTerminalStatus(run, agentstormv1alpha1.AgentTestRunCancelled, "Cancelled", "run cancellation requested")
		run.Status.JobName = jobName
		run.Status.Active = 0
		run.Status.ObservedGeneration = run.Generation
		return ctrl.Result{}, r.patchStatus(ctx, before, run)
	}

	datasetReady, err := r.datasetReady(ctx, run)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !datasetReady {
		run.Status.Phase = agentstormv1alpha1.AgentTestRunPending
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
			Type: conditionDataset, Status: metav1.ConditionFalse, Reason: "ConfigMapMissing",
			Message: "referenced dataset ConfigMap or key is not available", ObservedGeneration: run.Generation,
		})
		if err := r.patchStatus(ctx, before, run); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type: conditionDataset, Status: metav1.ConditionTrue, Reason: "Available",
		Message: "dataset ConfigMap is available", ObservedGeneration: run.Generation,
	})
	credentialsReady, reason, message, err := r.credentialsReady(ctx, run)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !credentialsReady {
		run.Status.Phase = agentstormv1alpha1.AgentTestRunPending
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
			Type: conditionCredentials, Status: metav1.ConditionFalse, Reason: reason,
			Message: message, ObservedGeneration: run.Generation,
		})
		if err := r.patchStatus(ctx, before, run); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if run.Spec.Target.APIKeySecretRef != nil {
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
			Type: conditionCredentials, Status: metav1.ConditionTrue, Reason: reason,
			Message: message, ObservedGeneration: run.Generation,
		})
	}
	resultSinkReady, reason, message, err := r.resultSinkReady(ctx, run)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !resultSinkReady {
		run.Status.Phase = agentstormv1alpha1.AgentTestRunPending
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
			Type: conditionResultSink, Status: metav1.ConditionFalse, Reason: reason,
			Message: message, ObservedGeneration: run.Generation,
		})
		if err := r.patchStatus(ctx, before, run); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if r.ResultSink.Enabled() {
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
			Type: conditionResultSink, Status: metav1.ConditionTrue, Reason: reason,
			Message: message, ObservedGeneration: run.Generation,
		})
	}
	if err := r.ensureWorkerConfig(ctx, run); err != nil {
		return ctrl.Result{}, err
	}

	job, created, err := r.ensureJob(ctx, run)
	if err != nil {
		return ctrl.Result{}, err
	}
	applyJobStatus(run, job)
	run.Status.JobName = job.Name
	run.Status.ObservedGeneration = run.Generation
	if created && run.Status.StartedAt == nil {
		now := metav1.Now()
		run.Status.StartedAt = &now
	}
	if err := r.patchStatus(ctx, before, run); err != nil {
		return ctrl.Result{}, err
	}

	if run.Status.Phase == agentstormv1alpha1.AgentTestRunRunning || run.Status.Phase == agentstormv1alpha1.AgentTestRunPending {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	logger.Info("AgentTestRun reached terminal phase", "phase", run.Status.Phase, "job", job.Name)
	return ctrl.Result{}, nil
}

func (r *AgentTestRunReconciler) resultSinkReady(ctx context.Context, run *agentstormv1alpha1.AgentTestRun) (bool, string, string, error) {
	if !r.ResultSink.Enabled() {
		return true, "NotConfigured", "durable result sink is not configured", nil
	}
	secret := &corev1.Secret{}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	err := reader.Get(ctx, types.NamespacedName{
		Namespace: run.Namespace,
		Name:      r.ResultSink.WriteTokenSecretName,
	}, secret)
	if apierrors.IsNotFound(err) {
		return false, "SecretMissing", "result sink write-token Secret is not available", nil
	}
	if err != nil {
		return false, "", "", err
	}
	if _, exists := secret.Data[r.ResultSink.WriteTokenSecretKey]; !exists {
		if _, exists = secret.StringData[r.ResultSink.WriteTokenSecretKey]; !exists {
			return false, "KeyMissing", "result sink write-token Secret key is not available", nil
		}
	}
	return true, "Available", "result sink write-token Secret is available", nil
}

func (r *AgentTestRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&agentstormv1alpha1.AgentTestRun{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.ConfigMap{}).
		Complete(r)
}

func validateAndDefault(run *agentstormv1alpha1.AgentTestRun) error {
	if run.Spec.Target.Provider != "fake" && run.Spec.Target.Provider != "openai-agents" {
		return fmt.Errorf("unsupported provider %q", run.Spec.Target.Provider)
	}
	if run.Spec.Target.Provider == "openai-agents" && strings.TrimSpace(run.Spec.Target.Model) == "" {
		return fmt.Errorf("target.model is required for provider openai-agents")
	}
	if strings.TrimSpace(run.Spec.Runner.Image) == "" {
		return fmt.Errorf("runner.image is required")
	}
	if strings.TrimSpace(run.Spec.Workload.DatasetRef.Name) == "" || strings.TrimSpace(run.Spec.Workload.DatasetRef.Key) == "" {
		return fmt.Errorf("workload.datasetRef.name and key are required")
	}
	if run.Spec.Workload.Parallelism == 0 {
		run.Spec.Workload.Parallelism = 1
	}
	if run.Spec.Workload.ConcurrencyPerWorker == 0 {
		run.Spec.Workload.ConcurrencyPerWorker = 1
	}
	if run.Spec.Workload.Iterations == 0 {
		run.Spec.Workload.Iterations = 1
	}
	if run.Spec.Workload.TimeoutSeconds == 0 {
		run.Spec.Workload.TimeoutSeconds = 900
	}
	if run.Spec.Workload.Parallelism < 1 || run.Spec.Workload.ConcurrencyPerWorker < 1 || run.Spec.Workload.Iterations < 1 || run.Spec.Workload.TimeoutSeconds < 1 {
		return fmt.Errorf("parallelism, concurrencyPerWorker, iterations and timeoutSeconds must be positive")
	}
	return nil
}

func terminalPhase(phase agentstormv1alpha1.AgentTestRunPhase) bool {
	return phase == agentstormv1alpha1.AgentTestRunSucceeded ||
		phase == agentstormv1alpha1.AgentTestRunFailed ||
		phase == agentstormv1alpha1.AgentTestRunCancelled
}

func (r *AgentTestRunReconciler) datasetReady(ctx context.Context, run *agentstormv1alpha1.AgentTestRun) (bool, error) {
	ref := run.Spec.Workload.DatasetRef
	dataset := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: ref.Name}, dataset)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_, exists := dataset.Data[ref.Key]
	if !exists {
		_, exists = dataset.BinaryData[ref.Key]
	}
	return exists, nil
}

func (r *AgentTestRunReconciler) credentialsReady(ctx context.Context, run *agentstormv1alpha1.AgentTestRun) (bool, string, string, error) {
	ref := run.Spec.Target.APIKeySecretRef
	if ref == nil {
		return true, "NotRequired", "no provider Secret is configured", nil
	}

	secret := &corev1.Secret{}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	err := reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: ref.Name}, secret)
	if apierrors.IsNotFound(err) {
		if ref.Optional != nil && *ref.Optional {
			return true, "Optional", "optional provider Secret is not available", nil
		}
		return false, "SecretMissing", "referenced provider Secret is not available", nil
	}
	if err != nil {
		return false, "", "", err
	}

	_, exists := secret.Data[ref.Key]
	if !exists {
		_, exists = secret.StringData[ref.Key]
	}
	if !exists {
		if ref.Optional != nil && *ref.Optional {
			return true, "Optional", "optional provider Secret key is not available", nil
		}
		return false, "KeyMissing", "referenced provider Secret key is not available", nil
	}
	return true, "Available", "provider Secret key is available", nil
}

type workerRunConfig struct {
	RunID      string                                 `json:"run_id"`
	Source     workerRunSourceConfig                  `json:"source"`
	Dataset    workerDatasetConfig                    `json:"dataset"`
	Target     agentstormv1alpha1.AgentTargetSpec     `json:"target"`
	Workload   workerWorkloadConfig                   `json:"workload"`
	Evaluation agentstormv1alpha1.AgentEvaluationSpec `json:"evaluation,omitempty"`
}

type workerRunSourceConfig struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type workerDatasetConfig struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

type workerWorkloadConfig struct {
	Concurrency    int32 `json:"concurrency"`
	Iterations     int32 `json:"iterations"`
	TimeoutSeconds int64 `json:"timeout_seconds"`
}

func (r *AgentTestRunReconciler) ensureWorkerConfig(ctx context.Context, run *agentstormv1alpha1.AgentTestRun) error {
	config := workerRunConfig{
		RunID:  string(run.UID),
		Source: workerRunSourceConfig{Namespace: run.Namespace, Name: run.Name},
		Dataset: workerDatasetConfig{
			Name: run.Spec.Workload.DatasetRef.Name,
			Key:  run.Spec.Workload.DatasetRef.Key,
		},
		Target: run.Spec.Target,
		Workload: workerWorkloadConfig{
			Concurrency:    run.Spec.Workload.ConcurrencyPerWorker,
			Iterations:     run.Spec.Workload.Iterations,
			TimeoutSeconds: run.Spec.Workload.TimeoutSeconds,
		},
		Evaluation: run.Spec.Evaluation,
	}
	// Secrets are mounted through environment variables and must never be serialized.
	config.Target.APIKeySecretRef = nil
	payload, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal worker config: %w", err)
	}

	name := childName(run.Name, "config")
	cm := &corev1.ConfigMap{}
	key := types.NamespacedName{Namespace: run.Namespace, Name: name}
	if err := r.Get(ctx, key, cm); apierrors.IsNotFound(err) {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: run.Namespace},
			Data:       map[string]string{workerConfigKey: string(payload)},
		}
		if err := controllerutil.SetControllerReference(run, cm, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, cm)
	} else if err != nil {
		return err
	}

	if cm.Data[workerConfigKey] == string(payload) {
		return nil
	}
	cm.Data[workerConfigKey] = string(payload)
	return r.Update(ctx, cm)
}

func (r *AgentTestRunReconciler) ensureJob(ctx context.Context, run *agentstormv1alpha1.AgentTestRun) (*batchv1.Job, bool, error) {
	job := &batchv1.Job{}
	name := childName(run.Name, "worker")
	key := types.NamespacedName{Namespace: run.Namespace, Name: name}
	if err := r.Get(ctx, key, job); err == nil {
		return job, false, nil
	} else if !apierrors.IsNotFound(err) {
		return nil, false, err
	}

	job = buildJob(run, r.ResultSink)
	if err := controllerutil.SetControllerReference(run, job, r.Scheme); err != nil {
		return nil, false, err
	}
	if err := r.Create(ctx, job); err != nil {
		return nil, false, err
	}
	return job, true, nil
}

func buildJob(run *agentstormv1alpha1.AgentTestRun, resultSink ResultSinkConfig) *batchv1.Job {
	parallelism := run.Spec.Workload.Parallelism
	backoffLimit := int32(0)
	completionMode := batchv1.IndexedCompletion
	pullPolicy := run.Spec.Runner.ImagePullPolicy
	if pullPolicy == "" {
		pullPolicy = corev1.PullIfNotPresent
	}
	labels := map[string]string{
		"app.kubernetes.io/name":       "agentstorm-worker",
		"app.kubernetes.io/managed-by": "agentstorm-controller",
		"agentstorm.io/run":            run.Name,
	}
	env := []corev1.EnvVar{
		{Name: "AGENTSTORM_RUN_CONFIG", Value: "/etc/agentstorm/run/run.json"},
		{Name: "AGENTSTORM_DATASET", Value: "/etc/agentstorm/dataset/" + run.Spec.Workload.DatasetRef.Key},
		{Name: "AGENTSTORM_OUTPUT_DIR", Value: "/tmp/agentstorm"},
		{Name: "AGENTSTORM_SHARD_COUNT", Value: strconv.Itoa(int(parallelism))},
		{
			Name: "AGENTSTORM_SHARD_INDEX",
			ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: "metadata.annotations['batch.kubernetes.io/job-completion-index']",
			}},
		},
	}
	if ref := run.Spec.Target.APIKeySecretRef; ref != nil {
		env = append(env, corev1.EnvVar{
			Name:      "OPENAI_API_KEY",
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: ref.DeepCopy()},
		})
	}
	if resultSink.Enabled() {
		env = append(env,
			corev1.EnvVar{Name: "AGENTSTORM_RESULT_API_URL", Value: resultSink.URL},
			corev1.EnvVar{
				Name: "AGENTSTORM_RESULT_WRITE_TOKEN",
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: resultSink.WriteTokenSecretName},
					Key:                  resultSink.WriteTokenSecretKey,
				}},
			},
			corev1.EnvVar{Name: "AGENTSTORM_INCLUDE_SENSITIVE_RESULTS", Value: strconv.FormatBool(resultSink.IncludeSensitive)},
			corev1.EnvVar{Name: "AGENTSTORM_RESULT_TIMEOUT_SECONDS", Value: strconv.Itoa(resultSink.TimeoutSeconds)},
		)
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: childName(run.Name, "worker"), Namespace: run.Namespace, Labels: labels},
		Spec: batchv1.JobSpec{
			Parallelism:           &parallelism,
			Completions:           &parallelism,
			CompletionMode:        &completionMode,
			BackoffLimit:          &backoffLimit,
			ActiveDeadlineSeconds: &run.Spec.Workload.TimeoutSeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: ptr.To(false),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot:   ptr.To(true),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{{
						Name: "worker", Image: run.Spec.Runner.Image, ImagePullPolicy: pullPolicy,
						Args: []string{"run"}, Env: env,
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: ptr.To(false),
							ReadOnlyRootFilesystem:   ptr.To(true),
							RunAsNonRoot:             ptr.To(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "run-config", MountPath: "/etc/agentstorm/run", ReadOnly: true},
							{Name: "dataset", MountPath: "/etc/agentstorm/dataset", ReadOnly: true},
							{Name: "output", MountPath: "/tmp/agentstorm"},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "run-config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: childName(run.Name, "config")},
						}}},
						{Name: "dataset", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: run.Spec.Workload.DatasetRef.Name},
						}}},
						{Name: "output", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
				},
			},
		},
	}
}

func applyJobStatus(run *agentstormv1alpha1.AgentTestRun, job *batchv1.Job) {
	run.Status.Active = job.Status.Active
	run.Status.Succeeded = job.Status.Succeeded
	run.Status.Failed = job.Status.Failed
	if job.Status.StartTime != nil {
		run.Status.StartedAt = job.Status.StartTime.DeepCopy()
	}

	phase := agentstormv1alpha1.AgentTestRunRunning
	reason := "WorkersRunning"
	message := "worker Job is running"
	for _, condition := range job.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		switch condition.Type {
		case batchv1.JobComplete:
			phase, reason, message = agentstormv1alpha1.AgentTestRunSucceeded, "WorkersCompleted", "all worker pods completed"
		case batchv1.JobFailed:
			phase, reason, message = agentstormv1alpha1.AgentTestRunFailed, "WorkerJobFailed", condition.Message
		}
	}
	run.Status.Phase = phase
	readyStatus := metav1.ConditionFalse
	if phase == agentstormv1alpha1.AgentTestRunSucceeded {
		readyStatus = metav1.ConditionTrue
	}
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type: conditionReady, Status: readyStatus, Reason: reason, Message: message,
		ObservedGeneration: run.Generation,
	})
	if phase == agentstormv1alpha1.AgentTestRunSucceeded || phase == agentstormv1alpha1.AgentTestRunFailed {
		if job.Status.CompletionTime != nil {
			run.Status.CompletionTime = job.Status.CompletionTime.DeepCopy()
		} else if run.Status.CompletionTime == nil {
			now := metav1.Now()
			run.Status.CompletionTime = &now
		}
	}
}

func setTerminalStatus(run *agentstormv1alpha1.AgentTestRun, phase agentstormv1alpha1.AgentTestRunPhase, reason, message string) {
	run.Status.Phase = phase
	if run.Status.CompletionTime == nil {
		now := metav1.Now()
		run.Status.CompletionTime = &now
	}
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type: conditionReady, Status: metav1.ConditionFalse, Reason: reason, Message: message,
		ObservedGeneration: run.Generation,
	})
}

func (r *AgentTestRunReconciler) cancelJob(ctx context.Context, namespace, name string) error {
	job := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, job)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground))
}

func (r *AgentTestRunReconciler) patchStatus(ctx context.Context, before, run *agentstormv1alpha1.AgentTestRun) error {
	if reflect.DeepEqual(before.Status, run.Status) {
		return nil
	}
	return r.Status().Patch(ctx, run, client.MergeFrom(before))
}

func childName(name, suffix string) string {
	if len(name)+len(suffix)+1 <= 63 {
		return name + "-" + suffix
	}

	digest := sha256.Sum256([]byte(name))
	hash := fmt.Sprintf("%x", digest[:4])
	maxBase := 63 - len(suffix) - len(hash) - 2
	base := strings.TrimRight(name[:maxBase], "-")
	return base + "-" + hash + "-" + suffix
}
