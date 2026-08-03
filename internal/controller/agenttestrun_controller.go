package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	agentstormv1alpha1 "github.com/Alphasxd/AgentStorm/api/v1alpha1"
	"github.com/Alphasxd/AgentStorm/internal/reliability"
	"github.com/Alphasxd/AgentStorm/internal/results"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/record"
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
	conditionTelemetry   = "TelemetryReady"
	conditionScenario    = "ScenarioReady"
	conditionScheduling  = "SchedulingReady"
	workerConfigKey      = "run.json"
)

var pricePattern = regexp.MustCompile(`^(0|[1-9][0-9]{0,17})(\.[0-9]{1,12})?$`)

type ResultSinkConfig struct {
	URL                  string
	WriteTokenSecretName string
	WriteTokenSecretKey  string
	IncludeSensitive     bool
	DistributedLimits    bool
	TimeoutSeconds       int
}

type TelemetryConfig struct {
	OTLPEndpoint  string
	AllowRedacted bool
}

func (c *TelemetryConfig) ValidateAndDefault() error {
	c.OTLPEndpoint = strings.TrimRight(strings.TrimSpace(c.OTLPEndpoint), "/")
	if c.OTLPEndpoint == "" {
		return nil
	}
	parsed, err := url.Parse(c.OTLPEndpoint)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("otel-exporter-otlp-endpoint must be an HTTP(S) URL")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("otel-exporter-otlp-endpoint must not contain credentials, query, or fragment")
	}
	return nil
}

func (c TelemetryConfig) Enabled() bool {
	return c.OTLPEndpoint != ""
}

func (c *ResultSinkConfig) ValidateAndDefault() error {
	c.URL = strings.TrimRight(strings.TrimSpace(c.URL), "/")
	if c.URL == "" {
		if c.IncludeSensitive {
			return fmt.Errorf("include-sensitive-results requires result-api-url")
		}
		if c.DistributedLimits {
			return fmt.Errorf("enable-distributed-limits requires result-api-url")
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

// AgentTestRunReconciler turns an AgentTestRun into an Indexed Job or durable KEDA ScaledJob.
type AgentTestRunReconciler struct {
	client.Client
	APIReader    client.Reader
	Scheme       *runtime.Scheme
	ResultSink   ResultSinkConfig
	Telemetry    TelemetryConfig
	Scheduler    SchedulerConfig
	ResultWriter ResultWriter
	QueueReader  QueueReader
	Recorder     record.EventRecorder
}

// +kubebuilder:rbac:groups=agentstorm.io,resources=agenttestruns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=agentstorm.io,resources=agenttestruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=agentstorm.io,resources=agenttestruns/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get
// +kubebuilder:rbac:groups=keda.sh,resources=scaledjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=resourcequotas,verbs=get;list;watch
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
	if err := validateAndDefaultWithScheduler(run, r.Scheduler); err != nil {
		setTerminalStatus(run, agentstormv1alpha1.AgentTestRunFailed, "InvalidSpec", err.Error())
		return ctrl.Result{}, r.patchStatus(ctx, before, run)
	}

	jobName := childName(run.Name, "worker")
	if run.Spec.Cancel {
		if r.ResultSink.Enabled() {
			if err := r.persistTerminal(ctx, run, results.TerminalRequest{
				Status: results.RunCancelled, ReasonCode: "cancellation_requested",
			}); err != nil {
				meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
					Type: conditionResultSink, Status: metav1.ConditionFalse, Reason: "TerminalWriteFailed",
					Message: "run cancellation could not be persisted to the durable result sink", ObservedGeneration: run.Generation,
				})
				if r.Recorder != nil {
					r.Recorder.Event(run, corev1.EventTypeWarning, "ResultTerminalWriteFailed", "durable cancellation could not be persisted")
				}
			} else {
				meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
					Type: conditionResultSink, Status: metav1.ConditionTrue, Reason: "CancelledPersisted",
					Message: "run cancellation was persisted to the durable result sink", ObservedGeneration: run.Generation,
				})
			}
		}
		if run.Spec.Scheduling.Strategy == "keda" {
			if err := r.deleteScaledJob(ctx, run); err != nil {
				return ctrl.Result{}, err
			}
			run.Status.ScaledJobName = jobName
		} else {
			if err := r.cancelJob(ctx, run.Namespace, jobName); err != nil {
				return ctrl.Result{}, err
			}
			run.Status.JobName = jobName
		}
		setTerminalStatus(run, agentstormv1alpha1.AgentTestRunCancelled, "Cancelled", "run cancellation requested")
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
	telemetryReady, reason, message := r.telemetryReady(run)
	if !telemetryReady {
		run.Status.Phase = agentstormv1alpha1.AgentTestRunPending
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
			Type: conditionTelemetry, Status: metav1.ConditionFalse, Reason: reason,
			Message: message, ObservedGeneration: run.Generation,
		})
		if err := r.patchStatus(ctx, before, run); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if run.Spec.Telemetry.ContentMode == "redacted" {
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
			Type: conditionTelemetry, Status: metav1.ConditionTrue, Reason: reason,
			Message: message, ObservedGeneration: run.Generation,
		})
	}
	scenario, scenarioReady, reason, message, err := r.scenarioSnapshot(ctx, run)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !scenarioReady {
		run.Status.Phase = agentstormv1alpha1.AgentTestRunPending
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
			Type: conditionScenario, Status: metav1.ConditionFalse, Reason: reason,
			Message: message, ObservedGeneration: run.Generation,
		})
		if err := r.patchStatus(ctx, before, run); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if run.Spec.Reliability != nil && run.Spec.Reliability.ScenarioRef != nil {
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
			Type: conditionScenario, Status: metav1.ConditionTrue, Reason: reason,
			Message: message, ObservedGeneration: run.Generation,
		})
	}
	schedulingReady, reason, message, err := r.schedulingReady(ctx, run)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !schedulingReady {
		run.Status.Phase = agentstormv1alpha1.AgentTestRunPending
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
			Type: conditionScheduling, Status: metav1.ConditionFalse, Reason: reason,
			Message: message, ObservedGeneration: run.Generation,
		})
		if err := r.patchStatus(ctx, before, run); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type: conditionScheduling, Status: metav1.ConditionTrue, Reason: reason,
		Message: message, ObservedGeneration: run.Generation,
	})
	if err := r.ensureWorkerConfig(ctx, run, scenario); err != nil {
		return ctrl.Result{}, err
	}
	if r.ResultSink.Enabled() {
		token, err := r.resultWriteToken(ctx, run)
		if err != nil {
			return ctrl.Result{}, err
		}
		writer := r.ResultWriter
		if writer == nil {
			writer = NewHTTPResultWriter(r.ResultSink)
		}
		if err := writer.Register(ctx, string(run.UID), token, buildResultRegistration(run, scenario)); err != nil {
			run.Status.Phase = agentstormv1alpha1.AgentTestRunPending
			meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
				Type: conditionResultSink, Status: metav1.ConditionFalse, Reason: "RegistrationFailed",
				Message: "run registration with the durable result sink failed", ObservedGeneration: run.Generation,
			})
			if err := r.patchStatus(ctx, before, run); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
			Type: conditionResultSink, Status: metav1.ConditionTrue, Reason: "Registered",
			Message: "run is registered with the durable result sink", ObservedGeneration: run.Generation,
		})
	}

	if run.Spec.Scheduling.Strategy == "keda" {
		scaledJob, created, err := r.ensureScaledJob(ctx, run)
		if err != nil {
			run.Status.Phase = agentstormv1alpha1.AgentTestRunPending
			meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
				Type: conditionScheduling, Status: metav1.ConditionFalse, Reason: "ScaledJobUnavailable",
				Message: "KEDA ScaledJob could not be created", ObservedGeneration: run.Generation,
			})
			if patchErr := r.patchStatus(ctx, before, run); patchErr != nil {
				return ctrl.Result{}, patchErr
			}
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		reader := r.QueueReader
		if reader == nil {
			reader = NewHTTPResultWriter(r.ResultSink)
		}
		queueStatus, err := reader.QueueStatus(ctx, string(run.UID))
		if err != nil {
			run.Status.Phase = agentstormv1alpha1.AgentTestRunPending
			meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
				Type: conditionScheduling, Status: metav1.ConditionFalse, Reason: "QueueStatusUnavailable",
				Message: "durable shard queue status is unavailable", ObservedGeneration: run.Generation,
			})
			if patchErr := r.patchStatus(ctx, before, run); patchErr != nil {
				return ctrl.Result{}, patchErr
			}
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		applyQueueStatus(run, queueStatus)
		run.Status.ScaledJobName = scaledJob.GetName()
		run.Status.JobName = ""
		run.Status.ObservedGeneration = run.Generation
		if created && run.Status.StartedAt == nil {
			now := metav1.Now()
			run.Status.StartedAt = &now
		}
		if err := r.patchStatus(ctx, before, run); err != nil {
			return ctrl.Result{}, err
		}
		if !terminalPhase(run.Status.Phase) {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		logger.Info("AgentTestRun reached terminal phase", "phase", run.Status.Phase, "scaledJob", scaledJob.GetName())
		return ctrl.Result{}, nil
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

func (r *AgentTestRunReconciler) telemetryReady(run *agentstormv1alpha1.AgentTestRun) (bool, string, string) {
	if run.Spec.Telemetry.ContentMode != "redacted" {
		return true, "ContentOmitted", "trace content is omitted"
	}
	if !r.Telemetry.Enabled() {
		return false, "EndpointMissing", "redacted telemetry requires an OTLP endpoint"
	}
	if !r.Telemetry.AllowRedacted {
		return false, "RedactedTelemetryDisabled", "redacted telemetry is disabled by the controller"
	}
	return true, "Available", "redacted telemetry is explicitly enabled"
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
	if value, exists := secret.Data[r.ResultSink.WriteTokenSecretKey]; exists && len(value) > 0 {
		return true, "Available", "result sink write-token Secret is available", nil
	}
	if value, exists := secret.StringData[r.ResultSink.WriteTokenSecretKey]; exists && strings.TrimSpace(value) != "" {
		return true, "Available", "result sink write-token Secret is available", nil
	}
	return false, "KeyMissing", "result sink write-token Secret key is not available", nil
}

func (r *AgentTestRunReconciler) resultWriteToken(ctx context.Context, run *agentstormv1alpha1.AgentTestRun) ([]byte, error) {
	secret := &corev1.Secret{}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	if err := reader.Get(ctx, types.NamespacedName{
		Namespace: run.Namespace,
		Name:      r.ResultSink.WriteTokenSecretName,
	}, secret); err != nil {
		return nil, err
	}
	if value := secret.Data[r.ResultSink.WriteTokenSecretKey]; len(value) > 0 {
		return append([]byte(nil), value...), nil
	}
	if value := secret.StringData[r.ResultSink.WriteTokenSecretKey]; strings.TrimSpace(value) != "" {
		return []byte(value), nil
	}
	return nil, fmt.Errorf("result sink write token is unavailable")
}

func (r *AgentTestRunReconciler) persistTerminal(ctx context.Context, run *agentstormv1alpha1.AgentTestRun, terminal results.TerminalRequest) error {
	token, err := r.resultWriteToken(ctx, run)
	if err != nil {
		return err
	}
	writer := r.ResultWriter
	if writer == nil {
		writer = NewHTTPResultWriter(r.ResultSink)
	}
	return writer.Terminal(ctx, string(run.UID), token, terminal)
}

func (r *AgentTestRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&agentstormv1alpha1.AgentTestRun{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.ConfigMap{}).
		Complete(r)
}

func validateAndDefault(run *agentstormv1alpha1.AgentTestRun) error {
	config := SchedulerConfig{}
	if err := config.ValidateAndDefault(); err != nil {
		return err
	}
	return validateAndDefaultWithScheduler(run, config)
}

func validateAndDefaultWithScheduler(run *agentstormv1alpha1.AgentTestRun, config SchedulerConfig) error {
	if err := config.ValidateAndDefault(); err != nil {
		return err
	}
	if run.Spec.Target.Provider != "fake" && run.Spec.Target.Provider != "openai-agents" {
		return fmt.Errorf("unsupported provider %q", run.Spec.Target.Provider)
	}
	if run.Spec.Target.Provider == "openai-agents" && strings.TrimSpace(run.Spec.Target.Model) == "" {
		return fmt.Errorf("target.model is required for provider openai-agents")
	}
	if run.Spec.Target.Pricing != nil &&
		(!pricePattern.MatchString(run.Spec.Target.Pricing.InputUSDPerMillionTokens) ||
			!pricePattern.MatchString(run.Spec.Target.Pricing.OutputUSDPerMillionTokens)) {
		return fmt.Errorf("target.pricing requires non-negative decimal USD prices")
	}
	if run.Spec.Telemetry.ContentMode == "" {
		run.Spec.Telemetry.ContentMode = "omit"
	}
	if run.Spec.Telemetry.ContentMode != "omit" && run.Spec.Telemetry.ContentMode != "redacted" {
		return fmt.Errorf("telemetry.contentMode must be omit or redacted")
	}
	if len(run.Spec.Telemetry.Redaction.Patterns) > 20 {
		return fmt.Errorf("telemetry.redaction.patterns must contain at most 20 entries")
	}
	for index, pattern := range run.Spec.Telemetry.Redaction.Patterns {
		if len([]byte(pattern)) > 256 {
			return fmt.Errorf("telemetry.redaction.patterns[%d] must be at most 256 bytes", index)
		}
		if _, err := regexp.Compile(pattern); err != nil {
			return fmt.Errorf("telemetry.redaction.patterns[%d] is invalid", index)
		}
	}
	if err := validateReliability(run.Spec.Reliability); err != nil {
		return err
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
	if run.Spec.Workload.Parallelism > 10000 {
		return fmt.Errorf("workload.parallelism must not exceed 10000")
	}
	if err := validateScheduling(run, config); err != nil {
		return err
	}
	return nil
}

func validateReliability(spec *agentstormv1alpha1.AgentReliabilitySpec) error {
	if spec == nil {
		return nil
	}
	if spec.Seed != nil && *spec.Seed < 0 {
		return fmt.Errorf("reliability.seed must be non-negative")
	}
	if spec.ScenarioRef != nil {
		if spec.Seed == nil {
			return fmt.Errorf("reliability.seed is required when scenarioRef is configured")
		}
		if strings.TrimSpace(spec.ScenarioRef.Name) == "" || len(utilvalidation.IsDNS1123Subdomain(spec.ScenarioRef.Name)) > 0 {
			return fmt.Errorf("reliability.scenarioRef.name must be a valid ConfigMap name")
		}
		if strings.TrimSpace(spec.ScenarioRef.Key) == "" || len(utilvalidation.IsConfigMapKey(spec.ScenarioRef.Key)) > 0 {
			return fmt.Errorf("reliability.scenarioRef.key must be a valid ConfigMap key")
		}
	}
	retry := &spec.Retry
	if retry.MaxAttempts == 0 {
		retry.MaxAttempts = 1
	}
	if retry.InitialBackoffMs == 0 {
		retry.InitialBackoffMs = 100
	}
	if retry.MaxBackoffMs == 0 {
		retry.MaxBackoffMs = 2000
	}
	if retry.MaxCumulativeBackoffMs == 0 {
		retry.MaxCumulativeBackoffMs = 5000
	}
	if retry.JitterRatio == nil {
		value := 0.2
		retry.JitterRatio = &value
	}
	if retry.MaxAttempts < 1 || retry.MaxAttempts > 10 {
		return fmt.Errorf("reliability.retry.maxAttempts must be between 1 and 10")
	}
	if retry.InitialBackoffMs < 1 || retry.MaxBackoffMs < 1 || retry.MaxCumulativeBackoffMs < 1 {
		return fmt.Errorf("reliability retry backoff values must be positive")
	}
	if retry.InitialBackoffMs > retry.MaxBackoffMs {
		return fmt.Errorf("reliability.retry.initialBackoffMs cannot exceed maxBackoffMs")
	}
	if *retry.JitterRatio < 0 || *retry.JitterRatio > 1 {
		return fmt.Errorf("reliability.retry.jitterRatio must be between 0 and 1")
	}
	if breaker := spec.CircuitBreaker; breaker != nil {
		if breaker.FailureThreshold < 1 || breaker.OpenDurationMs < 1 {
			return fmt.Errorf("reliability.circuitBreaker values must be positive")
		}
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
	RunID       string                                 `json:"run_id"`
	Source      workerRunSourceConfig                  `json:"source"`
	Dataset     workerDatasetConfig                    `json:"dataset"`
	Target      agentstormv1alpha1.AgentTargetSpec     `json:"target"`
	Workload    workerWorkloadConfig                   `json:"workload"`
	Evaluation  agentstormv1alpha1.AgentEvaluationSpec `json:"evaluation,omitempty"`
	Telemetry   agentstormv1alpha1.AgentTelemetrySpec  `json:"telemetry,omitempty"`
	Reliability *workerReliabilityConfig               `json:"reliability,omitempty"`
}

type workerReliabilityConfig struct {
	Seed           *int64                                      `json:"seed,omitempty"`
	Retry          agentstormv1alpha1.AgentRetrySpec           `json:"retry"`
	CircuitBreaker *agentstormv1alpha1.AgentCircuitBreakerSpec `json:"circuitBreaker,omitempty"`
	Scenario       *reliability.Snapshot                       `json:"scenario,omitempty"`
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

func (r *AgentTestRunReconciler) ensureWorkerConfig(ctx context.Context, run *agentstormv1alpha1.AgentTestRun, scenario *reliability.Snapshot) error {
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
		Telemetry:  run.Spec.Telemetry,
	}
	if run.Spec.Reliability != nil {
		config.Reliability = &workerReliabilityConfig{
			Seed:           run.Spec.Reliability.Seed,
			Retry:          run.Spec.Reliability.Retry,
			CircuitBreaker: run.Spec.Reliability.CircuitBreaker,
			Scenario:       scenario,
		}
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

func (r *AgentTestRunReconciler) scenarioSnapshot(ctx context.Context, run *agentstormv1alpha1.AgentTestRun) (*reliability.Snapshot, bool, string, string, error) {
	if run.Spec.Reliability == nil || run.Spec.Reliability.ScenarioRef == nil {
		return nil, true, "NotConfigured", "fault scenario is not configured", nil
	}

	// Once generated, the worker ConfigMap is the immutable execution snapshot. Source ConfigMap
	// changes or deletion must not alter an in-flight run.
	generated := &corev1.ConfigMap{}
	generatedKey := types.NamespacedName{Namespace: run.Namespace, Name: childName(run.Name, "config")}
	if err := r.Get(ctx, generatedKey, generated); err == nil {
		if metav1.IsControlledBy(generated, run) {
			var config workerRunConfig
			if payload := generated.Data[workerConfigKey]; payload != "" && json.Unmarshal([]byte(payload), &config) == nil && config.Reliability != nil && config.Reliability.Scenario != nil {
				return config.Reliability.Scenario, true, "Snapshotted", "fault scenario snapshot is immutable", nil
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, false, "", "", err
	}

	ref := run.Spec.Reliability.ScenarioRef
	source := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: ref.Name}, source)
	if apierrors.IsNotFound(err) {
		return nil, false, "ConfigMapMissing", "referenced fault scenario ConfigMap is not available", nil
	}
	if err != nil {
		return nil, false, "", "", err
	}
	payload, exists := source.Data[ref.Key]
	if !exists {
		if binary, binaryExists := source.BinaryData[ref.Key]; binaryExists {
			payload, exists = string(binary), true
		}
	}
	if !exists {
		return nil, false, "KeyMissing", "referenced fault scenario ConfigMap key is not available", nil
	}
	scenario, _, digest, err := reliability.ParseScenario([]byte(payload))
	if err != nil {
		return nil, false, "InvalidScenario", "referenced fault scenario failed strict validation", nil
	}
	return &reliability.Snapshot{
		SourceName: ref.Name,
		SourceKey:  ref.Key,
		Digest:     digest,
		Scenario:   scenario,
	}, true, "Available", "fault scenario is valid and ready to snapshot", nil
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

	job = buildJob(run, r.ResultSink, r.Telemetry)
	if err := controllerutil.SetControllerReference(run, job, r.Scheme); err != nil {
		return nil, false, err
	}
	if err := r.Create(ctx, job); err != nil {
		return nil, false, err
	}
	return job, true, nil
}

func buildJob(run *agentstormv1alpha1.AgentTestRun, resultSink ResultSinkConfig, telemetry TelemetryConfig) *batchv1.Job {
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
			corev1.EnvVar{Name: "AGENTSTORM_RESULT_PRE_REGISTERED", Value: "true"},
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
		if resultSink.DistributedLimits {
			env = append(env,
				corev1.EnvVar{Name: "AGENTSTORM_DISTRIBUTED_LIMITS", Value: "true"},
				corev1.EnvVar{
					Name: "AGENTSTORM_WORKER_ID",
					ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.uid",
					}},
				},
			)
		}
	}
	if telemetry.Enabled() {
		env = append(env,
			corev1.EnvVar{Name: "AGENTSTORM_OTEL_ENABLED", Value: "true"},
			corev1.EnvVar{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: telemetry.OTLPEndpoint},
			corev1.EnvVar{Name: "OTEL_EXPORTER_OTLP_PROTOCOL", Value: "http/protobuf"},
			corev1.EnvVar{Name: "OTEL_SERVICE_NAME", Value: "agentstorm-worker"},
		)
	}

	job := &batchv1.Job{
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
					RestartPolicy:                 corev1.RestartPolicyNever,
					TerminationGracePeriodSeconds: ptr.To[int64](30),
					AutomountServiceAccountToken:  ptr.To(false),
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
	applyScheduling(&job.Spec.Template.Spec, run.Spec.Scheduling)
	return job
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
