package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AgentTestRunPhase describes the controller-observed lifecycle of a run.
type AgentTestRunPhase string

const (
	AgentTestRunPending   AgentTestRunPhase = "Pending"
	AgentTestRunRunning   AgentTestRunPhase = "Running"
	AgentTestRunSucceeded AgentTestRunPhase = "Succeeded"
	AgentTestRunFailed    AgentTestRunPhase = "Failed"
	AgentTestRunCancelled AgentTestRunPhase = "Cancelled"
)

type AgentTargetSpec struct {
	// Provider selects a worker adapter. The built-in adapters are fake and openai-agents.
	// +kubebuilder:validation:Enum=fake;openai-agents
	Provider string `json:"provider"`
	// Model is passed to the provider. It may be empty for the fake provider.
	Model string `json:"model,omitempty"`
	// BaseURL optionally points to an OpenAI-compatible endpoint.
	BaseURL string `json:"baseURL,omitempty"`
	// APIKeySecretRef injects the selected secret value as OPENAI_API_KEY.
	APIKeySecretRef *corev1.SecretKeySelector `json:"apiKeySecretRef,omitempty"`
	// Pricing snapshots the USD price used to calculate reproducible run cost.
	Pricing *AgentPricingSpec `json:"pricing,omitempty"`
}

type AgentPricingSpec struct {
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]{0,17})(\.[0-9]{1,12})?$`
	InputUSDPerMillionTokens string `json:"inputUSDPerMillionTokens"`
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]{0,17})(\.[0-9]{1,12})?$`
	OutputUSDPerMillionTokens string `json:"outputUSDPerMillionTokens"`
}

type AgentTelemetrySpec struct {
	// ContentMode controls whether sanitized content is added to traces.
	// +kubebuilder:validation:Enum=omit;redacted
	// +kubebuilder:default=omit
	ContentMode string `json:"contentMode,omitempty"`
	// Redaction configures the sanitizer used when contentMode is redacted.
	Redaction AgentTelemetryRedactionSpec `json:"redaction,omitempty"`
}

type AgentTelemetryRedactionSpec struct {
	// Patterns are regular expressions replaced with [REDACTED].
	// +kubebuilder:validation:MaxItems=20
	Patterns []string `json:"patterns,omitempty"`
	// MetadataKeys explicitly allow selected top-level dataset metadata keys into traces.
	MetadataKeys []string `json:"metadataKeys,omitempty"`
}

type AgentWorkloadSpec struct {
	// DatasetRef points to a JSONL dataset in a ConfigMap.
	DatasetRef corev1.ConfigMapKeySelector `json:"datasetRef"`
	// Parallelism is the number of indexed Kubernetes Job pods.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	Parallelism int32 `json:"parallelism,omitempty"`
	// ConcurrencyPerWorker is the maximum number of in-flight cases in each pod.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	ConcurrencyPerWorker int32 `json:"concurrencyPerWorker,omitempty"`
	// Iterations is the number of dataset executions per worker.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	Iterations int32 `json:"iterations,omitempty"`
	// TimeoutSeconds is both the run deadline and the per-worker default timeout.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=900
	TimeoutSeconds int64 `json:"timeoutSeconds,omitempty"`
}

type AgentEvaluationSpec struct {
	// MinSuccessRate fails a worker summary below this ratio.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	MinSuccessRate *float64 `json:"minSuccessRate,omitempty"`
	// MaxErrorRate fails a worker summary above this ratio.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	MaxErrorRate *float64 `json:"maxErrorRate,omitempty"`
	// MaxP95LatencyMs fails a worker summary above this latency.
	// +kubebuilder:validation:Minimum=1
	MaxP95LatencyMs *int64 `json:"maxP95LatencyMs,omitempty"`
}

type AgentFaultScenarioRef struct {
	// Name is the ConfigMap containing the FaultScenario JSON document.
	Name string `json:"name"`
	// Key selects the FaultScenario JSON document within the ConfigMap.
	Key string `json:"key"`
}

type AgentRetrySpec struct {
	// MaxAttempts includes the initial attempt. The default preserves the pre-M4 no-retry behavior.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	// +kubebuilder:default=1
	MaxAttempts int32 `json:"maxAttempts,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=100
	InitialBackoffMs int64 `json:"initialBackoffMs,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=2000
	MaxBackoffMs int64 `json:"maxBackoffMs,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=5000
	MaxCumulativeBackoffMs int64 `json:"maxCumulativeBackoffMs,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	// +kubebuilder:default=0.2
	JitterRatio *float64 `json:"jitterRatio,omitempty"`
	// AllowAmbiguousRetries opts into retrying failures that may have completed provider or tool work.
	AllowAmbiguousRetries bool `json:"allowAmbiguousRetries,omitempty"`
}

type AgentCircuitBreakerSpec struct {
	// +kubebuilder:validation:Minimum=1
	FailureThreshold int32 `json:"failureThreshold"`
	// +kubebuilder:validation:Minimum=1
	OpenDurationMs int64 `json:"openDurationMs"`
}

type AgentReliabilitySpec struct {
	// Seed is required when scenarioRef is configured and makes fault selection reproducible.
	// +kubebuilder:validation:Minimum=0
	Seed        *int64                 `json:"seed,omitempty"`
	ScenarioRef *AgentFaultScenarioRef `json:"scenarioRef,omitempty"`
	// +kubebuilder:default={}
	Retry          AgentRetrySpec           `json:"retry,omitempty"`
	CircuitBreaker *AgentCircuitBreakerSpec `json:"circuitBreaker,omitempty"`
}

type AgentRunnerSpec struct {
	// Image is the AgentStorm worker image.
	Image string `json:"image"`
	// ImagePullPolicy controls worker image pulls.
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`
}

type AgentTestRunSpec struct {
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="target is immutable; create a new AgentTestRun"
	Target AgentTargetSpec `json:"target"`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="workload is immutable; create a new AgentTestRun"
	Workload AgentWorkloadSpec `json:"workload"`
	// +kubebuilder:default={}
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="evaluation is immutable; create a new AgentTestRun"
	Evaluation AgentEvaluationSpec `json:"evaluation"`
	// +kubebuilder:default={}
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="telemetry is immutable; create a new AgentTestRun"
	Telemetry AgentTelemetrySpec `json:"telemetry,omitempty"`
	// Reliability configures immutable fault injection, retry, and per-worker circuit breaking.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="reliability is immutable; create a new AgentTestRun"
	// +kubebuilder:validation:XValidation:rule="!has(self.scenarioRef) || has(self.seed)",message="reliability.seed is required when scenarioRef is configured"
	Reliability *AgentReliabilitySpec `json:"reliability,omitempty"`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="runner is immutable; create a new AgentTestRun"
	Runner AgentRunnerSpec `json:"runner"`
	// Cancel declaratively requests termination of the worker Job.
	Cancel bool `json:"cancel,omitempty"`
}

type AgentTestRunStatus struct {
	Phase              AgentTestRunPhase  `json:"phase,omitempty"`
	JobName            string             `json:"jobName,omitempty"`
	Active             int32              `json:"active,omitempty"`
	Succeeded          int32              `json:"succeeded,omitempty"`
	Failed             int32              `json:"failed,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	StartedAt          *metav1.Time       `json:"startedAt,omitempty"`
	CompletionTime     *metav1.Time       `json:"completionTime,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Active",type=integer,JSONPath=`.status.active`
// +kubebuilder:printcolumn:name="Succeeded",type=integer,JSONPath=`.status.succeeded`
// +kubebuilder:printcolumn:name="Failed",type=integer,JSONPath=`.status.failed`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type AgentTestRun struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AgentTestRunSpec   `json:"spec,omitempty"`
	Status AgentTestRunStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type AgentTestRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AgentTestRun `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AgentTestRun{}, &AgentTestRunList{})
}
