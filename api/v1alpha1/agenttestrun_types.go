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
