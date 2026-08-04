package results

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Alphasxd/AgentStorm/internal/reliability"
)

const SchemaVersion = "v1alpha1"

var (
	ErrConflict   = errors.New("result already exists with different content")
	ErrNotFound   = errors.New("result not found")
	ErrNotReady   = errors.New("result is not complete")
	ErrNoCapacity = errors.New("scheduler capacity is temporarily unavailable")
	ErrLeaseLost  = errors.New("scheduler lease is no longer valid")
	ErrQueueEmpty = errors.New("no queued shard is currently available")
)

type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}

func validationError(message string) error {
	return &ValidationError{Message: message}
}

func validationErrorf(format string, arguments ...any) error {
	return &ValidationError{Message: fmt.Sprintf(format, arguments...)}
}

type Registration struct {
	SchemaVersion  string          `json:"schema_version"`
	ExpectedShards int             `json:"expected_shards"`
	Source         RunSource       `json:"source"`
	Target         RunTarget       `json:"target"`
	Dataset        RunDataset      `json:"dataset"`
	Evaluation     Evaluation      `json:"evaluation"`
	Reliability    *RunReliability `json:"reliability,omitempty"`
	Scheduling     *RunScheduling  `json:"scheduling,omitempty"`
}

// RunScheduling is the immutable scheduling snapshot registered by the controller.
// It intentionally contains no Kubernetes or Result API credentials.
type RunScheduling struct {
	Strategy        string `json:"strategy"`
	MaxWorkers      int32  `json:"max_workers"`
	ResourceProfile string `json:"resource_profile"`
}

type RunReliability struct {
	Seed           *int64                    `json:"seed,omitempty"`
	Retry          RunRetry                  `json:"retry"`
	CircuitBreaker *RunCircuitBreaker        `json:"circuit_breaker,omitempty"`
	Scenario       *RunFaultScenarioSnapshot `json:"scenario,omitempty"`
}

type RunRetry struct {
	MaxAttempts            int32   `json:"max_attempts"`
	InitialBackoffMS       int64   `json:"initial_backoff_ms"`
	MaxBackoffMS           int64   `json:"max_backoff_ms"`
	MaxCumulativeBackoffMS int64   `json:"max_cumulative_backoff_ms"`
	JitterRatio            float64 `json:"jitter_ratio"`
	AllowAmbiguousRetries  bool    `json:"allow_ambiguous_retries"`
}

type RunCircuitBreaker struct {
	FailureThreshold int32 `json:"failure_threshold"`
	OpenDurationMS   int64 `json:"open_duration_ms"`
}

type RunFaultScenarioSnapshot struct {
	Source   RunScenarioSource         `json:"source"`
	Digest   string                    `json:"digest"`
	Document reliability.FaultScenario `json:"document"`
}

type RunScenarioSource struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

type RunSource struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type RunTarget struct {
	Provider          string      `json:"provider"`
	Model             string      `json:"model,omitempty"`
	AdapterEntrypoint string      `json:"adapter_entrypoint,omitempty"`
	Pricing           *RunPricing `json:"pricing,omitempty"`
}

type RunPricing struct {
	InputUSDPerMillionTokens  string `json:"input_usd_per_million_tokens"`
	OutputUSDPerMillionTokens string `json:"output_usd_per_million_tokens"`
}

type RunDataset struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

type Evaluation struct {
	MinSuccessRate *float64 `json:"min_success_rate,omitempty"`
	MaxP95MS       *float64 `json:"max_p95_ms,omitempty"`
	MaxFailureRate *float64 `json:"max_failure_rate,omitempty"`
}

type ShardUpload struct {
	SchemaVersion string       `json:"schema_version"`
	Summary       ShardSummary `json:"summary"`
	Cases         []CaseResult `json:"cases"`
}

type ShardSummary struct {
	Total          int     `json:"total"`
	Succeeded      int     `json:"succeeded"`
	Failed         int     `json:"failed"`
	DurationMS     float64 `json:"duration_ms"`
	InputTokens    int64   `json:"input_tokens"`
	OutputTokens   int64   `json:"output_tokens"`
	ModelCallCount *int64  `json:"model_call_count"`
	ToolCallCount  int64   `json:"tool_call_count,omitempty"`
	UsageComplete  *bool   `json:"usage_complete,omitempty"`
}

type CaseResult struct {
	IdempotencyKey  string            `json:"idempotency_key"`
	CaseID          string            `json:"case_id"`
	Iteration       int               `json:"iteration"`
	Success         bool              `json:"success"`
	LatencyMS       float64           `json:"latency_ms"`
	InputTokens     int64             `json:"input_tokens"`
	OutputTokens    int64             `json:"output_tokens"`
	ModelCallCount  *int64            `json:"model_call_count"`
	ToolCallCount   int64             `json:"tool_call_count,omitempty"`
	FailureKind     string            `json:"failure_kind,omitempty"`
	FailureCategory string            `json:"failure_category,omitempty"`
	ErrorCode       string            `json:"error_code,omitempty"`
	Attempts        []AttemptResult   `json:"attempts,omitempty"`
	UsageComplete   *bool             `json:"usage_complete,omitempty"`
	Output          *string           `json:"output,omitempty"`
	Error           *string           `json:"error,omitempty"`
	ToolPath        []string          `json:"tool_path,omitempty"`
	Assertions      []AssertionResult `json:"assertions,omitempty"`
	InputCostUSD    *string           `json:"input_cost_usd"`
	OutputCostUSD   *string           `json:"output_cost_usd"`
	CostUSD         *string           `json:"cost_usd"`
}

type AttemptResult struct {
	Number          int      `json:"number"`
	LatencyMS       float64  `json:"latency_ms"`
	Outcome         string   `json:"outcome"`
	FailureCategory string   `json:"failure_category,omitempty"`
	ErrorCode       string   `json:"error_code,omitempty"`
	InjectedRule    string   `json:"injected_rule,omitempty"`
	InjectedFault   string   `json:"injected_fault,omitempty"`
	Ambiguous       bool     `json:"ambiguous"`
	RetryDecision   string   `json:"retry_decision"`
	BackoffMS       int64    `json:"backoff_ms"`
	InputTokens     int64    `json:"input_tokens"`
	OutputTokens    int64    `json:"output_tokens"`
	ModelCallCount  *int64   `json:"model_call_count"`
	ToolCallCount   int64    `json:"tool_call_count,omitempty"`
	UsageComplete   *bool    `json:"usage_complete,omitempty"`
	CircuitEvents   []string `json:"circuit_events,omitempty"`
}

type AssertionResult struct {
	Index      int     `json:"index"`
	Type       string  `json:"type"`
	Passed     bool    `json:"passed"`
	ReasonCode string  `json:"reason_code"`
	Message    *string `json:"message,omitempty"`
}

type RunStatus string

const (
	RunCollecting    RunStatus = "collecting"
	RunComplete      RunStatus = "complete"
	RunCancelled     RunStatus = "cancelled"
	RunHarnessFailed RunStatus = "harness_failed"
)

type TerminalRequest struct {
	Status     RunStatus `json:"status"`
	ReasonCode string    `json:"reason_code"`
}

type TerminalReason struct {
	Status     RunStatus `json:"status"`
	ReasonCode string    `json:"reason_code"`
	At         time.Time `json:"at"`
}

type Aggregate struct {
	Total                        int64    `json:"total"`
	Succeeded                    int64    `json:"succeeded"`
	Failed                       int64    `json:"failed"`
	SuccessRate                  float64  `json:"success_rate"`
	FailureRate                  float64  `json:"failure_rate"`
	QualityFailures              int64    `json:"quality_failures"`
	QualityFailureRate           float64  `json:"quality_failure_rate"`
	InfrastructureFailures       int64    `json:"infrastructure_failures"`
	InfrastructureFailureRate    float64  `json:"infrastructure_failure_rate"`
	AttemptCount                 int64    `json:"attempt_count"`
	RetryCount                   int64    `json:"retry_count"`
	RetriedCases                 int64    `json:"retried_cases"`
	RetrySuccesses               int64    `json:"retry_successes"`
	RetrySuccessRate             float64  `json:"retry_success_rate"`
	InjectedFaults               int64    `json:"injected_faults"`
	CircuitRejections            int64    `json:"circuit_rejections"`
	P50MS                        float64  `json:"p50_ms"`
	P95MS                        float64  `json:"p95_ms"`
	P99MS                        float64  `json:"p99_ms"`
	InputTokens                  int64    `json:"input_tokens"`
	OutputTokens                 int64    `json:"output_tokens"`
	ModelCallCount               *int64   `json:"model_call_count"`
	ToolCallCount                int64    `json:"tool_call_count"`
	ModelCallsPerSuccessfulAgent *float64 `json:"model_calls_per_successful_agent"`
	ToolCallsPerSuccessfulAgent  *float64 `json:"tool_calls_per_successful_agent"`
	InputCostUSD                 *string  `json:"input_cost_usd"`
	OutputCostUSD                *string  `json:"output_cost_usd"`
	CostUSD                      *string  `json:"cost_usd"`
	ThresholdsPassed             bool     `json:"thresholds_passed"`
	UsageComplete                bool     `json:"usage_complete"`
}

type RunDetail struct {
	ID             string          `json:"id"`
	Registration   Registration    `json:"registration"`
	Status         RunStatus       `json:"status"`
	ReceivedShards int             `json:"received_shards"`
	Summary        *Aggregate      `json:"summary,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
	Partial        bool            `json:"partial"`
	TerminalReason *TerminalReason `json:"terminal_reason,omitempty"`
}

type CasePage struct {
	Cases      []CaseResult `json:"cases"`
	NextCursor string       `json:"next_cursor,omitempty"`
}

type Comparison struct {
	BaselineID  string          `json:"baseline_id"`
	CandidateID string          `json:"candidate_id"`
	Baseline    Aggregate       `json:"baseline"`
	Candidate   Aggregate       `json:"candidate"`
	Delta       ComparisonDelta `json:"delta"`
}

type ComparisonDelta struct {
	SuccessRate                  float64  `json:"success_rate"`
	FailureRate                  float64  `json:"failure_rate"`
	QualityFailures              int64    `json:"quality_failures"`
	QualityFailureRate           float64  `json:"quality_failure_rate"`
	InfrastructureFailures       int64    `json:"infrastructure_failures"`
	InfrastructureFailureRate    float64  `json:"infrastructure_failure_rate"`
	AttemptCount                 int64    `json:"attempt_count"`
	RetryCount                   int64    `json:"retry_count"`
	RetriedCases                 int64    `json:"retried_cases"`
	RetrySuccesses               int64    `json:"retry_successes"`
	RetrySuccessRate             float64  `json:"retry_success_rate"`
	InjectedFaults               int64    `json:"injected_faults"`
	CircuitRejections            int64    `json:"circuit_rejections"`
	P50MS                        float64  `json:"p50_ms"`
	P50Percent                   *float64 `json:"p50_percent"`
	P95MS                        float64  `json:"p95_ms"`
	P95Percent                   *float64 `json:"p95_percent"`
	P99MS                        float64  `json:"p99_ms"`
	P99Percent                   *float64 `json:"p99_percent"`
	InputTokens                  int64    `json:"input_tokens"`
	OutputTokens                 int64    `json:"output_tokens"`
	ModelCallCount               *int64   `json:"model_call_count"`
	ToolCallCount                int64    `json:"tool_call_count"`
	ModelCallsPerSuccessfulAgent *float64 `json:"model_calls_per_successful_agent"`
	ToolCallsPerSuccessfulAgent  *float64 `json:"tool_calls_per_successful_agent"`
	CostUSD                      *string  `json:"cost_usd"`
	CostPercent                  *float64 `json:"cost_percent"`
}

type ShardReservation struct {
	AlreadyComplete bool
	Pricing         *RunPricing
}

type ShardResult struct {
	Created       bool
	InputCostUSD  *string
	OutputCostUSD *string
}

type TerminalResult struct {
	Created bool
}

type QueueClaimRequest struct {
	WorkerID string `json:"worker_id"`
}

type QueueClaim struct {
	ShardIndex     int       `json:"shard_index"`
	LeaseToken     string    `json:"lease_token"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
	RenewAfterMS   int64     `json:"renew_after_ms"`
}

type QueueRenewRequest struct {
	LeaseToken string `json:"lease_token"`
}

type QueueLease struct {
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
	RenewAfterMS   int64     `json:"renew_after_ms"`
}

type QueueStatus struct {
	Pending          int64     `json:"pending"`
	Available        int64     `json:"available"`
	Leased           int64     `json:"leased"`
	Completed        int64     `json:"completed"`
	Expected         int64     `json:"expected"`
	RunStatus        RunStatus `json:"run_status"`
	ThresholdsPassed *bool     `json:"thresholds_passed,omitempty"`
}

type PermitRequest struct {
	RequestID string `json:"request_id"`
	WorkerID  string `json:"worker_id"`
	Provider  string `json:"provider"`
}

type PermitGrant struct {
	PermitID       string    `json:"permit_id"`
	LeaseToken     string    `json:"lease_token"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
	RenewAfterMS   int64     `json:"renew_after_ms"`
}

type PermitLeaseRequest struct {
	LeaseToken string `json:"lease_token"`
}

type PermitLease struct {
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
	RenewAfterMS   int64     `json:"renew_after_ms"`
}

type Limit struct {
	MaxConcurrency    int `json:"max_concurrency"`
	RequestsPerMinute int `json:"requests_per_minute"`
}

type LimitPolicy struct {
	Global        Limit            `json:"global"`
	Providers     map[string]Limit `json:"providers,omitempty"`
	LeaseDuration time.Duration    `json:"-"`
}

type CapacityError struct {
	RetryAfter time.Duration
}

func (e *CapacityError) Error() string { return ErrNoCapacity.Error() }

func (e *CapacityError) Unwrap() error { return ErrNoCapacity }

type Repository interface {
	Ready(context.Context) error
	RegisterRun(context.Context, string, string, string, Registration) (bool, error)
	TerminateRun(context.Context, string, string, string, TerminalRequest) (bool, error)
	ReserveShard(context.Context, string, int, string, string, string, string, ShardSummary) (ShardReservation, error)
	FinalizeShard(context.Context, string, int, string, string, []CaseResult, *RunPricing) (bool, error)
	ClaimShard(context.Context, string, string, string, time.Duration) (int, time.Time, error)
	RenewShardLease(context.Context, string, int, string, time.Duration) (time.Time, error)
	QueueStatus(context.Context, string) (QueueStatus, error)
	AcquirePermit(context.Context, string, PermitRequest, string, LimitPolicy) (string, time.Time, error)
	RenewPermit(context.Context, string, string, string, time.Duration) (time.Time, error)
	ReleasePermit(context.Context, string, string, string) error
	GetRun(context.Context, string) (RunDetail, error)
	ListCases(context.Context, string, string, int, bool) (CasePage, error)
	Compare(context.Context, string, string) (Comparison, error)
}

type ObjectStore interface {
	Ready(context.Context) error
	Put(context.Context, string, []byte, string, string) error
}
