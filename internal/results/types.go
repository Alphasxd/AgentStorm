package results

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const SchemaVersion = "v1alpha1"

var (
	ErrConflict = errors.New("result already exists with different content")
	ErrNotFound = errors.New("result not found")
	ErrNotReady = errors.New("result is not complete")
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
	SchemaVersion  string     `json:"schema_version"`
	ExpectedShards int        `json:"expected_shards"`
	Source         RunSource  `json:"source"`
	Target         RunTarget  `json:"target"`
	Dataset        RunDataset `json:"dataset"`
	Evaluation     Evaluation `json:"evaluation"`
}

type RunSource struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type RunTarget struct {
	Provider string `json:"provider"`
	Model    string `json:"model,omitempty"`
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
	Total        int     `json:"total"`
	Succeeded    int     `json:"succeeded"`
	Failed       int     `json:"failed"`
	DurationMS   float64 `json:"duration_ms"`
	InputTokens  int64   `json:"input_tokens"`
	OutputTokens int64   `json:"output_tokens"`
}

type CaseResult struct {
	IdempotencyKey string  `json:"idempotency_key"`
	CaseID         string  `json:"case_id"`
	Iteration      int     `json:"iteration"`
	Success        bool    `json:"success"`
	LatencyMS      float64 `json:"latency_ms"`
	InputTokens    int64   `json:"input_tokens"`
	OutputTokens   int64   `json:"output_tokens"`
	FailureKind    string  `json:"failure_kind,omitempty"`
	Output         *string `json:"output,omitempty"`
	Error          *string `json:"error,omitempty"`
}

type RunStatus string

const (
	RunCollecting RunStatus = "collecting"
	RunComplete   RunStatus = "complete"
)

type Aggregate struct {
	Total            int64   `json:"total"`
	Succeeded        int64   `json:"succeeded"`
	Failed           int64   `json:"failed"`
	SuccessRate      float64 `json:"success_rate"`
	FailureRate      float64 `json:"failure_rate"`
	P50MS            float64 `json:"p50_ms"`
	P95MS            float64 `json:"p95_ms"`
	P99MS            float64 `json:"p99_ms"`
	InputTokens      int64   `json:"input_tokens"`
	OutputTokens     int64   `json:"output_tokens"`
	ThresholdsPassed bool    `json:"thresholds_passed"`
}

type RunDetail struct {
	ID             string       `json:"id"`
	Registration   Registration `json:"registration"`
	Status         RunStatus    `json:"status"`
	ReceivedShards int          `json:"received_shards"`
	Summary        *Aggregate   `json:"summary,omitempty"`
	CreatedAt      time.Time    `json:"created_at"`
	UpdatedAt      time.Time    `json:"updated_at"`
	CompletedAt    *time.Time   `json:"completed_at,omitempty"`
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
	SuccessRate  float64  `json:"success_rate"`
	FailureRate  float64  `json:"failure_rate"`
	P50MS        float64  `json:"p50_ms"`
	P50Percent   *float64 `json:"p50_percent"`
	P95MS        float64  `json:"p95_ms"`
	P95Percent   *float64 `json:"p95_percent"`
	P99MS        float64  `json:"p99_ms"`
	P99Percent   *float64 `json:"p99_percent"`
	InputTokens  int64    `json:"input_tokens"`
	OutputTokens int64    `json:"output_tokens"`
}

type ShardReservation struct {
	AlreadyComplete bool
}

type Repository interface {
	Ready(context.Context) error
	RegisterRun(context.Context, string, string, string, Registration) (bool, error)
	ReserveShard(context.Context, string, int, string, string, string, ShardSummary) (ShardReservation, error)
	FinalizeShard(context.Context, string, int, string, []CaseResult) (bool, error)
	GetRun(context.Context, string) (RunDetail, error)
	ListCases(context.Context, string, string, int, bool) (CasePage, error)
	Compare(context.Context, string, string) (Comparison, error)
}

type ObjectStore interface {
	Ready(context.Context) error
	Put(context.Context, string, []byte, string, string) error
}
