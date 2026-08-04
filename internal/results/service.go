package results

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/Alphasxd/AgentStorm/internal/reliability"
)

var (
	runIDPattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$`)
	pricePattern      = regexp.MustCompile(`^(0|[1-9][0-9]{0,17})(\.[0-9]{1,12})?$`)
	entrypointPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*:[A-Za-z_][A-Za-z0-9_]*$`)
	reasonCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	digestPattern     = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

type Service struct {
	repository Repository
	objects    ObjectStore
	limits     LimitPolicy
}

func NewService(repository Repository, objects ObjectStore) *Service {
	return NewServiceWithLimitPolicy(repository, objects, LimitPolicy{})
}

func NewServiceWithLimitPolicy(repository Repository, objects ObjectStore, policy LimitPolicy) *Service {
	if policy.LeaseDuration == 0 {
		policy.LeaseDuration = 30 * time.Second
	}
	if policy.Providers == nil {
		policy.Providers = map[string]Limit{}
	}
	return &Service{repository: repository, objects: objects, limits: policy}
}

func (s *Service) Ready(ctx context.Context) error {
	if err := s.repository.Ready(ctx); err != nil {
		return fmt.Errorf("database: %w", err)
	}
	if err := s.objects.Ready(ctx); err != nil {
		return fmt.Errorf("object store: %w", err)
	}
	return nil
}

func (s *Service) RegisterRun(ctx context.Context, runID, idempotencyKey string, registration Registration) (bool, error) {
	if err := validateRegistration(runID, idempotencyKey, registration); err != nil {
		return false, err
	}
	payloadHash, err := contentHash(registration)
	if err != nil {
		return false, fmt.Errorf("hash registration: %w", err)
	}
	return s.repository.RegisterRun(ctx, runID, idempotencyKey, payloadHash, registration)
}

func (s *Service) TerminateRun(ctx context.Context, runID, idempotencyKey string, terminal TerminalRequest) (TerminalResult, error) {
	if err := validateTerminal(runID, idempotencyKey, terminal); err != nil {
		return TerminalResult{}, err
	}
	payloadHash, err := contentHash(terminal)
	if err != nil {
		return TerminalResult{}, fmt.Errorf("hash terminal request: %w", err)
	}
	created, err := s.repository.TerminateRun(ctx, runID, idempotencyKey, payloadHash, terminal)
	return TerminalResult{Created: created}, err
}

func (s *Service) UploadShard(ctx context.Context, runID string, shardIndex int, idempotencyKey string, upload ShardUpload) (ShardResult, error) {
	return s.UploadShardWithLease(ctx, runID, shardIndex, idempotencyKey, "", upload)
}

func (s *Service) UploadShardWithLease(
	ctx context.Context,
	runID string,
	shardIndex int,
	idempotencyKey string,
	leaseToken string,
	upload ShardUpload,
) (ShardResult, error) {
	if err := validateShard(runID, shardIndex, idempotencyKey, upload); err != nil {
		return ShardResult{}, err
	}
	normalizeShard(&upload)

	payload, err := json.Marshal(upload)
	if err != nil {
		return ShardResult{}, fmt.Errorf("marshal shard: %w", err)
	}
	hash := sha256.Sum256(payload)
	payloadHash := hex.EncodeToString(hash[:])
	objectKey := fmt.Sprintf("runs/%s/shards/%d/%s.json.gz", url.PathEscape(runID), shardIndex, payloadHash)

	leaseHash := schedulerTokenHash(leaseToken)
	reservation, err := s.repository.ReserveShard(
		ctx, runID, shardIndex, idempotencyKey, payloadHash, objectKey, leaseHash, upload.Summary,
	)
	if err != nil {
		return ShardResult{}, err
	}
	if reservation.AlreadyComplete {
		return ShardResult{}, nil
	}

	compressed, err := gzipJSON(payload)
	if err != nil {
		return ShardResult{}, fmt.Errorf("compress shard: %w", err)
	}
	if err := s.objects.Put(ctx, objectKey, compressed, "application/json", "gzip"); err != nil {
		return ShardResult{}, fmt.Errorf("store raw shard: %w", err)
	}
	created, err := s.repository.FinalizeShard(
		ctx, runID, shardIndex, payloadHash, leaseHash, upload.Cases, reservation.Pricing,
	)
	if err != nil || !created {
		return ShardResult{}, err
	}
	var inputCost, outputCost *string
	if usageComplete(upload.Summary.UsageComplete) {
		inputCost, outputCost, _, err = costsForTokens(
			reservation.Pricing, upload.Summary.InputTokens, upload.Summary.OutputTokens,
		)
	}
	if err != nil {
		return ShardResult{}, fmt.Errorf("calculate shard cost: %w", err)
	}
	return ShardResult{Created: true, InputCostUSD: inputCost, OutputCostUSD: outputCost}, nil
}

const shardLeaseDuration = 45 * time.Second

func (s *Service) ClaimShard(ctx context.Context, runID string, request QueueClaimRequest) (QueueClaim, error) {
	if err := validateRunID(runID); err != nil {
		return QueueClaim{}, err
	}
	if strings.TrimSpace(request.WorkerID) == "" || invalidText(request.WorkerID, 253) {
		return QueueClaim{}, validationError("worker_id is required and must not exceed 253 bytes")
	}
	token, err := schedulerToken()
	if err != nil {
		return QueueClaim{}, err
	}
	index, expiresAt, err := s.repository.ClaimShard(
		ctx, runID, request.WorkerID, schedulerTokenHash(token), shardLeaseDuration,
	)
	if err != nil {
		return QueueClaim{}, err
	}
	return QueueClaim{
		ShardIndex: index, LeaseToken: token, LeaseExpiresAt: expiresAt,
		RenewAfterMS: shardLeaseDuration.Milliseconds() / 3,
	}, nil
}

func (s *Service) RenewShardLease(
	ctx context.Context,
	runID string,
	shardIndex int,
	request QueueRenewRequest,
) (QueueLease, error) {
	if err := validateRunID(runID); err != nil {
		return QueueLease{}, err
	}
	if shardIndex < 0 || strings.TrimSpace(request.LeaseToken) == "" || invalidText(request.LeaseToken, 512) {
		return QueueLease{}, validationError("valid shard index and lease_token are required")
	}
	expiresAt, err := s.repository.RenewShardLease(
		ctx, runID, shardIndex, schedulerTokenHash(request.LeaseToken), shardLeaseDuration,
	)
	if err != nil {
		return QueueLease{}, err
	}
	return QueueLease{LeaseExpiresAt: expiresAt, RenewAfterMS: shardLeaseDuration.Milliseconds() / 3}, nil
}

func (s *Service) QueueStatus(ctx context.Context, runID string) (QueueStatus, error) {
	if err := validateRunID(runID); err != nil {
		return QueueStatus{}, err
	}
	return s.repository.QueueStatus(ctx, runID)
}

func (s *Service) AcquirePermit(ctx context.Context, runID string, request PermitRequest) (PermitGrant, error) {
	if err := validateRunID(runID); err != nil {
		return PermitGrant{}, err
	}
	if !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(request.RequestID) {
		return PermitGrant{}, validationError("request_id must be a lowercase SHA-256 digest")
	}
	if strings.TrimSpace(request.WorkerID) == "" || invalidText(request.WorkerID, 253) ||
		strings.TrimSpace(request.Provider) == "" || invalidText(request.Provider, 128) {
		return PermitGrant{}, validationError("worker_id and provider are required")
	}
	token, err := schedulerToken()
	if err != nil {
		return PermitGrant{}, err
	}
	permitID, expiresAt, err := s.repository.AcquirePermit(
		ctx, runID, request, schedulerTokenHash(token), s.limits,
	)
	if err != nil {
		return PermitGrant{}, err
	}
	return PermitGrant{
		PermitID: permitID, LeaseToken: token, LeaseExpiresAt: expiresAt,
		RenewAfterMS: s.limits.LeaseDuration.Milliseconds() / 3,
	}, nil
}

func (s *Service) RenewPermit(
	ctx context.Context,
	runID string,
	permitID string,
	request PermitLeaseRequest,
) (PermitLease, error) {
	if err := validateSchedulerLease(runID, permitID, request.LeaseToken); err != nil {
		return PermitLease{}, err
	}
	expiresAt, err := s.repository.RenewPermit(
		ctx, runID, permitID, schedulerTokenHash(request.LeaseToken), s.limits.LeaseDuration,
	)
	if err != nil {
		return PermitLease{}, err
	}
	return PermitLease{LeaseExpiresAt: expiresAt, RenewAfterMS: s.limits.LeaseDuration.Milliseconds() / 3}, nil
}

func (s *Service) ReleasePermit(ctx context.Context, runID, permitID string, request PermitLeaseRequest) error {
	if err := validateSchedulerLease(runID, permitID, request.LeaseToken); err != nil {
		return err
	}
	return s.repository.ReleasePermit(ctx, runID, permitID, schedulerTokenHash(request.LeaseToken))
}

func validateSchedulerLease(runID, permitID, leaseToken string) error {
	if err := validateRunID(runID); err != nil {
		return err
	}
	if !regexp.MustCompile(`^[a-f0-9]{32}$`).MatchString(permitID) {
		return validationError("permit ID is invalid")
	}
	if strings.TrimSpace(leaseToken) == "" || invalidText(leaseToken, 512) {
		return validationError("lease_token is required")
	}
	return nil
}

func schedulerToken() (string, error) {
	payload := make([]byte, 32)
	if _, err := rand.Read(payload); err != nil {
		return "", fmt.Errorf("generate scheduler token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func schedulerTokenHash(token string) string {
	if token == "" {
		return ""
	}
	hash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(hash[:])
}

func normalizeShard(upload *ShardUpload) {
	if upload.Summary.UsageComplete == nil {
		upload.Summary.UsageComplete = boolPointer(true)
	}
	for index := range upload.Cases {
		item := &upload.Cases[index]
		if item.ToolPath == nil {
			item.ToolPath = []string{}
		}
		if item.Assertions == nil {
			item.Assertions = []AssertionResult{}
		}
		if item.Attempts == nil {
			item.Attempts = []AttemptResult{}
		}
		if item.UsageComplete == nil {
			item.UsageComplete = boolPointer(true)
		}
		for attemptIndex := range item.Attempts {
			if item.Attempts[attemptIndex].UsageComplete == nil {
				item.Attempts[attemptIndex].UsageComplete = boolPointer(true)
			}
		}
		if item.Success || item.FailureCategory != "" {
			continue
		}
		switch item.FailureKind {
		case "assertion":
			item.FailureCategory = "evaluation"
			if item.ErrorCode == "" {
				item.ErrorCode = "legacy_assertion_failure"
			}
		case "tool":
			item.FailureCategory = "tool"
			if item.ErrorCode == "" {
				item.ErrorCode = "legacy_tool_failure"
			}
		case "harness":
			item.FailureCategory = "harness"
			if item.ErrorCode == "" {
				item.ErrorCode = "legacy_harness_failure"
			}
		default:
			item.FailureCategory = "provider"
			if item.ErrorCode == "" {
				item.ErrorCode = "legacy_provider_failure"
			}
		}
	}
}

func boolPointer(value bool) *bool {
	return &value
}

func (s *Service) GetRun(ctx context.Context, runID string) (RunDetail, error) {
	if err := validateRunID(runID); err != nil {
		return RunDetail{}, err
	}
	return s.repository.GetRun(ctx, runID)
}

func (s *Service) ListCases(ctx context.Context, runID, cursor string, limit int, failedOnly bool) (CasePage, error) {
	if err := validateRunID(runID); err != nil {
		return CasePage{}, err
	}
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 500 {
		return CasePage{}, validationError("limit must be between 1 and 500")
	}
	return s.repository.ListCases(ctx, runID, cursor, limit, failedOnly)
}

func (s *Service) Compare(ctx context.Context, baselineID, candidateID string) (Comparison, error) {
	if err := validateRunID(baselineID); err != nil {
		return Comparison{}, validationErrorf("invalid baseline: %s", err)
	}
	if err := validateRunID(candidateID); err != nil {
		return Comparison{}, validationErrorf("invalid candidate: %s", err)
	}
	return s.repository.Compare(ctx, baselineID, candidateID)
}

func validateRegistration(runID, idempotencyKey string, registration Registration) error {
	if err := validateRunID(runID); err != nil {
		return err
	}
	if idempotencyKey != "run/"+runID {
		return validationError("invalid registration idempotency key")
	}
	if registration.SchemaVersion != SchemaVersion {
		return validationErrorf("schema_version must be %q", SchemaVersion)
	}
	if registration.ExpectedShards < 1 || registration.ExpectedShards > 10000 {
		return validationError("expected_shards must be between 1 and 10000")
	}
	if strings.TrimSpace(registration.Source.Namespace) == "" || strings.TrimSpace(registration.Source.Name) == "" {
		return validationError("source namespace and name are required")
	}
	if invalidText(registration.Source.Namespace, 253) || invalidText(registration.Source.Name, 253) {
		return validationError("source namespace and name must be valid text no longer than 253 bytes")
	}
	if strings.TrimSpace(registration.Target.Provider) == "" {
		return validationError("target provider is required")
	}
	if invalidText(registration.Target.Provider, 128) || invalidText(registration.Target.Model, 512) ||
		invalidText(registration.Target.AdapterEntrypoint, 256) {
		return validationError("target fields exceed their size limit or contain NUL")
	}
	if registration.Target.AdapterEntrypoint != "" && !entrypointPattern.MatchString(registration.Target.AdapterEntrypoint) {
		return validationError("target adapter_entrypoint must be a module:function reference")
	}
	if registration.Target.Pricing != nil &&
		(!pricePattern.MatchString(registration.Target.Pricing.InputUSDPerMillionTokens) ||
			!pricePattern.MatchString(registration.Target.Pricing.OutputUSDPerMillionTokens)) {
		return validationError("target pricing must contain non-negative decimal USD prices")
	}
	if strings.TrimSpace(registration.Dataset.Name) == "" || strings.TrimSpace(registration.Dataset.Key) == "" {
		return validationError("dataset name and key are required")
	}
	if invalidText(registration.Dataset.Name, 253) || invalidText(registration.Dataset.Key, 1024) {
		return validationError("dataset fields exceed their size limit or contain NUL")
	}
	for name, value := range map[string]*float64{
		"min_success_rate": registration.Evaluation.MinSuccessRate,
		"max_failure_rate": registration.Evaluation.MaxFailureRate,
	} {
		if value != nil && (!validMetric(*value) || *value < 0 || *value > 1) {
			return validationErrorf("%s must be between 0 and 1", name)
		}
	}
	if registration.Evaluation.MaxP95MS != nil && (!validMetric(*registration.Evaluation.MaxP95MS) || *registration.Evaluation.MaxP95MS < 0) {
		return validationError("max_p95_ms must not be negative")
	}
	if reliability := registration.Reliability; reliability != nil {
		if reliability.Seed != nil && *reliability.Seed < 0 {
			return validationError("reliability seed must not be negative")
		}
		retry := reliability.Retry
		if retry.MaxAttempts < 1 || retry.MaxAttempts > 10 || retry.InitialBackoffMS < 1 ||
			retry.MaxBackoffMS < retry.InitialBackoffMS || retry.MaxCumulativeBackoffMS < 1 ||
			!validMetric(retry.JitterRatio) || retry.JitterRatio < 0 || retry.JitterRatio > 1 {
			return validationError("reliability retry snapshot is invalid")
		}
		if breaker := reliability.CircuitBreaker; breaker != nil && (breaker.FailureThreshold < 1 || breaker.OpenDurationMS < 1) {
			return validationError("reliability circuit breaker snapshot is invalid")
		}
		if scenario := reliability.Scenario; scenario != nil {
			if reliability.Seed == nil || strings.TrimSpace(scenario.Source.Name) == "" ||
				strings.TrimSpace(scenario.Source.Key) == "" || !digestPattern.MatchString(scenario.Digest) {
				return validationError("reliability scenario snapshot is invalid")
			}
			canonical, _, digest, err := reliabilityScenarioDigest(scenario.Document)
			if err != nil || len(canonical) > 64*1024 || digest != scenario.Digest {
				return validationError("reliability scenario digest does not match its normalized document")
			}
		}
	}
	if scheduling := registration.Scheduling; scheduling != nil {
		if scheduling.Strategy != "indexed" && scheduling.Strategy != "keda" {
			return validationError("scheduling strategy must be indexed or keda")
		}
		if scheduling.MaxWorkers < 1 || scheduling.MaxWorkers > 1000 {
			return validationError("scheduling max_workers must be between 1 and 1000")
		}
		if scheduling.ResourceProfile != "small" && scheduling.ResourceProfile != "medium" && scheduling.ResourceProfile != "large" {
			return validationError("scheduling resource_profile is invalid")
		}
	}
	return nil
}

func validateTerminal(runID, idempotencyKey string, terminal TerminalRequest) error {
	if err := validateRunID(runID); err != nil {
		return err
	}
	if terminal.Status != RunCancelled && terminal.Status != RunHarnessFailed {
		return validationError("terminal status must be cancelled or harness_failed")
	}
	if idempotencyKey != fmt.Sprintf("run/%s/terminal/%s", runID, terminal.Status) {
		return validationError("invalid terminal idempotency key")
	}
	if !reasonCodePattern.MatchString(terminal.ReasonCode) {
		return validationError("terminal reason_code must be a stable lowercase identifier")
	}
	return nil
}

func reliabilityScenarioDigest(document reliability.FaultScenario) ([]byte, []byte, string, error) {
	payload, err := json.Marshal(document)
	if err != nil {
		return nil, nil, "", err
	}
	_, canonical, digest, err := reliability.ParseScenario(payload)
	if err != nil {
		return nil, nil, "", err
	}
	return canonical, nil, digest, nil
}

func validateShard(runID string, shardIndex int, idempotencyKey string, upload ShardUpload) error {
	if err := validateRunID(runID); err != nil {
		return err
	}
	if shardIndex < 0 {
		return validationError("shard index must not be negative")
	}
	if idempotencyKey != fmt.Sprintf("run/%s/shard/%d", runID, shardIndex) {
		return validationError("invalid shard idempotency key")
	}
	if upload.SchemaVersion != SchemaVersion {
		return validationErrorf("schema_version must be %q", SchemaVersion)
	}
	if upload.Summary.Total != len(upload.Cases) {
		return validationError("summary total does not match case count")
	}
	if upload.Summary.Succeeded+upload.Summary.Failed != upload.Summary.Total {
		return validationError("summary succeeded and failed counts do not match total")
	}
	if upload.Summary.Total < 0 || upload.Summary.Succeeded < 0 || upload.Summary.Failed < 0 ||
		!validMetric(upload.Summary.DurationMS) || upload.Summary.DurationMS < 0 ||
		upload.Summary.InputTokens < 0 || upload.Summary.OutputTokens < 0 ||
		(upload.Summary.ModelCallCount != nil && *upload.Summary.ModelCallCount < 0) ||
		upload.Summary.ToolCallCount < 0 {
		return validationError("summary contains a negative or non-finite metric")
	}
	if len(upload.Cases) > 100000 {
		return validationError("a shard cannot contain more than 100000 cases")
	}
	seen := make(map[string]struct{}, len(upload.Cases))
	var caseInputTokens, caseOutputTokens, caseModelCalls, caseToolCalls int64
	allModelCallsKnown := true
	allUsageComplete := true
	for index, item := range upload.Cases {
		if strings.TrimSpace(item.CaseID) == "" {
			return validationErrorf("cases[%d].case_id is required", index)
		}
		if invalidText(item.CaseID, 1024) {
			return validationErrorf("cases[%d].case_id exceeds 1024 bytes or contains NUL", index)
		}
		if item.Iteration < 0 {
			return validationErrorf("cases[%d].iteration must not be negative", index)
		}
		expected := fmt.Sprintf("run/%s/case/%s/iteration/%d", runID, url.QueryEscape(item.CaseID), item.Iteration)
		if item.IdempotencyKey != expected {
			return validationErrorf("cases[%d].idempotency_key is invalid", index)
		}
		identity := fmt.Sprintf("%s\x00%d", item.CaseID, item.Iteration)
		if _, ok := seen[identity]; ok {
			return validationErrorf("duplicate case %q iteration %d", item.CaseID, item.Iteration)
		}
		seen[identity] = struct{}{}
		if !validMetric(item.LatencyMS) || item.LatencyMS < 0 || item.InputTokens < 0 || item.OutputTokens < 0 ||
			(item.ModelCallCount != nil && *item.ModelCallCount < 0) || item.ToolCallCount < 0 {
			return validationErrorf("cases[%d] contains a negative metric", index)
		}
		if invalidText(item.FailureKind, 128) ||
			invalidText(item.FailureCategory, 64) || invalidText(item.ErrorCode, 128) ||
			(item.Output != nil && invalidText(*item.Output, maxRequestBytes)) ||
			(item.Error != nil && invalidText(*item.Error, maxRequestBytes)) {
			return validationErrorf("cases[%d] contains invalid text", index)
		}
		if item.InputCostUSD != nil || item.OutputCostUSD != nil || item.CostUSD != nil {
			return validationErrorf("cases[%d] cost is calculated by the Result API", index)
		}
		if item.FailureCategory != "" && !validFailureCategory(item.FailureCategory) {
			return validationErrorf("cases[%d].failure_category is invalid", index)
		}
		if item.ErrorCode != "" && !reasonCodePattern.MatchString(item.ErrorCode) {
			return validationErrorf("cases[%d].error_code is invalid", index)
		}
		var attemptInputTokens, attemptOutputTokens, attemptModelCalls, attemptToolCalls int64
		attemptModelCallsKnown := true
		attemptsUsageComplete := true
		if len(item.Attempts) > 10 {
			return validationErrorf("cases[%d].attempts cannot contain more than 10 items", index)
		}
		for attemptIndex, attempt := range item.Attempts {
			if attempt.Number != attemptIndex+1 || !validMetric(attempt.LatencyMS) || attempt.LatencyMS < 0 ||
				attempt.BackoffMS < 0 || attempt.InputTokens < 0 || attempt.OutputTokens < 0 ||
				(attempt.ModelCallCount != nil && *attempt.ModelCallCount < 0) || attempt.ToolCallCount < 0 ||
				!validAttemptOutcome(attempt.Outcome) || strings.TrimSpace(attempt.RetryDecision) == "" ||
				invalidText(attempt.RetryDecision, 64) || invalidText(attempt.InjectedRule, 128) ||
				invalidText(attempt.ErrorCode, 128) || invalidText(attempt.FailureCategory, 64) ||
				(attempt.FailureCategory != "" && !validFailureCategory(attempt.FailureCategory)) ||
				(attempt.ErrorCode != "" && !reasonCodePattern.MatchString(attempt.ErrorCode)) {
				return validationErrorf("cases[%d].attempts[%d] is invalid", index, attemptIndex)
			}
			if attempt.InjectedFault != "" && !validInjectedFault(attempt.InjectedFault) {
				return validationErrorf("cases[%d].attempts[%d].injected_fault is invalid", index, attemptIndex)
			}
			for _, event := range attempt.CircuitEvents {
				if !validCircuitEvent(event) {
					return validationErrorf("cases[%d].attempts[%d].circuit_events is invalid", index, attemptIndex)
				}
			}
			if attempt.InputTokens > math.MaxInt64-attemptInputTokens || attempt.OutputTokens > math.MaxInt64-attemptOutputTokens {
				return validationError("attempt token totals exceed the supported range")
			}
			attemptInputTokens += attempt.InputTokens
			attemptOutputTokens += attempt.OutputTokens
			if attempt.ModelCallCount == nil {
				attemptModelCallsKnown = false
			} else if *attempt.ModelCallCount > math.MaxInt64-attemptModelCalls {
				return validationError("attempt model call totals exceed the supported range")
			} else {
				attemptModelCalls += *attempt.ModelCallCount
			}
			if attempt.ToolCallCount > math.MaxInt64-attemptToolCalls {
				return validationError("attempt tool call totals exceed the supported range")
			}
			attemptToolCalls += attempt.ToolCallCount
			attemptsUsageComplete = attemptsUsageComplete && usageComplete(attempt.UsageComplete)
		}
		if len(item.Attempts) > 0 && (attemptInputTokens != item.InputTokens || attemptOutputTokens != item.OutputTokens) {
			return validationErrorf("cases[%d] token counts do not match attempt totals", index)
		}
		if len(item.Attempts) > 0 && usageComplete(item.UsageComplete) != attemptsUsageComplete {
			return validationErrorf("cases[%d].usage_complete does not match attempts", index)
		}
		if len(item.Attempts) > 0 {
			if attemptModelCallsKnown {
				if item.ModelCallCount == nil || *item.ModelCallCount != attemptModelCalls {
					return validationErrorf("cases[%d] model call count does not match attempts", index)
				}
			} else if item.ModelCallCount != nil {
				return validationErrorf("cases[%d] model call count must be unknown when any attempt is unknown", index)
			}
			if item.ToolCallCount != attemptToolCalls {
				return validationErrorf("cases[%d] tool call count does not match attempts", index)
			}
		}
		allUsageComplete = allUsageComplete && usageComplete(item.UsageComplete)
		if len(item.ToolPath) > 1000 {
			return validationErrorf("cases[%d].tool_path cannot contain more than 1000 items", index)
		}
		for toolIndex, toolName := range item.ToolPath {
			if strings.TrimSpace(toolName) == "" || invalidText(toolName, 512) {
				return validationErrorf("cases[%d].tool_path[%d] is invalid", index, toolIndex)
			}
		}
		if len(item.Assertions) > 100 {
			return validationErrorf("cases[%d].assertions cannot contain more than 100 items", index)
		}
		failedAssertion := false
		for assertionIndex, assertion := range item.Assertions {
			if assertion.Index != assertionIndex || invalidText(assertion.Type, 64) ||
				strings.TrimSpace(assertion.Type) == "" || invalidText(assertion.ReasonCode, 64) ||
				strings.TrimSpace(assertion.ReasonCode) == "" ||
				(assertion.Message != nil && invalidText(*assertion.Message, maxRequestBytes)) {
				return validationErrorf("cases[%d].assertions[%d] is invalid", index, assertionIndex)
			}
			failedAssertion = failedAssertion || !assertion.Passed
		}
		if !item.Success && strings.TrimSpace(item.FailureKind) == "" {
			return validationErrorf("cases[%d].failure_kind is required for failed results", index)
		}
		if item.Success && failedAssertion {
			return validationErrorf("cases[%d] cannot succeed with a failed assertion", index)
		}
		if item.FailureKind == "assertion" && !failedAssertion {
			return validationErrorf("cases[%d] assertion failure requires a failed assertion", index)
		}
		if item.InputTokens > math.MaxInt64-caseInputTokens ||
			item.OutputTokens > math.MaxInt64-caseOutputTokens {
			return validationError("case token totals exceed the supported range")
		}
		caseInputTokens += item.InputTokens
		caseOutputTokens += item.OutputTokens
		if item.ModelCallCount == nil {
			allModelCallsKnown = false
		} else if *item.ModelCallCount > math.MaxInt64-caseModelCalls {
			return validationError("case model call totals exceed the supported range")
		} else {
			caseModelCalls += *item.ModelCallCount
		}
		if item.ToolCallCount > math.MaxInt64-caseToolCalls {
			return validationError("case tool call totals exceed the supported range")
		}
		caseToolCalls += item.ToolCallCount
	}
	if caseInputTokens != upload.Summary.InputTokens || caseOutputTokens != upload.Summary.OutputTokens {
		return validationError("summary token counts do not match case totals")
	}
	if upload.Summary.UsageComplete != nil && *upload.Summary.UsageComplete != allUsageComplete {
		return validationError("summary usage_complete does not match cases")
	}
	if len(upload.Cases) == 0 {
		if upload.Summary.ModelCallCount != nil && *upload.Summary.ModelCallCount != 0 {
			return validationError("empty summary model call count must be zero or unknown")
		}
	} else if allModelCallsKnown {
		if upload.Summary.ModelCallCount == nil || *upload.Summary.ModelCallCount != caseModelCalls {
			return validationError("summary model call count does not match cases")
		}
	} else if upload.Summary.ModelCallCount != nil {
		return validationError("summary model call count must be unknown when any case is unknown")
	}
	if upload.Summary.ToolCallCount != caseToolCalls {
		return validationError("summary tool call count does not match cases")
	}
	return nil
}

func usageComplete(value *bool) bool {
	return value == nil || *value
}

func validFailureCategory(value string) bool {
	return value == "evaluation" || value == "provider" || value == "tool" || value == "harness"
}

func validAttemptOutcome(value string) bool {
	return value == "succeeded" || value == "failed" || value == "rejected" || value == "cancelled"
}

func validInjectedFault(value string) bool {
	switch value {
	case "latency", "timeout", "http_error", "malformed_response", "rate_limit", "tool_error":
		return true
	default:
		return false
	}
}

func validCircuitEvent(value string) bool {
	switch value {
	case "open", "reject", "half_open", "close", "reopen":
		return true
	default:
		return false
	}
}

func validateRunID(runID string) error {
	if !runIDPattern.MatchString(runID) {
		return validationError("run ID must use 1-200 letters, digits, dots, underscores, or hyphens")
	}
	return nil
}

func invalidText(value string, maxBytes int64) bool {
	return int64(len(value)) > maxBytes || strings.ContainsRune(value, '\x00')
}

func validMetric(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func contentHash(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:]), nil
}

func gzipJSON(payload []byte) ([]byte, error) {
	var output bytes.Buffer
	writer := gzip.NewWriter(&output)
	if _, err := writer.Write(payload); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}
