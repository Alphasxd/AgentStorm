package results

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

type repositoryStub struct {
	ready         func(context.Context) error
	registerRun   func(context.Context, string, string, string, Registration) (bool, error)
	terminateRun  func(context.Context, string, string, string, TerminalRequest) (bool, error)
	reserveShard  func(context.Context, string, int, string, string, string, string, ShardSummary) (ShardReservation, error)
	finalizeShard func(context.Context, string, int, string, string, []CaseResult, *RunPricing) (bool, error)
	claimShard    func(context.Context, string, string, string, time.Duration) (int, time.Time, error)
	renewShard    func(context.Context, string, int, string, time.Duration) (time.Time, error)
	queueStatus   func(context.Context, string) (QueueStatus, error)
	acquirePermit func(context.Context, string, PermitRequest, string, LimitPolicy) (string, time.Time, error)
	renewPermit   func(context.Context, string, string, string, time.Duration) (time.Time, error)
	releasePermit func(context.Context, string, string, string) error
	getRun        func(context.Context, string) (RunDetail, error)
	listCases     func(context.Context, string, string, int, bool) (CasePage, error)
	compare       func(context.Context, string, string) (Comparison, error)
}

func (s repositoryStub) Ready(ctx context.Context) error {
	if s.ready != nil {
		return s.ready(ctx)
	}
	return nil
}

func (s repositoryStub) RegisterRun(ctx context.Context, runID, key, hash string, registration Registration) (bool, error) {
	return s.registerRun(ctx, runID, key, hash, registration)
}

func (s repositoryStub) TerminateRun(ctx context.Context, runID, key, hash string, terminal TerminalRequest) (bool, error) {
	if s.terminateRun == nil {
		return false, nil
	}
	return s.terminateRun(ctx, runID, key, hash, terminal)
}

func (s repositoryStub) ReserveShard(ctx context.Context, runID string, index int, key, hash, objectKey, leaseHash string, summary ShardSummary) (ShardReservation, error) {
	return s.reserveShard(ctx, runID, index, key, hash, objectKey, leaseHash, summary)
}

func (s repositoryStub) FinalizeShard(ctx context.Context, runID string, index int, hash, leaseHash string, cases []CaseResult, pricing *RunPricing) (bool, error) {
	return s.finalizeShard(ctx, runID, index, hash, leaseHash, cases, pricing)
}

func (s repositoryStub) ClaimShard(ctx context.Context, runID, workerID, leaseHash string, duration time.Duration) (int, time.Time, error) {
	if s.claimShard == nil {
		return 0, time.Time{}, ErrQueueEmpty
	}
	return s.claimShard(ctx, runID, workerID, leaseHash, duration)
}

func (s repositoryStub) RenewShardLease(ctx context.Context, runID string, index int, leaseHash string, duration time.Duration) (time.Time, error) {
	if s.renewShard == nil {
		return time.Time{}, ErrLeaseLost
	}
	return s.renewShard(ctx, runID, index, leaseHash, duration)
}

func (s repositoryStub) QueueStatus(ctx context.Context, runID string) (QueueStatus, error) {
	if s.queueStatus == nil {
		return QueueStatus{}, ErrNotFound
	}
	return s.queueStatus(ctx, runID)
}

func (s repositoryStub) AcquirePermit(ctx context.Context, runID string, request PermitRequest, leaseHash string, policy LimitPolicy) (string, time.Time, error) {
	if s.acquirePermit == nil {
		return "", time.Time{}, ErrNoCapacity
	}
	return s.acquirePermit(ctx, runID, request, leaseHash, policy)
}

func (s repositoryStub) RenewPermit(ctx context.Context, runID, permitID, leaseHash string, duration time.Duration) (time.Time, error) {
	if s.renewPermit == nil {
		return time.Time{}, ErrLeaseLost
	}
	return s.renewPermit(ctx, runID, permitID, leaseHash, duration)
}

func (s repositoryStub) ReleasePermit(ctx context.Context, runID, permitID, leaseHash string) error {
	if s.releasePermit == nil {
		return ErrLeaseLost
	}
	return s.releasePermit(ctx, runID, permitID, leaseHash)
}

func (s repositoryStub) GetRun(ctx context.Context, runID string) (RunDetail, error) {
	return s.getRun(ctx, runID)
}

func (s repositoryStub) ListCases(ctx context.Context, runID, cursor string, limit int, failed bool) (CasePage, error) {
	return s.listCases(ctx, runID, cursor, limit, failed)
}

func (s repositoryStub) Compare(ctx context.Context, baseline, candidate string) (Comparison, error) {
	return s.compare(ctx, baseline, candidate)
}

type objectStoreStub struct {
	ready func(context.Context) error
	put   func(context.Context, string, []byte, string, string) error
}

func (s objectStoreStub) Ready(ctx context.Context) error {
	if s.ready != nil {
		return s.ready(ctx)
	}
	return nil
}

func (s objectStoreStub) Put(ctx context.Context, key string, payload []byte, contentType, contentEncoding string) error {
	return s.put(ctx, key, payload, contentType, contentEncoding)
}

func testRegistration(shards int) Registration {
	return Registration{
		SchemaVersion:  SchemaVersion,
		ExpectedShards: shards,
		Source:         RunSource{Namespace: "tests", Name: "run"},
		Target:         RunTarget{Provider: "fake"},
		Dataset:        RunDataset{Name: "basic", Key: "dataset.jsonl"},
	}
}

func testShard(runID string, index int, caseID string, success bool) ShardUpload {
	failureKind := ""
	var assertions []AssertionResult
	if !success {
		failureKind = "assertion"
		assertions = []AssertionResult{{
			Index: 0, Type: "contains", Passed: false, ReasonCode: "mismatch",
		}}
	}
	return ShardUpload{
		SchemaVersion: SchemaVersion,
		Summary: ShardSummary{
			Total:     1,
			Succeeded: map[bool]int{true: 1, false: 0}[success],
			Failed:    map[bool]int{true: 0, false: 1}[success],
		},
		Cases: []CaseResult{{
			IdempotencyKey: "run/" + runID + "/case/" + caseID + "/iteration/0",
			CaseID:         caseID,
			Iteration:      0,
			Success:        success,
			LatencyMS:      12.5,
			FailureKind:    failureKind,
			ToolPath:       []string{"safe.lookup"},
			Assertions:     assertions,
		}},
	}
}

func TestRegisterRunValidatesIdempotencyKey(t *testing.T) {
	repositoryCalled := false
	service := NewService(repositoryStub{
		registerRun: func(_ context.Context, runID, key, hash string, registration Registration) (bool, error) {
			repositoryCalled = true
			if runID != "run-1" || key != "run/run-1" || hash == "" {
				t.Fatalf("unexpected repository call: %q %q %q", runID, key, hash)
			}
			return true, nil
		},
	}, objectStoreStub{})

	if _, err := service.RegisterRun(context.Background(), "run-1", "wrong", testRegistration(1)); err == nil {
		t.Fatal("expected invalid idempotency key to fail")
	}
	if repositoryCalled {
		t.Fatal("repository was called for invalid registration")
	}

	created, err := service.RegisterRun(context.Background(), "run-1", "run/run-1", testRegistration(1))
	if err != nil {
		t.Fatalf("register run: %v", err)
	}
	if !created || !repositoryCalled {
		t.Fatal("valid registration was not created")
	}
}

func TestRegisterRunValidatesPriceSnapshot(t *testing.T) {
	registration := testRegistration(1)
	registration.Target.Pricing = &RunPricing{
		InputUSDPerMillionTokens:  "automatic",
		OutputUSDPerMillionTokens: "10",
	}
	if err := validateRegistration("run-1", "run/run-1", registration); err == nil {
		t.Fatal("expected invalid price snapshot to fail")
	}
	registration.Target.Pricing.InputUSDPerMillionTokens = "2.5"
	if err := validateRegistration("run-1", "run/run-1", registration); err != nil {
		t.Fatalf("valid price snapshot failed: %v", err)
	}
}

func TestTerminateRunUsesStableIdempotency(t *testing.T) {
	called := false
	service := NewService(repositoryStub{
		terminateRun: func(_ context.Context, runID, key, hash string, terminal TerminalRequest) (bool, error) {
			called = true
			if runID != "run-1" || key != "run/run-1/terminal/cancelled" || hash == "" ||
				terminal.Status != RunCancelled || terminal.ReasonCode != "user_requested" {
				t.Fatalf("unexpected terminal request: %q %q %q %#v", runID, key, hash, terminal)
			}
			return true, nil
		},
	}, objectStoreStub{})

	result, err := service.TerminateRun(context.Background(), "run-1", "run/run-1/terminal/cancelled", TerminalRequest{
		Status: RunCancelled, ReasonCode: "user_requested",
	})
	if err != nil || !result.Created || !called {
		t.Fatalf("terminal update failed: result=%#v err=%v", result, err)
	}
	if _, err := service.TerminateRun(context.Background(), "run-1", "wrong", TerminalRequest{
		Status: RunComplete, ReasonCode: "invalid reason",
	}); err == nil {
		t.Fatal("invalid terminal request should fail")
	}
}

func TestQueueClaimsAndLeaseRenewalKeepTokensOutOfRepository(t *testing.T) {
	expiresAt := time.Now().Add(time.Minute).UTC()
	var storedHash string
	service := NewService(repositoryStub{
		claimShard: func(_ context.Context, runID, workerID, leaseHash string, duration time.Duration) (int, time.Time, error) {
			if runID != "run-1" || workerID != "worker-1" || duration != shardLeaseDuration || len(leaseHash) != 64 {
				t.Fatalf("unexpected queue claim: %q %q %q %s", runID, workerID, leaseHash, duration)
			}
			storedHash = leaseHash
			return 3, expiresAt, nil
		},
		renewShard: func(_ context.Context, runID string, index int, leaseHash string, duration time.Duration) (time.Time, error) {
			if runID != "run-1" || index != 3 || leaseHash != storedHash || duration != shardLeaseDuration {
				t.Fatal("renewal did not use the claimed lease token hash")
			}
			return expiresAt, nil
		},
	}, objectStoreStub{})

	claim, err := service.ClaimShard(context.Background(), "run-1", QueueClaimRequest{WorkerID: "worker-1"})
	if err != nil || claim.ShardIndex != 3 || claim.LeaseToken == "" || claim.RenewAfterMS != 15000 {
		t.Fatalf("unexpected claim: %#v err=%v", claim, err)
	}
	if schedulerTokenHash(claim.LeaseToken) != storedHash || claim.LeaseToken == storedHash {
		t.Fatal("repository should receive only a one-way lease token hash")
	}
	lease, err := service.RenewShardLease(context.Background(), "run-1", 3, QueueRenewRequest{LeaseToken: claim.LeaseToken})
	if err != nil || !lease.LeaseExpiresAt.Equal(expiresAt) {
		t.Fatalf("renew lease: %#v err=%v", lease, err)
	}
}

func TestDistributedPermitUsesConfiguredPolicyAndStableRequestIdentity(t *testing.T) {
	expiresAt := time.Now().Add(time.Minute).UTC()
	policy := LimitPolicy{
		Global:        Limit{MaxConcurrency: 4, RequestsPerMinute: 60},
		Providers:     map[string]Limit{"fake": {MaxConcurrency: 2, RequestsPerMinute: 30}},
		LeaseDuration: 42 * time.Second,
	}
	service := NewServiceWithLimitPolicy(repositoryStub{
		acquirePermit: func(_ context.Context, runID string, request PermitRequest, leaseHash string, got LimitPolicy) (string, time.Time, error) {
			if runID != "run-1" || request.Provider != "fake" || request.WorkerID != "worker-1" ||
				request.RequestID != strings.Repeat("a", 64) || got.Global.MaxConcurrency != 4 ||
				got.Providers["fake"].RequestsPerMinute != 30 || len(leaseHash) != 64 {
				t.Fatalf("unexpected permit request: %#v %#v", request, got)
			}
			return strings.Repeat("b", 32), expiresAt, nil
		},
	}, objectStoreStub{}, policy)

	grant, err := service.AcquirePermit(context.Background(), "run-1", PermitRequest{
		RequestID: strings.Repeat("a", 64), WorkerID: "worker-1", Provider: "fake",
	})
	if err != nil || grant.PermitID != strings.Repeat("b", 32) || grant.LeaseToken == "" || grant.RenewAfterMS != 14000 {
		t.Fatalf("unexpected permit grant: %#v err=%v", grant, err)
	}
}

func TestUploadShardStoresCanonicalGzipBeforeFinalizing(t *testing.T) {
	runID := "run-1"
	upload := testShard(runID, 0, "case-1", true)
	putCalled := false
	finalizeCalled := false
	var reservedHash string
	service := NewService(repositoryStub{
		reserveShard: func(_ context.Context, gotRunID string, index int, key, hash, objectKey, leaseHash string, summary ShardSummary) (ShardReservation, error) {
			if gotRunID != runID || index != 0 || key != "run/run-1/shard/0" {
				t.Fatalf("unexpected reservation identity: %q %d %q", gotRunID, index, key)
			}
			if objectKey != "runs/run-1/shards/0/"+hash+".json.gz" {
				t.Fatalf("unexpected object key %q", objectKey)
			}
			reservedHash = hash
			return ShardReservation{}, nil
		},
		finalizeShard: func(_ context.Context, gotRunID string, index int, hash, leaseHash string, cases []CaseResult, pricing *RunPricing) (bool, error) {
			if pricing != nil {
				t.Fatal("unpriced test unexpectedly received pricing")
			}
			if !putCalled {
				t.Fatal("shard finalized before raw object was stored")
			}
			if gotRunID != runID || index != 0 || hash != reservedHash || len(cases) != 1 {
				t.Fatalf("unexpected finalization arguments")
			}
			finalizeCalled = true
			return true, nil
		},
	}, objectStoreStub{
		put: func(_ context.Context, key string, payload []byte, contentType, contentEncoding string) error {
			if contentType != "application/json" || contentEncoding != "gzip" {
				t.Fatalf("unexpected object metadata: %q %q", contentType, contentEncoding)
			}
			reader, err := gzip.NewReader(bytes.NewReader(payload))
			if err != nil {
				t.Fatalf("open gzip: %v", err)
			}
			decompressed, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("read gzip: %v", err)
			}
			var stored ShardUpload
			if err := json.Unmarshal(decompressed, &stored); err != nil {
				t.Fatalf("decode stored payload: %v", err)
			}
			if stored.Cases[0].CaseID != "case-1" || key == "" {
				t.Fatalf("unexpected stored shard: %#v", stored)
			}
			putCalled = true
			return nil
		},
	})

	result, err := service.UploadShard(context.Background(), runID, 0, "run/run-1/shard/0", upload)
	if err != nil {
		t.Fatalf("upload shard: %v", err)
	}
	if !result.Created || !putCalled || !finalizeCalled {
		t.Fatal("shard upload did not complete all persistence steps")
	}
}

func TestUploadShardDoesNotFinalizeWhenObjectWriteFails(t *testing.T) {
	service := NewService(repositoryStub{
		reserveShard: func(context.Context, string, int, string, string, string, string, ShardSummary) (ShardReservation, error) {
			return ShardReservation{}, nil
		},
		finalizeShard: func(context.Context, string, int, string, string, []CaseResult, *RunPricing) (bool, error) {
			t.Fatal("finalize should not run after object failure")
			return false, nil
		},
	}, objectStoreStub{
		put: func(context.Context, string, []byte, string, string) error {
			return errors.New("S3 unavailable")
		},
	})

	_, err := service.UploadShard(context.Background(), "run-1", 0, "run/run-1/shard/0", testShard("run-1", 0, "case-1", true))
	if err == nil {
		t.Fatal("expected object write failure")
	}
}

func TestUploadShardDerivesPricedCostFromRegisteredSnapshot(t *testing.T) {
	pricing := &RunPricing{
		InputUSDPerMillionTokens:  "2.5",
		OutputUSDPerMillionTokens: "10",
	}
	upload := testShard("run-1", 0, "case-1", true)
	upload.Cases[0].InputTokens = 1000
	upload.Cases[0].OutputTokens = 500
	upload.Summary.InputTokens = 1000
	upload.Summary.OutputTokens = 500
	service := NewService(repositoryStub{
		reserveShard: func(context.Context, string, int, string, string, string, string, ShardSummary) (ShardReservation, error) {
			return ShardReservation{Pricing: pricing}, nil
		},
		finalizeShard: func(_ context.Context, _ string, _ int, _, _ string, _ []CaseResult, got *RunPricing) (bool, error) {
			if got != pricing {
				t.Fatal("repository did not receive the registered pricing snapshot")
			}
			return true, nil
		},
	}, objectStoreStub{
		put: func(context.Context, string, []byte, string, string) error { return nil },
	})

	result, err := service.UploadShard(
		context.Background(), "run-1", 0, "run/run-1/shard/0", upload,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Created || result.InputCostUSD == nil || *result.InputCostUSD != "0.002500000000" ||
		result.OutputCostUSD == nil || *result.OutputCostUSD != "0.005000000000" {
		t.Fatalf("unexpected shard cost: %#v", result)
	}
}

func TestValidateShardRejectsDuplicateIdentity(t *testing.T) {
	upload := testShard("run-1", 0, "case-1", true)
	upload.Cases = append(upload.Cases, upload.Cases[0])
	upload.Summary.Total = 2
	upload.Summary.Succeeded = 2

	if err := validateShard("run-1", 0, "run/run-1/shard/0", upload); err == nil {
		t.Fatal("expected duplicate case identity to fail")
	}
}

func TestValidateShardChecksStructuredAssertions(t *testing.T) {
	upload := testShard("run-1", 0, "case-1", false)
	upload.Cases[0].Assertions[0].Index = 1
	if err := validateShard("run-1", 0, "run/run-1/shard/0", upload); err == nil {
		t.Fatal("expected non-canonical assertion index to fail")
	}

	upload = testShard("run-1", 0, "case-1", true)
	upload.Cases[0].Assertions = []AssertionResult{{
		Index: 0, Type: "exact", Passed: false, ReasonCode: "mismatch",
	}}
	if err := validateShard("run-1", 0, "run/run-1/shard/0", upload); err == nil {
		t.Fatal("expected successful case with failed assertion to fail")
	}
}

func TestValidateShardChecksAttemptTokensAndUsageCompleteness(t *testing.T) {
	complete := true
	incomplete := false
	upload := testShard("run-1", 0, "case-1", false)
	upload.Cases[0].FailureKind = "provider"
	upload.Cases[0].FailureCategory = "provider"
	upload.Cases[0].ErrorCode = "rate_limited"
	upload.Cases[0].Assertions = nil
	upload.Cases[0].InputTokens = 3
	upload.Cases[0].OutputTokens = 5
	upload.Cases[0].UsageComplete = &complete
	upload.Cases[0].Attempts = []AttemptResult{{
		Number: 1, LatencyMS: 2, Outcome: "failed", FailureCategory: "provider",
		ErrorCode: "rate_limited", RetryDecision: "attempt_limit", InputTokens: 3,
		OutputTokens: 5, UsageComplete: &complete,
	}}
	upload.Summary.InputTokens = 3
	upload.Summary.OutputTokens = 5
	upload.Summary.UsageComplete = &complete
	if err := validateShard("run-1", 0, "run/run-1/shard/0", upload); err != nil {
		t.Fatalf("valid attempts failed: %v", err)
	}

	upload.Cases[0].Attempts[0].UsageComplete = &incomplete
	if err := validateShard("run-1", 0, "run/run-1/shard/0", upload); err == nil {
		t.Fatal("inconsistent attempt usage completeness should fail")
	}
}

func TestUploadShardNormalizesLegacyWorkerClassification(t *testing.T) {
	service := NewService(repositoryStub{
		reserveShard: func(context.Context, string, int, string, string, string, string, ShardSummary) (ShardReservation, error) {
			return ShardReservation{}, nil
		},
		finalizeShard: func(_ context.Context, _ string, _ int, _, _ string, cases []CaseResult, _ *RunPricing) (bool, error) {
			if len(cases) != 1 || cases[0].FailureCategory != "evaluation" ||
				cases[0].ErrorCode != "legacy_assertion_failure" || !usageComplete(cases[0].UsageComplete) ||
				cases[0].Attempts == nil {
				t.Fatalf("legacy case was not normalized: %#v", cases)
			}
			return true, nil
		},
	}, objectStoreStub{put: func(context.Context, string, []byte, string, string) error { return nil }})
	if _, err := service.UploadShard(
		context.Background(), "run-1", 0, "run/run-1/shard/0", testShard("run-1", 0, "case-1", false),
	); err != nil {
		t.Fatal(err)
	}
}

func TestThresholdsAndPercentDelta(t *testing.T) {
	minSuccess := 0.9
	maxFailure := 0.1
	maxP95 := 100.0
	aggregate := Aggregate{SuccessRate: 0.9, FailureRate: 0.1, P95MS: 100}
	if !thresholdsPassed(aggregate, Evaluation{MinSuccessRate: &minSuccess, MaxFailureRate: &maxFailure, MaxP95MS: &maxP95}) {
		t.Fatal("boundary values should pass thresholds")
	}
	aggregate.P95MS = 100.1
	if thresholdsPassed(aggregate, Evaluation{MaxP95MS: &maxP95}) {
		t.Fatal("latency above threshold should fail")
	}
	if percentDelta(0, 1) != nil {
		t.Fatal("zero baseline percentage must be null")
	}
	value := percentDelta(20, 30)
	if value == nil || *value != 50 {
		t.Fatalf("unexpected percentage delta: %v", value)
	}
}
