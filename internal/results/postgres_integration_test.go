//go:build integration

package results

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
)

func TestPostgresLifecycleAndIdempotency(t *testing.T) {
	databaseURL := os.Getenv("AGENTSTORM_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("AGENTSTORM_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()
	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	runID := fmt.Sprintf("integration-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM agentstorm_runs WHERE id = $1`, runID)
	})
	store := &captureObjectStore{}
	service := NewService(NewPostgresRepository(pool), store)
	registration := testRegistration(2)
	registration.Target.Pricing = &RunPricing{
		InputUSDPerMillionTokens:  "2",
		OutputUSDPerMillionTokens: "4",
	}
	minSuccess := 0.6
	registration.Evaluation.MinSuccessRate = &minSuccess

	created, err := service.RegisterRun(ctx, runID, "run/"+runID, registration)
	if err != nil || !created {
		t.Fatalf("register: created=%v err=%v", created, err)
	}
	created, err = service.RegisterRun(ctx, runID, "run/"+runID, registration)
	if err != nil || created {
		t.Fatalf("repeat register: created=%v err=%v", created, err)
	}
	conflicting := registration
	conflicting.ExpectedShards = 3
	if _, err := service.RegisterRun(ctx, runID, "run/"+runID, conflicting); err != ErrConflict {
		t.Fatalf("expected registration conflict, got %v", err)
	}

	first := testShard(runID, 0, "case-a", true)
	first.Cases[0].LatencyMS = 10
	first.Cases[0].InputTokens = 3
	first.Cases[0].Attempts = []AttemptResult{{
		Number: 1, LatencyMS: 10, Outcome: "succeeded", RetryDecision: "not_needed", InputTokens: 3,
		ModelCallCount: int64Pointer(2), ToolCallCount: 1,
	}}
	first.Cases[0].ModelCallCount = int64Pointer(2)
	first.Cases[0].ToolCallCount = 1
	first.Summary.InputTokens = 3
	first.Summary.ModelCallCount = int64Pointer(2)
	first.Summary.ToolCallCount = 1
	shardResult, err := service.UploadShard(ctx, runID, 0, "run/"+runID+"/shard/0", first)
	if err != nil || !shardResult.Created {
		t.Fatalf("upload first shard: created=%v err=%v", shardResult.Created, err)
	}
	shardResult, err = service.UploadShard(ctx, runID, 0, "run/"+runID+"/shard/0", first)
	if err != nil || shardResult.Created {
		t.Fatalf("repeat first shard: created=%v err=%v", shardResult.Created, err)
	}
	detail, err := service.GetRun(ctx, runID)
	if err != nil || detail.Status != RunCollecting || detail.ReceivedShards != 1 {
		t.Fatalf("collecting detail: %#v err=%v", detail, err)
	}

	second := testShard(runID, 1, "case-b", false)
	second.Cases[0].LatencyMS = 30
	second.Cases[0].OutputTokens = 5
	second.Cases[0].Attempts = []AttemptResult{{
		Number: 1, LatencyMS: 30, Outcome: "succeeded", RetryDecision: "not_needed", OutputTokens: 5,
		ModelCallCount: int64Pointer(3), ToolCallCount: 2,
	}}
	second.Cases[0].ModelCallCount = int64Pointer(3)
	second.Cases[0].ToolCallCount = 2
	second.Summary.OutputTokens = 5
	second.Summary.ModelCallCount = int64Pointer(3)
	second.Summary.ToolCallCount = 2
	second.Cases[0].Assertions[0].Message = stringPointer("sensitive detail")
	shardResult, err = service.UploadShard(ctx, runID, 1, "run/"+runID+"/shard/1", second)
	if err != nil || !shardResult.Created {
		t.Fatalf("upload second shard: created=%v err=%v", shardResult.Created, err)
	}
	detail, err = service.GetRun(ctx, runID)
	if err != nil || detail.Status != RunComplete || detail.Summary == nil {
		t.Fatalf("complete detail: %#v err=%v", detail, err)
	}
	if detail.Summary.Total != 2 || detail.Summary.Succeeded != 1 || detail.Summary.Failed != 1 {
		t.Fatalf("unexpected counts: %#v", detail.Summary)
	}
	if detail.Summary.QualityFailures != 1 || detail.Summary.QualityFailureRate != 0.5 ||
		detail.Summary.InfrastructureFailures != 0 || detail.Summary.AttemptCount != 2 ||
		detail.Summary.RetryCount != 0 || detail.Summary.InjectedFaults != 0 {
		t.Fatalf("unexpected reliability aggregate: %#v", detail.Summary)
	}
	if math.Abs(detail.Summary.P50MS-20) > 0.001 || math.Abs(detail.Summary.P95MS-29) > 0.001 {
		t.Fatalf("unexpected percentiles: %#v", detail.Summary)
	}
	if detail.Summary.ThresholdsPassed {
		t.Fatal("50% success rate should fail 60% threshold")
	}
	if detail.Summary.CostUSD == nil || *detail.Summary.CostUSD != "0.000026000000" {
		t.Fatalf("unexpected priced aggregate: %#v", detail.Summary)
	}
	if detail.Summary.ModelCallCount == nil || *detail.Summary.ModelCallCount != 5 ||
		detail.Summary.ToolCallCount != 3 || detail.Summary.ModelCallsPerSuccessfulAgent == nil ||
		*detail.Summary.ModelCallsPerSuccessfulAgent != 2 ||
		detail.Summary.ToolCallsPerSuccessfulAgent == nil || *detail.Summary.ToolCallsPerSuccessfulAgent != 1 {
		t.Fatalf("unexpected agent call aggregate: %#v", detail.Summary)
	}
	failed, err := service.ListCases(ctx, runID, "", 1, true)
	if err != nil || len(failed.Cases) != 1 || failed.Cases[0].CaseID != "case-b" {
		t.Fatalf("failed cases: %#v err=%v", failed, err)
	}
	if len(failed.Cases[0].Assertions) != 1 || failed.Cases[0].Assertions[0].ReasonCode != "mismatch" ||
		len(failed.Cases[0].ToolPath) != 1 || failed.Cases[0].ToolPath[0] != "safe.lookup" {
		t.Fatalf("structured evaluation fields were not persisted: %#v", failed.Cases[0])
	}
	firstPage, err := service.ListCases(ctx, runID, "", 1, false)
	if err != nil || len(firstPage.Cases) != 1 || firstPage.NextCursor == "" {
		t.Fatalf("first page: %#v err=%v", firstPage, err)
	}
	secondPage, err := service.ListCases(ctx, runID, firstPage.NextCursor, 1, false)
	if err != nil || len(secondPage.Cases) != 1 || secondPage.Cases[0].CaseID == firstPage.Cases[0].CaseID {
		t.Fatalf("second page: %#v err=%v", secondPage, err)
	}

	candidateID := runID + "-candidate"
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM agentstorm_runs WHERE id = $1`, candidateID)
	})
	candidateRegistration := testRegistration(1)
	candidateRegistration.Target.Pricing = &RunPricing{
		InputUSDPerMillionTokens:  "3",
		OutputUSDPerMillionTokens: "6",
	}
	if _, err := service.RegisterRun(ctx, candidateID, "run/"+candidateID, candidateRegistration); err != nil {
		t.Fatalf("register candidate: %v", err)
	}
	if _, err := service.Compare(ctx, runID, candidateID); err != ErrNotReady {
		t.Fatalf("expected incomplete comparison to fail, got %v", err)
	}
	candidateShard := testShard(candidateID, 0, "case-c", true)
	candidateShard.Cases[0].LatencyMS = 40
	candidateShard.Cases[0].InputTokens = 10
	candidateShard.Cases[0].Attempts = []AttemptResult{
		{
			Number: 1, LatencyMS: 1, Outcome: "failed", FailureCategory: "provider",
			ErrorCode: "rate_limited", InjectedRule: "first-attempt", InjectedFault: "rate_limit",
			RetryDecision: "retry_safe", ModelCallCount: int64Pointer(0),
		},
		{
			Number: 2, LatencyMS: 39, Outcome: "succeeded", RetryDecision: "not_needed", InputTokens: 10,
			ModelCallCount: int64Pointer(4), ToolCallCount: 3,
		},
	}
	candidateShard.Cases[0].ModelCallCount = int64Pointer(4)
	candidateShard.Cases[0].ToolCallCount = 3
	candidateShard.Cases = append(candidateShard.Cases, CaseResult{
		IdempotencyKey:  "run/" + candidateID + "/case/case-d/iteration/0",
		CaseID:          "case-d",
		Iteration:       0,
		Success:         false,
		LatencyMS:       40,
		FailureKind:     "provider",
		FailureCategory: "provider",
		ErrorCode:       "circuit_open",
		Attempts: []AttemptResult{{
			Number: 1, Outcome: "rejected", FailureCategory: "provider", ErrorCode: "circuit_open",
			RetryDecision: "not_retryable", CircuitEvents: []string{"reject"}, ModelCallCount: int64Pointer(0),
		}},
		ModelCallCount: int64Pointer(0),
	})
	candidateShard.Summary.Total = 2
	candidateShard.Summary.Succeeded = 1
	candidateShard.Summary.Failed = 1
	candidateShard.Summary.InputTokens = 10
	candidateShard.Summary.ModelCallCount = int64Pointer(4)
	candidateShard.Summary.ToolCallCount = 3
	if _, err := service.UploadShard(ctx, candidateID, 0, "run/"+candidateID+"/shard/0", candidateShard); err != nil {
		t.Fatalf("upload candidate: %v", err)
	}
	comparison, err := service.Compare(ctx, runID, candidateID)
	if err != nil {
		t.Fatalf("compare runs: %v", err)
	}
	if math.Abs(comparison.Delta.SuccessRate) > 0.001 ||
		math.Abs(comparison.Delta.P50MS-20) > 0.001 ||
		comparison.Delta.P50Percent == nil || math.Abs(*comparison.Delta.P50Percent-100) > 0.001 ||
		comparison.Delta.InputTokens != 7 || comparison.Delta.CostUSD == nil ||
		*comparison.Delta.CostUSD != "0.000004000000" || comparison.Delta.CostPercent == nil ||
		math.Abs(*comparison.Delta.CostPercent-15.3846153846) > 0.0001 ||
		comparison.Delta.QualityFailures != -1 || comparison.Delta.QualityFailureRate != -0.5 ||
		comparison.Delta.InfrastructureFailures != 1 || comparison.Delta.InfrastructureFailureRate != 0.5 ||
		comparison.Delta.AttemptCount != 1 || comparison.Delta.RetryCount != 1 ||
		comparison.Delta.RetriedCases != 1 || comparison.Delta.RetrySuccesses != 1 ||
		comparison.Delta.RetrySuccessRate != 1 || comparison.Delta.InjectedFaults != 1 ||
		comparison.Delta.CircuitRejections != 1 || comparison.Delta.ModelCallCount == nil ||
		*comparison.Delta.ModelCallCount != -1 || comparison.Delta.ToolCallCount != 0 ||
		comparison.Delta.ModelCallsPerSuccessfulAgent == nil ||
		*comparison.Delta.ModelCallsPerSuccessfulAgent != 2 ||
		comparison.Delta.ToolCallsPerSuccessfulAgent == nil ||
		*comparison.Delta.ToolCallsPerSuccessfulAgent != 2 {
		t.Fatalf("unexpected comparison: %#v", comparison)
	}

	if store.count() != 3 {
		t.Fatalf("expected three raw object writes, got %d", store.count())
	}
}

func stringPointer(value string) *string { return &value }
func int64Pointer(value int64) *int64    { return &value }
func permitRequestID(runID, suffix string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(runID+"\x00"+suffix)))
}

func TestMigrationUpgradeFromM3BackfillsM4Reporting(t *testing.T) {
	databaseURL := os.Getenv("AGENTSTORM_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("AGENTSTORM_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	adminPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer adminPool.Close()
	schemaName := fmt.Sprintf("migration_%d", time.Now().UnixNano())
	schemaSQL := pgx.Identifier{schemaName}.Sanitize()
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+schemaSQL); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = adminPool.Exec(context.Background(), "DROP SCHEMA "+schemaSQL+" CASCADE") })

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schemaName
	legacyPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer legacyPool.Close()
	for _, name := range []string{"001_init.sql", "002_m3_evaluation.sql"} {
		schema, err := migrations.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := legacyPool.Exec(ctx, string(schema)); err != nil {
			t.Fatalf("apply legacy migration %s: %v", name, err)
		}
	}
	if _, err := legacyPool.Exec(ctx, `
		INSERT INTO agentstorm_runs
			(id, registration_key, registration_hash, registration, expected_shards, status)
		VALUES ('legacy-run', 'run/legacy-run', 'hash', '{}', 1, 'complete');
		INSERT INTO agentstorm_case_results
			(run_id, case_id, iteration, shard_index, idempotency_key, payload_hash,
			 success, latency_ms, input_tokens, output_tokens, failure_kind)
		VALUES ('legacy-run', 'legacy-case', 0, 0,
		        'run/legacy-run/case/legacy-case/iteration/0', 'case-hash',
		        false, 1, 0, 0, 'assertion');
		INSERT INTO agentstorm_run_summaries
			(run_id, total, succeeded, failed, success_rate, failure_rate, p50_ms, p95_ms,
			 p99_ms, input_tokens, output_tokens, thresholds_passed)
		VALUES ('legacy-run', 1, 0, 1, 0, 1, 1, 1, 1, 0, 0, false);`); err != nil {
		t.Fatal(err)
	}
	if err := ApplyMigrations(ctx, legacyPool); err != nil {
		t.Fatalf("upgrade legacy schema: %v", err)
	}
	var category string
	var modelCalls *int64
	var qualityFailures, infrastructureFailures, attempts, toolCalls int64
	if err := legacyPool.QueryRow(ctx, `
		SELECT c.failure_category, s.quality_failures, s.infrastructure_failures, s.attempt_count,
		       s.model_call_count, s.tool_call_count
		FROM agentstorm_case_results c
		JOIN agentstorm_run_summaries s ON s.run_id = c.run_id
		WHERE c.run_id = 'legacy-run'`).Scan(
		&category, &qualityFailures, &infrastructureFailures, &attempts, &modelCalls, &toolCalls,
	); err != nil {
		t.Fatal(err)
	}
	if category != "evaluation" || qualityFailures != 1 || infrastructureFailures != 0 || attempts != 0 {
		t.Fatalf("legacy M3 data was not backfilled: category=%q quality=%d infrastructure=%d attempts=%d",
			category, qualityFailures, infrastructureFailures, attempts)
	}
	if modelCalls != nil || toolCalls != 0 {
		t.Fatalf("legacy call counts must remain unknown/zero: model=%v tool=%d", modelCalls, toolCalls)
	}
}

func TestTerminalRunRemainsPartialAfterLateShard(t *testing.T) {
	databaseURL := os.Getenv("AGENTSTORM_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("AGENTSTORM_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	runID := fmt.Sprintf("terminal-%d", time.Now().UnixNano())
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DELETE FROM agentstorm_runs WHERE id = $1`, runID) })
	service := NewService(NewPostgresRepository(pool), &captureObjectStore{})
	registration := testRegistration(2)
	registration.Target.Pricing = &RunPricing{
		InputUSDPerMillionTokens: "2", OutputUSDPerMillionTokens: "4",
	}
	if _, err := service.RegisterRun(ctx, runID, "run/"+runID, registration); err != nil {
		t.Fatal(err)
	}
	terminal := TerminalRequest{Status: RunCancelled, ReasonCode: "user_requested"}
	key := "run/" + runID + "/terminal/cancelled"
	result, err := service.TerminateRun(ctx, runID, key, terminal)
	if err != nil || !result.Created {
		t.Fatalf("terminal result=%#v err=%v", result, err)
	}
	result, err = service.TerminateRun(ctx, runID, key, terminal)
	if err != nil || result.Created {
		t.Fatalf("duplicate terminal result=%#v err=%v", result, err)
	}
	if _, err := service.TerminateRun(ctx, runID, key, TerminalRequest{
		Status: RunCancelled, ReasonCode: "different_reason",
	}); err != ErrConflict {
		t.Fatalf("different terminal content should conflict, got %v", err)
	}
	incomplete := false
	partial := testShard(runID, 0, "partial", true)
	partial.Cases[0].InputTokens = 7
	partial.Cases[0].UsageComplete = &incomplete
	partial.Summary.InputTokens = 7
	partial.Summary.UsageComplete = &incomplete
	if _, err := service.UploadShard(ctx, runID, 0, "run/"+runID+"/shard/0", partial); err != nil {
		t.Fatal(err)
	}
	detail, err := service.GetRun(ctx, runID)
	if err != nil || detail.Status != RunCancelled || !detail.Partial || detail.ReceivedShards != 1 ||
		detail.TerminalReason == nil || detail.TerminalReason.ReasonCode != "user_requested" {
		t.Fatalf("terminal detail=%#v err=%v", detail, err)
	}
	if detail.Summary == nil || detail.Summary.UsageComplete || detail.Summary.CostUSD != nil {
		t.Fatalf("unknown attempt usage must keep aggregate cost null: %#v", detail.Summary)
	}
	if _, err := service.Compare(ctx, runID, runID); err != ErrNotReady {
		t.Fatalf("terminal run must not be comparable, got %v", err)
	}
}

type captureObjectStore struct {
	mutex sync.Mutex
	keys  []string
}

func (s *captureObjectStore) Ready(context.Context) error { return nil }

func (s *captureObjectStore) Put(_ context.Context, key string, _ []byte, _, _ string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.keys = append(s.keys, key)
	return nil
}

func (s *captureObjectStore) count() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return len(s.keys)
}

func TestConcurrentShardFinalizationCompletesRun(t *testing.T) {
	databaseURL := os.Getenv("AGENTSTORM_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("AGENTSTORM_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()
	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	runID := fmt.Sprintf("concurrent-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM agentstorm_runs WHERE id = $1`, runID)
	})
	service := NewService(NewPostgresRepository(pool), &captureObjectStore{})
	if _, err := service.RegisterRun(ctx, runID, "run/"+runID, testRegistration(2)); err != nil {
		t.Fatalf("register run: %v", err)
	}

	errorsChannel := make(chan error, 2)
	for index, caseID := range []string{"case-a", "case-b"} {
		go func() {
			_, err := service.UploadShard(
				ctx,
				runID,
				index,
				fmt.Sprintf("run/%s/shard/%d", runID, index),
				testShard(runID, index, caseID, true),
			)
			errorsChannel <- err
		}()
	}
	for range 2 {
		if err := <-errorsChannel; err != nil {
			t.Fatalf("upload concurrent shard: %v", err)
		}
	}

	detail, err := service.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if detail.Status != RunComplete || detail.ReceivedShards != 2 || detail.Summary == nil || detail.Summary.Total != 2 {
		t.Fatalf("concurrent finalization left an incomplete run: %#v", detail)
	}
}

func TestPostgresDurableQueueAndDistributedLimits(t *testing.T) {
	databaseURL := os.Getenv("AGENTSTORM_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("AGENTSTORM_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()
	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	runID := fmt.Sprintf("queue-%d", time.Now().UnixNano())
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DELETE FROM agentstorm_runs WHERE id = $1`, runID) })
	policy := LimitPolicy{
		Global:        Limit{MaxConcurrency: 1, RequestsPerMinute: 2},
		Providers:     map[string]Limit{"fake": {MaxConcurrency: 1}},
		LeaseDuration: 30 * time.Second,
	}
	service := NewServiceWithLimitPolicy(NewPostgresRepository(pool), &captureObjectStore{}, policy)
	registration := testRegistration(2)
	registration.Scheduling = &RunScheduling{Strategy: "keda", MaxWorkers: 2, ResourceProfile: "small"}
	if _, err := service.RegisterRun(ctx, runID, "run/"+runID, registration); err != nil {
		t.Fatalf("register queued run: %v", err)
	}
	status, err := service.QueueStatus(ctx, runID)
	if err != nil || status.Pending != 2 || status.Available != 2 || status.Leased != 0 {
		t.Fatalf("initial queue status: %#v err=%v", status, err)
	}
	firstClaim, err := service.ClaimShard(ctx, runID, QueueClaimRequest{WorkerID: "worker-a"})
	if err != nil || firstClaim.ShardIndex != 0 {
		t.Fatalf("claim first shard: %#v err=%v", firstClaim, err)
	}
	rotatedClaim, err := service.ClaimShard(ctx, runID, QueueClaimRequest{WorkerID: "worker-a"})
	if err != nil || rotatedClaim.ShardIndex != 0 || rotatedClaim.LeaseToken == firstClaim.LeaseToken {
		t.Fatalf("repeat claim should rotate the same lease: %#v err=%v", rotatedClaim, err)
	}
	if _, err := service.RenewShardLease(ctx, runID, 0, QueueRenewRequest{LeaseToken: firstClaim.LeaseToken}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("old lease token should be invalid, got %v", err)
	}
	if _, err := service.RenewShardLease(ctx, runID, 0, QueueRenewRequest{LeaseToken: rotatedClaim.LeaseToken}); err != nil {
		t.Fatalf("renew active lease: %v", err)
	}

	requestA := PermitRequest{RequestID: permitRequestID(runID, "a"), WorkerID: "worker-a", Provider: "fake"}
	permitA, err := service.AcquirePermit(ctx, runID, requestA)
	if err != nil {
		t.Fatalf("acquire first permit: %v", err)
	}
	requestB := PermitRequest{RequestID: permitRequestID(runID, "b"), WorkerID: "worker-b", Provider: "fake"}
	if _, err := service.AcquirePermit(ctx, runID, requestB); !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("second permit should be limited, got %v", err)
	}
	if err := service.ReleasePermit(ctx, runID, permitA.PermitID, PermitLeaseRequest{LeaseToken: permitA.LeaseToken}); err != nil {
		t.Fatalf("release permit: %v", err)
	}
	permitB, err := service.AcquirePermit(ctx, runID, requestB)
	if err != nil {
		t.Fatalf("acquire permit after release: %v", err)
	}
	if err := service.ReleasePermit(ctx, runID, permitB.PermitID, PermitLeaseRequest{LeaseToken: permitB.LeaseToken}); err != nil {
		t.Fatalf("release second permit: %v", err)
	}
	requestC := PermitRequest{RequestID: permitRequestID(runID, "c"), WorkerID: "worker-c", Provider: "fake"}
	if _, err := service.AcquirePermit(ctx, runID, requestC); !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("third permit should be rate limited, got %v", err)
	}

	upload := testShard(runID, 0, "case-a", true)
	if _, err := service.UploadShardWithLease(ctx, runID, 0, "run/"+runID+"/shard/0", rotatedClaim.LeaseToken, upload); err != nil {
		t.Fatalf("upload leased shard: %v", err)
	}
	status, err = service.QueueStatus(ctx, runID)
	if err != nil || status.Completed != 1 || status.Pending != 1 || status.Leased != 0 {
		t.Fatalf("queue status after upload: %#v err=%v", status, err)
	}
	expiredClaim, err := service.ClaimShard(ctx, runID, QueueClaimRequest{WorkerID: "worker-c"})
	if err != nil || expiredClaim.ShardIndex != 1 {
		t.Fatalf("claim shard for expiry: %#v err=%v", expiredClaim, err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE agentstorm_shard_queue
		SET lease_expires_at = now() - interval '1 second'
		WHERE run_id = $1 AND shard_index = 1`, runID); err != nil {
		t.Fatalf("expire shard lease: %v", err)
	}
	reclaimed, err := service.ClaimShard(ctx, runID, QueueClaimRequest{WorkerID: "worker-d"})
	if err != nil || reclaimed.ShardIndex != 1 || reclaimed.LeaseToken == expiredClaim.LeaseToken {
		t.Fatalf("reclaim expired shard: %#v err=%v", reclaimed, err)
	}
	if _, err := service.RenewShardLease(ctx, runID, 1, QueueRenewRequest{LeaseToken: expiredClaim.LeaseToken}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired owner renewed reclaimed shard: %v", err)
	}
	if _, err := service.RenewShardLease(ctx, runID, 1, QueueRenewRequest{LeaseToken: reclaimed.LeaseToken}); err != nil {
		t.Fatalf("renew reclaimed shard: %v", err)
	}
	if _, err := service.TerminateRun(ctx, runID, "run/"+runID+"/terminal/cancelled", TerminalRequest{Status: RunCancelled, ReasonCode: "test_cancel"}); err != nil {
		t.Fatalf("cancel queued run: %v", err)
	}
	status, err = service.QueueStatus(ctx, runID)
	if err != nil || status.Available != 0 || status.RunStatus != RunCancelled {
		t.Fatalf("cancelled queue status: %#v err=%v", status, err)
	}
}

func TestMinIOObjectStore(t *testing.T) {
	endpoint := os.Getenv("AGENTSTORM_TEST_S3_ENDPOINT")
	accessKey := os.Getenv("AGENTSTORM_TEST_S3_ACCESS_KEY")
	secretKey := os.Getenv("AGENTSTORM_TEST_S3_SECRET_KEY")
	if endpoint == "" || accessKey == "" || secretKey == "" {
		t.Skip("MinIO integration environment is not set")
	}
	store, err := NewMinIOStore(MinIOConfig{
		Endpoint:  endpoint,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Bucket:    "agentstorm-integration",
	})
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	ctx := context.Background()
	if err := store.EnsureBucket(ctx, "us-east-1"); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}
	key := fmt.Sprintf("tests/%d.json.gz", time.Now().UnixNano())
	if err := store.Put(ctx, key, []byte("payload"), "application/json", "gzip"); err != nil {
		t.Fatalf("put object: %v", err)
	}
	if _, err := store.client.StatObject(ctx, store.bucket, key, minio.StatObjectOptions{}); err != nil {
		t.Fatalf("stat object: %v", err)
	}
	t.Cleanup(func() {
		_ = store.client.RemoveObject(context.Background(), store.bucket, key, minio.RemoveObjectOptions{})
	})
}
