package results

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
)

type repositoryStub struct {
	ready         func(context.Context) error
	registerRun   func(context.Context, string, string, string, Registration) (bool, error)
	reserveShard  func(context.Context, string, int, string, string, string, ShardSummary) (ShardReservation, error)
	finalizeShard func(context.Context, string, int, string, []CaseResult, *RunPricing) (bool, error)
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

func (s repositoryStub) ReserveShard(ctx context.Context, runID string, index int, key, hash, objectKey string, summary ShardSummary) (ShardReservation, error) {
	return s.reserveShard(ctx, runID, index, key, hash, objectKey, summary)
}

func (s repositoryStub) FinalizeShard(ctx context.Context, runID string, index int, hash string, cases []CaseResult, pricing *RunPricing) (bool, error) {
	return s.finalizeShard(ctx, runID, index, hash, cases, pricing)
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

func TestUploadShardStoresCanonicalGzipBeforeFinalizing(t *testing.T) {
	runID := "run-1"
	upload := testShard(runID, 0, "case-1", true)
	putCalled := false
	finalizeCalled := false
	var reservedHash string
	service := NewService(repositoryStub{
		reserveShard: func(_ context.Context, gotRunID string, index int, key, hash, objectKey string, summary ShardSummary) (ShardReservation, error) {
			if gotRunID != runID || index != 0 || key != "run/run-1/shard/0" {
				t.Fatalf("unexpected reservation identity: %q %d %q", gotRunID, index, key)
			}
			if objectKey != "runs/run-1/shards/0/"+hash+".json.gz" {
				t.Fatalf("unexpected object key %q", objectKey)
			}
			reservedHash = hash
			return ShardReservation{}, nil
		},
		finalizeShard: func(_ context.Context, gotRunID string, index int, hash string, cases []CaseResult, pricing *RunPricing) (bool, error) {
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
		reserveShard: func(context.Context, string, int, string, string, string, ShardSummary) (ShardReservation, error) {
			return ShardReservation{}, nil
		},
		finalizeShard: func(context.Context, string, int, string, []CaseResult, *RunPricing) (bool, error) {
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
		reserveShard: func(context.Context, string, int, string, string, string, ShardSummary) (ShardReservation, error) {
			return ShardReservation{Pricing: pricing}, nil
		},
		finalizeShard: func(_ context.Context, _ string, _ int, _ string, _ []CaseResult, got *RunPricing) (bool, error) {
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
