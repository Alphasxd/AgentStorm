//go:build integration

package results

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"testing"
	"time"

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
	first.Summary.InputTokens = 3
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
	second.Summary.OutputTokens = 5
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
	if math.Abs(detail.Summary.P50MS-20) > 0.001 || math.Abs(detail.Summary.P95MS-29) > 0.001 {
		t.Fatalf("unexpected percentiles: %#v", detail.Summary)
	}
	if detail.Summary.ThresholdsPassed {
		t.Fatal("50% success rate should fail 60% threshold")
	}
	if detail.Summary.CostUSD == nil || *detail.Summary.CostUSD != "0.000026000000" {
		t.Fatalf("unexpected priced aggregate: %#v", detail.Summary)
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
	candidateShard.Summary.InputTokens = 10
	if _, err := service.UploadShard(ctx, candidateID, 0, "run/"+candidateID+"/shard/0", candidateShard); err != nil {
		t.Fatalf("upload candidate: %v", err)
	}
	comparison, err := service.Compare(ctx, runID, candidateID)
	if err != nil {
		t.Fatalf("compare runs: %v", err)
	}
	if math.Abs(comparison.Delta.SuccessRate-0.5) > 0.001 ||
		math.Abs(comparison.Delta.P50MS-20) > 0.001 ||
		comparison.Delta.P50Percent == nil || math.Abs(*comparison.Delta.P50Percent-100) > 0.001 ||
		comparison.Delta.InputTokens != 7 || comparison.Delta.CostUSD == nil ||
		*comparison.Delta.CostUSD != "0.000004000000" || comparison.Delta.CostPercent == nil ||
		math.Abs(*comparison.Delta.CostPercent-15.3846153846) > 0.0001 {
		t.Fatalf("unexpected comparison: %#v", comparison)
	}

	if store.count() != 3 {
		t.Fatalf("expected three raw object writes, got %d", store.count())
	}
}

func stringPointer(value string) *string { return &value }

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
