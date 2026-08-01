package results

import (
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrations embed.FS

const migrationLockID int64 = 652206314814

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	connection, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer connection.Release()

	if _, err := connection.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockID); err != nil {
		return fmt.Errorf("lock migrations: %w", err)
	}
	defer func() {
		_, _ = connection.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", migrationLockID)
	}()

	schema, err := migrations.ReadFile("migrations/001_init.sql")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	if _, err := connection.Exec(ctx, string(schema)); err != nil {
		return fmt.Errorf("apply migration 001_init: %w", err)
	}
	return nil
}

func (r *PostgresRepository) Ready(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

func (r *PostgresRepository) RegisterRun(ctx context.Context, runID, key, hash string, registration Registration) (bool, error) {
	payload, err := json.Marshal(registration)
	if err != nil {
		return false, fmt.Errorf("marshal registration: %w", err)
	}
	var inserted string
	err = r.pool.QueryRow(ctx, `
		INSERT INTO agentstorm_runs (id, registration_key, registration_hash, registration, expected_shards)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING
		RETURNING id`, runID, key, hash, payload, registration.ExpectedShards).Scan(&inserted)
	if err == nil {
		return true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("register run: %w", err)
	}

	var existingID, existingKey, existingHash string
	err = r.pool.QueryRow(ctx, `
		SELECT id, registration_key, registration_hash
		FROM agentstorm_runs
		WHERE id = $1 OR registration_key = $2`, runID, key).Scan(&existingID, &existingKey, &existingHash)
	if err != nil {
		return false, fmt.Errorf("read conflicting registration: %w", err)
	}
	if existingID != runID || existingKey != key || existingHash != hash {
		return false, ErrConflict
	}
	return false, nil
}

func (r *PostgresRepository) ReserveShard(
	ctx context.Context,
	runID string,
	shardIndex int,
	key string,
	hash string,
	objectKey string,
	summary ShardSummary,
) (ShardReservation, error) {
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		return ShardReservation{}, fmt.Errorf("begin shard reservation: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }()

	var expectedShards int
	if err := transaction.QueryRow(ctx, `SELECT expected_shards FROM agentstorm_runs WHERE id = $1 FOR UPDATE`, runID).Scan(&expectedShards); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ShardReservation{}, ErrNotFound
		}
		return ShardReservation{}, fmt.Errorf("read run for shard: %w", err)
	}
	if shardIndex >= expectedShards {
		return ShardReservation{}, validationErrorf("shard index %d is outside expected range [0,%d)", shardIndex, expectedShards)
	}

	summaryPayload, err := json.Marshal(summary)
	if err != nil {
		return ShardReservation{}, fmt.Errorf("marshal shard summary: %w", err)
	}
	var inserted int
	err = transaction.QueryRow(ctx, `
		INSERT INTO agentstorm_shard_receipts
			(run_id, shard_index, idempotency_key, payload_hash, object_key, summary)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT DO NOTHING
		RETURNING shard_index`, runID, shardIndex, key, hash, objectKey, summaryPayload).Scan(&inserted)
	if err == nil {
		if err := transaction.Commit(ctx); err != nil {
			return ShardReservation{}, fmt.Errorf("commit shard reservation: %w", err)
		}
		return ShardReservation{}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ShardReservation{}, fmt.Errorf("reserve shard: %w", err)
	}

	var existingKey, existingHash, existingStatus string
	err = transaction.QueryRow(ctx, `
		SELECT idempotency_key, payload_hash, status
		FROM agentstorm_shard_receipts
		WHERE run_id = $1 AND shard_index = $2`, runID, shardIndex).Scan(&existingKey, &existingHash, &existingStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ShardReservation{}, ErrConflict
		}
		return ShardReservation{}, fmt.Errorf("read shard reservation: %w", err)
	}
	if existingKey != key || existingHash != hash {
		return ShardReservation{}, ErrConflict
	}
	if err := transaction.Commit(ctx); err != nil {
		return ShardReservation{}, fmt.Errorf("commit existing shard reservation: %w", err)
	}
	return ShardReservation{AlreadyComplete: existingStatus == "complete"}, nil
}

func (r *PostgresRepository) FinalizeShard(ctx context.Context, runID string, shardIndex int, hash string, cases []CaseResult) (bool, error) {
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin shard finalization: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }()

	var lockedRunID string
	if err := transaction.QueryRow(ctx, `
		SELECT id
		FROM agentstorm_runs
		WHERE id = $1
		FOR UPDATE`, runID).Scan(&lockedRunID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, ErrNotFound
		}
		return false, fmt.Errorf("lock run for shard finalization: %w", err)
	}

	var receiptHash, receiptStatus string
	if err := transaction.QueryRow(ctx, `
		SELECT payload_hash, status
		FROM agentstorm_shard_receipts
		WHERE run_id = $1 AND shard_index = $2
		FOR UPDATE`, runID, shardIndex).Scan(&receiptHash, &receiptStatus); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, ErrNotFound
		}
		return false, fmt.Errorf("lock shard receipt: %w", err)
	}
	if receiptHash != hash {
		return false, ErrConflict
	}
	if receiptStatus == "complete" {
		if err := transaction.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit duplicate shard: %w", err)
		}
		return false, nil
	}

	for _, item := range cases {
		itemHash, err := contentHash(item)
		if err != nil {
			return false, fmt.Errorf("hash case result: %w", err)
		}
		var inserted string
		err = transaction.QueryRow(ctx, `
			INSERT INTO agentstorm_case_results
				(run_id, case_id, iteration, shard_index, idempotency_key, payload_hash,
				 success, latency_ms, input_tokens, output_tokens, failure_kind, output, error)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
			ON CONFLICT DO NOTHING
			RETURNING case_id`, runID, item.CaseID, item.Iteration, shardIndex, item.IdempotencyKey,
			itemHash, item.Success, item.LatencyMS, item.InputTokens, item.OutputTokens,
			item.FailureKind, item.Output, item.Error).Scan(&inserted)
		if err == nil {
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return false, fmt.Errorf("insert case result: %w", err)
		}

		var existingKey, existingHash string
		err = transaction.QueryRow(ctx, `
			SELECT idempotency_key, payload_hash
			FROM agentstorm_case_results
			WHERE run_id = $1 AND case_id = $2 AND iteration = $3`, runID, item.CaseID, item.Iteration).Scan(&existingKey, &existingHash)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return false, ErrConflict
			}
			return false, fmt.Errorf("read conflicting case result: %w", err)
		}
		if existingKey != item.IdempotencyKey || existingHash != itemHash {
			return false, ErrConflict
		}
	}

	if _, err := transaction.Exec(ctx, `
		UPDATE agentstorm_shard_receipts
		SET status = 'complete', updated_at = now()
		WHERE run_id = $1 AND shard_index = $2`, runID, shardIndex); err != nil {
		return false, fmt.Errorf("complete shard receipt: %w", err)
	}
	if err := refreshAggregate(ctx, transaction, runID); err != nil {
		return false, err
	}
	if err := transaction.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit shard finalization: %w", err)
	}
	return true, nil
}

func refreshAggregate(ctx context.Context, transaction pgx.Tx, runID string) error {
	var aggregate Aggregate
	if err := transaction.QueryRow(ctx, `
		SELECT
			count(*),
			count(*) FILTER (WHERE success),
			count(*) FILTER (WHERE NOT success),
			COALESCE(percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ms), 0),
			COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms), 0),
			COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms), 0),
			COALESCE(sum(input_tokens), 0),
			COALESCE(sum(output_tokens), 0)
		FROM agentstorm_case_results
		WHERE run_id = $1`, runID).Scan(
		&aggregate.Total,
		&aggregate.Succeeded,
		&aggregate.Failed,
		&aggregate.P50MS,
		&aggregate.P95MS,
		&aggregate.P99MS,
		&aggregate.InputTokens,
		&aggregate.OutputTokens,
	); err != nil {
		return fmt.Errorf("aggregate run: %w", err)
	}
	if aggregate.Total > 0 {
		aggregate.SuccessRate = float64(aggregate.Succeeded) / float64(aggregate.Total)
		aggregate.FailureRate = float64(aggregate.Failed) / float64(aggregate.Total)
	}

	var registrationPayload []byte
	var expectedShards, completedShards int
	if err := transaction.QueryRow(ctx, `
		SELECT r.registration, r.expected_shards,
		       count(s.shard_index) FILTER (WHERE s.status = 'complete')
		FROM agentstorm_runs r
		LEFT JOIN agentstorm_shard_receipts s ON s.run_id = r.id
		WHERE r.id = $1
		GROUP BY r.id`, runID).Scan(&registrationPayload, &expectedShards, &completedShards); err != nil {
		return fmt.Errorf("read run thresholds: %w", err)
	}
	var registration Registration
	if err := json.Unmarshal(registrationPayload, &registration); err != nil {
		return fmt.Errorf("decode run registration: %w", err)
	}
	aggregate.ThresholdsPassed = thresholdsPassed(aggregate, registration.Evaluation)

	if _, err := transaction.Exec(ctx, `
		INSERT INTO agentstorm_run_summaries
			(run_id, total, succeeded, failed, success_rate, failure_rate, p50_ms, p95_ms,
			 p99_ms, input_tokens, output_tokens, thresholds_passed)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (run_id) DO UPDATE SET
			total = EXCLUDED.total,
			succeeded = EXCLUDED.succeeded,
			failed = EXCLUDED.failed,
			success_rate = EXCLUDED.success_rate,
			failure_rate = EXCLUDED.failure_rate,
			p50_ms = EXCLUDED.p50_ms,
			p95_ms = EXCLUDED.p95_ms,
			p99_ms = EXCLUDED.p99_ms,
			input_tokens = EXCLUDED.input_tokens,
			output_tokens = EXCLUDED.output_tokens,
			thresholds_passed = EXCLUDED.thresholds_passed,
			updated_at = now()`, runID, aggregate.Total, aggregate.Succeeded, aggregate.Failed,
		aggregate.SuccessRate, aggregate.FailureRate, aggregate.P50MS, aggregate.P95MS,
		aggregate.P99MS, aggregate.InputTokens, aggregate.OutputTokens, aggregate.ThresholdsPassed); err != nil {
		return fmt.Errorf("persist run aggregate: %w", err)
	}

	status := RunCollecting
	var completedAt any
	if completedShards == expectedShards {
		status = RunComplete
		completedAt = time.Now().UTC()
	}
	if _, err := transaction.Exec(ctx, `
		UPDATE agentstorm_runs
		SET status = $2, updated_at = now(),
		    completed_at = CASE WHEN $2 = 'complete' THEN COALESCE(completed_at, $3) ELSE completed_at END
		WHERE id = $1`, runID, status, completedAt); err != nil {
		return fmt.Errorf("update run status: %w", err)
	}
	return nil
}

func thresholdsPassed(aggregate Aggregate, evaluation Evaluation) bool {
	if evaluation.MinSuccessRate != nil && aggregate.SuccessRate < *evaluation.MinSuccessRate {
		return false
	}
	if evaluation.MaxFailureRate != nil && aggregate.FailureRate > *evaluation.MaxFailureRate {
		return false
	}
	if evaluation.MaxP95MS != nil && aggregate.P95MS > *evaluation.MaxP95MS {
		return false
	}
	return true
}

func (r *PostgresRepository) GetRun(ctx context.Context, runID string) (RunDetail, error) {
	var detail RunDetail
	var registrationPayload []byte
	var summary Aggregate
	var summaryPresent bool
	err := r.pool.QueryRow(ctx, `
		SELECT r.id, r.registration, r.status,
		       count(sr.shard_index) FILTER (WHERE sr.status = 'complete'),
		       r.created_at, r.updated_at, r.completed_at,
		       (rs.run_id IS NOT NULL),
		       COALESCE(rs.total, 0), COALESCE(rs.succeeded, 0), COALESCE(rs.failed, 0),
		       COALESCE(rs.success_rate, 0), COALESCE(rs.failure_rate, 0),
		       COALESCE(rs.p50_ms, 0), COALESCE(rs.p95_ms, 0), COALESCE(rs.p99_ms, 0),
		       COALESCE(rs.input_tokens, 0), COALESCE(rs.output_tokens, 0),
		       COALESCE(rs.thresholds_passed, false)
		FROM agentstorm_runs r
		LEFT JOIN agentstorm_shard_receipts sr ON sr.run_id = r.id
		LEFT JOIN agentstorm_run_summaries rs ON rs.run_id = r.id
		WHERE r.id = $1
		GROUP BY r.id, rs.run_id`, runID).Scan(
		&detail.ID,
		&registrationPayload,
		&detail.Status,
		&detail.ReceivedShards,
		&detail.CreatedAt,
		&detail.UpdatedAt,
		&detail.CompletedAt,
		&summaryPresent,
		&summary.Total,
		&summary.Succeeded,
		&summary.Failed,
		&summary.SuccessRate,
		&summary.FailureRate,
		&summary.P50MS,
		&summary.P95MS,
		&summary.P99MS,
		&summary.InputTokens,
		&summary.OutputTokens,
		&summary.ThresholdsPassed,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RunDetail{}, ErrNotFound
		}
		return RunDetail{}, fmt.Errorf("get run: %w", err)
	}
	if err := json.Unmarshal(registrationPayload, &detail.Registration); err != nil {
		return RunDetail{}, fmt.Errorf("decode registration: %w", err)
	}
	if summaryPresent {
		detail.Summary = &summary
	}
	return detail, nil
}

type caseCursor struct {
	CaseID    string `json:"case_id"`
	Iteration int    `json:"iteration"`
}

func (r *PostgresRepository) ListCases(ctx context.Context, runID, cursor string, limit int, failedOnly bool) (CasePage, error) {
	var exists bool
	if err := r.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM agentstorm_runs WHERE id = $1)`, runID).Scan(&exists); err != nil {
		return CasePage{}, fmt.Errorf("check run: %w", err)
	}
	if !exists {
		return CasePage{}, ErrNotFound
	}

	position := caseCursor{}
	if cursor != "" {
		payload, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil || json.Unmarshal(payload, &position) != nil || position.CaseID == "" {
			return CasePage{}, validationError("invalid cursor")
		}
	}
	rows, err := r.pool.Query(ctx, `
		SELECT idempotency_key, case_id, iteration, success, latency_ms,
		       input_tokens, output_tokens, failure_kind, output, error
		FROM agentstorm_case_results
		WHERE run_id = $1
		  AND ($2::boolean = false OR success = false)
		  AND ($3::text = '' OR (case_id, iteration) > ($3, $4))
		ORDER BY case_id, iteration
		LIMIT $5`, runID, failedOnly, position.CaseID, position.Iteration, limit+1)
	if err != nil {
		return CasePage{}, fmt.Errorf("list cases: %w", err)
	}
	defer rows.Close()

	page := CasePage{Cases: make([]CaseResult, 0, limit)}
	for rows.Next() {
		var item CaseResult
		if err := rows.Scan(
			&item.IdempotencyKey,
			&item.CaseID,
			&item.Iteration,
			&item.Success,
			&item.LatencyMS,
			&item.InputTokens,
			&item.OutputTokens,
			&item.FailureKind,
			&item.Output,
			&item.Error,
		); err != nil {
			return CasePage{}, fmt.Errorf("scan case: %w", err)
		}
		page.Cases = append(page.Cases, item)
	}
	if err := rows.Err(); err != nil {
		return CasePage{}, fmt.Errorf("iterate cases: %w", err)
	}
	if len(page.Cases) > limit {
		last := page.Cases[limit-1]
		cursorPayload, _ := json.Marshal(caseCursor{CaseID: last.CaseID, Iteration: last.Iteration})
		page.NextCursor = base64.RawURLEncoding.EncodeToString(cursorPayload)
		page.Cases = page.Cases[:limit]
	}
	return page, nil
}

func (r *PostgresRepository) Compare(ctx context.Context, baselineID, candidateID string) (Comparison, error) {
	baseline, err := r.GetRun(ctx, baselineID)
	if err != nil {
		return Comparison{}, err
	}
	candidate, err := r.GetRun(ctx, candidateID)
	if err != nil {
		return Comparison{}, err
	}
	if baseline.Status != RunComplete || candidate.Status != RunComplete || baseline.Summary == nil || candidate.Summary == nil {
		return Comparison{}, ErrNotReady
	}

	delta := ComparisonDelta{
		SuccessRate:  candidate.Summary.SuccessRate - baseline.Summary.SuccessRate,
		FailureRate:  candidate.Summary.FailureRate - baseline.Summary.FailureRate,
		P50MS:        candidate.Summary.P50MS - baseline.Summary.P50MS,
		P95MS:        candidate.Summary.P95MS - baseline.Summary.P95MS,
		P99MS:        candidate.Summary.P99MS - baseline.Summary.P99MS,
		InputTokens:  candidate.Summary.InputTokens - baseline.Summary.InputTokens,
		OutputTokens: candidate.Summary.OutputTokens - baseline.Summary.OutputTokens,
	}
	delta.P50Percent = percentDelta(baseline.Summary.P50MS, candidate.Summary.P50MS)
	delta.P95Percent = percentDelta(baseline.Summary.P95MS, candidate.Summary.P95MS)
	delta.P99Percent = percentDelta(baseline.Summary.P99MS, candidate.Summary.P99MS)

	return Comparison{
		BaselineID:  baselineID,
		CandidateID: candidateID,
		Baseline:    *baseline.Summary,
		Candidate:   *candidate.Summary,
		Delta:       delta,
	}, nil
}

func percentDelta(baseline, candidate float64) *float64 {
	if baseline == 0 {
		return nil
	}
	value := (candidate - baseline) / baseline * 100
	return &value
}
