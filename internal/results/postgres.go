package results

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
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

	entries, err := fs.ReadDir(migrations, "migrations")
	if err != nil {
		return fmt.Errorf("list migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && len(entry.Name()) > 4 && entry.Name()[len(entry.Name())-4:] == ".sql" {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		schema, err := migrations.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if _, err := connection.Exec(ctx, string(schema)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
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
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin run registration: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }()

	var inserted string
	err = transaction.QueryRow(ctx, `
		INSERT INTO agentstorm_runs (id, registration_key, registration_hash, registration, expected_shards)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING
		RETURNING id`, runID, key, hash, payload, registration.ExpectedShards).Scan(&inserted)
	if err == nil {
		if err := enqueueRunShards(ctx, transaction, runID, registration); err != nil {
			return false, err
		}
		if err := transaction.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit run registration: %w", err)
		}
		return true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("register run: %w", err)
	}

	var existingID, existingKey, existingHash string
	err = transaction.QueryRow(ctx, `
		SELECT id, registration_key, registration_hash
		FROM agentstorm_runs
		WHERE id = $1 OR registration_key = $2
		FOR UPDATE`, runID, key).Scan(&existingID, &existingKey, &existingHash)
	if err != nil {
		return false, fmt.Errorf("read conflicting registration: %w", err)
	}
	if existingID != runID || existingKey != key || existingHash != hash {
		return false, ErrConflict
	}
	if err := enqueueRunShards(ctx, transaction, runID, registration); err != nil {
		return false, err
	}
	if err := transaction.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit duplicate run registration: %w", err)
	}
	return false, nil
}

func enqueueRunShards(ctx context.Context, transaction pgx.Tx, runID string, registration Registration) error {
	if registration.Scheduling == nil || registration.Scheduling.Strategy != "keda" {
		return nil
	}
	if _, err := transaction.Exec(ctx, `
		INSERT INTO agentstorm_shard_queue (run_id, shard_index)
		SELECT $1, generate_series(0, $2 - 1)
		ON CONFLICT (run_id, shard_index) DO NOTHING`, runID, registration.ExpectedShards); err != nil {
		return fmt.Errorf("enqueue run shards: %w", err)
	}
	return nil
}

func (r *PostgresRepository) TerminateRun(ctx context.Context, runID, key, hash string, terminal TerminalRequest) (bool, error) {
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin terminal update: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }()

	var status RunStatus
	var existingKey, existingHash *string
	if err := transaction.QueryRow(ctx, `
		SELECT status, terminal_key, terminal_hash
		FROM agentstorm_runs
		WHERE id = $1
		FOR UPDATE`, runID).Scan(&status, &existingKey, &existingHash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, ErrNotFound
		}
		return false, fmt.Errorf("read run terminal state: %w", err)
	}
	if status == RunComplete {
		return false, ErrConflict
	}
	if status == RunCancelled || status == RunHarnessFailed {
		if existingKey == nil || existingHash == nil || *existingKey != key || *existingHash != hash || status != terminal.Status {
			return false, ErrConflict
		}
		if err := transaction.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit duplicate terminal update: %w", err)
		}
		return false, nil
	}
	if _, err := transaction.Exec(ctx, `
		UPDATE agentstorm_runs
		SET status = $2, terminal_key = $3, terminal_hash = $4,
		    terminal_reason_code = $5, terminal_at = now(), updated_at = now()
		WHERE id = $1`, runID, terminal.Status, key, hash, terminal.ReasonCode); err != nil {
		return false, fmt.Errorf("persist terminal update: %w", err)
	}
	if _, err := transaction.Exec(ctx, `
		UPDATE agentstorm_shard_queue
		SET state = 'cancelled', lease_owner = NULL, lease_token_hash = NULL,
		    lease_expires_at = NULL, updated_at = now()
		WHERE run_id = $1 AND state <> 'complete'`, runID); err != nil {
		return false, fmt.Errorf("cancel queued shards: %w", err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit terminal update: %w", err)
	}
	return true, nil
}

func (r *PostgresRepository) ClaimShard(
	ctx context.Context,
	runID string,
	workerID string,
	leaseHash string,
	duration time.Duration,
) (int, time.Time, error) {
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("begin shard claim: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }()

	var status RunStatus
	var strategy string
	if err := transaction.QueryRow(ctx, `
		SELECT status, COALESCE(registration->'scheduling'->>'strategy', '')
		FROM agentstorm_runs
		WHERE id = $1
		FOR UPDATE`, runID).Scan(&status, &strategy); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, time.Time{}, ErrNotFound
		}
		return 0, time.Time{}, fmt.Errorf("read queued run: %w", err)
	}
	if strategy != "keda" {
		return 0, time.Time{}, validationError("run does not use queued scheduling")
	}
	if status != RunCollecting {
		return 0, time.Time{}, ErrQueueEmpty
	}

	var shardIndex int
	var expiresAt time.Time
	err = transaction.QueryRow(ctx, `
		UPDATE agentstorm_shard_queue
		SET lease_token_hash = $3,
		    lease_expires_at = now() + ($4 * interval '1 millisecond'),
		    updated_at = now()
		WHERE run_id = $1 AND lease_owner = $2 AND state = 'leased' AND lease_expires_at > now()
		RETURNING shard_index, lease_expires_at`, runID, workerID, leaseHash, duration.Milliseconds()).Scan(&shardIndex, &expiresAt)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, time.Time{}, fmt.Errorf("renew repeated shard claim: %w", err)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		err = transaction.QueryRow(ctx, `
			WITH candidate AS (
				SELECT shard_index
				FROM agentstorm_shard_queue
				WHERE run_id = $1
				  AND (state = 'queued' OR (state = 'leased' AND lease_expires_at <= now()))
				ORDER BY shard_index
				FOR UPDATE SKIP LOCKED
				LIMIT 1
			)
			UPDATE agentstorm_shard_queue AS queue
			SET state = 'leased', lease_owner = $2, lease_token_hash = $3,
			    lease_expires_at = now() + ($4 * interval '1 millisecond'),
			    lease_count = lease_count + 1, updated_at = now()
			FROM candidate
			WHERE queue.run_id = $1 AND queue.shard_index = candidate.shard_index
			RETURNING queue.shard_index, queue.lease_expires_at`, runID, workerID, leaseHash, duration.Milliseconds()).Scan(&shardIndex, &expiresAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, time.Time{}, ErrQueueEmpty
		}
		if err != nil {
			return 0, time.Time{}, fmt.Errorf("claim queued shard: %w", err)
		}
	}
	if err := transaction.Commit(ctx); err != nil {
		return 0, time.Time{}, fmt.Errorf("commit shard claim: %w", err)
	}
	return shardIndex, expiresAt, nil
}

func (r *PostgresRepository) RenewShardLease(
	ctx context.Context,
	runID string,
	shardIndex int,
	leaseHash string,
	duration time.Duration,
) (time.Time, error) {
	var expiresAt time.Time
	err := r.pool.QueryRow(ctx, `
		UPDATE agentstorm_shard_queue
		SET lease_expires_at = now() + ($4 * interval '1 millisecond'), updated_at = now()
		WHERE run_id = $1 AND shard_index = $2 AND state = 'leased'
		  AND lease_token_hash = $3 AND lease_expires_at > now()
		RETURNING lease_expires_at`, runID, shardIndex, leaseHash, duration.Milliseconds()).Scan(&expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrLeaseLost
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("renew shard lease: %w", err)
	}
	return expiresAt, nil
}

func (r *PostgresRepository) QueueStatus(ctx context.Context, runID string) (QueueStatus, error) {
	var status QueueStatus
	var thresholdsPassed *bool
	err := r.pool.QueryRow(ctx, `
		SELECT r.status, r.expected_shards,
		       count(q.shard_index) FILTER (
		           WHERE q.state = 'queued' OR (q.state = 'leased' AND q.lease_expires_at <= now())
		       ),
		       count(q.shard_index) FILTER (WHERE q.state = 'leased' AND q.lease_expires_at > now()),
		       count(q.shard_index) FILTER (WHERE q.state = 'complete'),
		       CASE WHEN rs.run_id IS NULL THEN NULL ELSE rs.thresholds_passed END
		FROM agentstorm_runs r
		LEFT JOIN agentstorm_shard_queue q ON q.run_id = r.id
		LEFT JOIN agentstorm_run_summaries rs ON rs.run_id = r.id
		WHERE r.id = $1
		GROUP BY r.id, rs.run_id, rs.thresholds_passed`, runID).Scan(
		&status.RunStatus, &status.Expected, &status.Pending, &status.Leased, &status.Completed, &thresholdsPassed,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return QueueStatus{}, ErrNotFound
	}
	if err != nil {
		return QueueStatus{}, fmt.Errorf("read queue status: %w", err)
	}
	if status.RunStatus == RunCollecting {
		status.Available = status.Pending
	}
	status.ThresholdsPassed = thresholdsPassed
	return status, nil
}

const schedulerLockID int64 = migrationLockID + 1

func (r *PostgresRepository) AcquirePermit(
	ctx context.Context,
	runID string,
	request PermitRequest,
	leaseHash string,
	policy LimitPolicy,
) (string, time.Time, error) {
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("begin permit acquisition: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }()
	if _, err := transaction.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", schedulerLockID); err != nil {
		return "", time.Time{}, fmt.Errorf("lock distributed limits: %w", err)
	}
	if _, err := transaction.Exec(ctx, `
		DELETE FROM agentstorm_execution_permits
		WHERE updated_at < now() - interval '1 hour'
		  AND (released_at IS NOT NULL OR lease_expires_at <= now())`); err != nil {
		return "", time.Time{}, fmt.Errorf("prune execution permits: %w", err)
	}

	var runStatus RunStatus
	var registeredProvider string
	if err := transaction.QueryRow(ctx, `
		SELECT status, registration->'target'->>'provider'
		FROM agentstorm_runs
		WHERE id = $1`, runID).Scan(&runStatus, &registeredProvider); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", time.Time{}, ErrNotFound
		}
		return "", time.Time{}, fmt.Errorf("read permit run: %w", err)
	}
	if runStatus != RunCollecting {
		return "", time.Time{}, ErrLeaseLost
	}
	if registeredProvider != request.Provider {
		return "", time.Time{}, validationError("permit provider does not match registered run")
	}

	var existingID, existingHash string
	var existingExpiry time.Time
	var releasedAt *time.Time
	var existingActive bool
	err = transaction.QueryRow(ctx, `
		SELECT permit_id, lease_token_hash, lease_expires_at, released_at, lease_expires_at > now()
		FROM agentstorm_execution_permits
		WHERE request_id = $1
		FOR UPDATE`, request.RequestID).Scan(&existingID, &existingHash, &existingExpiry, &releasedAt, &existingActive)
	if err == nil {
		if releasedAt != nil || !existingActive {
			return "", time.Time{}, ErrLeaseLost
		}
		if _, err := transaction.Exec(ctx, `
			UPDATE agentstorm_execution_permits
			SET lease_token_hash = $2,
			    lease_expires_at = now() + ($3 * interval '1 millisecond'), updated_at = now()
			WHERE permit_id = $1`, existingID, leaseHash, policy.LeaseDuration.Milliseconds()); err != nil {
			return "", time.Time{}, fmt.Errorf("refresh repeated permit: %w", err)
		}
		if err := transaction.QueryRow(ctx, `SELECT lease_expires_at FROM agentstorm_execution_permits WHERE permit_id = $1`, existingID).Scan(&existingExpiry); err != nil {
			return "", time.Time{}, fmt.Errorf("read refreshed permit: %w", err)
		}
		if err := transaction.Commit(ctx); err != nil {
			return "", time.Time{}, fmt.Errorf("commit repeated permit: %w", err)
		}
		return existingID, existingExpiry, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", time.Time{}, fmt.Errorf("read existing permit: %w", err)
	}

	providerLimit := policy.Providers[request.Provider]
	var globalActive, providerActive, globalRate, providerRate int
	if err := transaction.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE released_at IS NULL AND lease_expires_at > now()),
			count(*) FILTER (WHERE provider = $1 AND released_at IS NULL AND lease_expires_at > now())
		FROM agentstorm_execution_permits`, request.Provider).Scan(&globalActive, &providerActive); err != nil {
		return "", time.Time{}, fmt.Errorf("count active permits: %w", err)
	}
	if err := transaction.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE provider = $1)
		FROM agentstorm_rate_events
		WHERE occurred_at > now() - interval '1 minute'`, request.Provider).Scan(&globalRate, &providerRate); err != nil {
		return "", time.Time{}, fmt.Errorf("count rate events: %w", err)
	}
	if (policy.Global.MaxConcurrency > 0 && globalActive >= policy.Global.MaxConcurrency) ||
		(providerLimit.MaxConcurrency > 0 && providerActive >= providerLimit.MaxConcurrency) {
		return "", time.Time{}, &CapacityError{RetryAfter: 250 * time.Millisecond}
	}
	if (policy.Global.RequestsPerMinute > 0 && globalRate >= policy.Global.RequestsPerMinute) ||
		(providerLimit.RequestsPerMinute > 0 && providerRate >= providerLimit.RequestsPerMinute) {
		return "", time.Time{}, &CapacityError{RetryAfter: time.Second}
	}

	permitID, err := randomDatabaseIdentifier()
	if err != nil {
		return "", time.Time{}, err
	}
	var expiresAt time.Time
	if err := transaction.QueryRow(ctx, `
		INSERT INTO agentstorm_execution_permits
			(permit_id, request_id, run_id, worker_id, provider, lease_token_hash, lease_expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, now() + ($7 * interval '1 millisecond'))
		RETURNING lease_expires_at`, permitID, request.RequestID, runID, request.WorkerID,
		request.Provider, leaseHash, policy.LeaseDuration.Milliseconds()).Scan(&expiresAt); err != nil {
		return "", time.Time{}, fmt.Errorf("persist execution permit: %w", err)
	}
	if _, err := transaction.Exec(ctx, `
		INSERT INTO agentstorm_rate_events (request_id, provider)
		VALUES ($1, $2)`, request.RequestID, request.Provider); err != nil {
		return "", time.Time{}, fmt.Errorf("persist rate event: %w", err)
	}
	if _, err := transaction.Exec(ctx, `DELETE FROM agentstorm_rate_events WHERE occurred_at < now() - interval '1 hour'`); err != nil {
		return "", time.Time{}, fmt.Errorf("prune rate events: %w", err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return "", time.Time{}, fmt.Errorf("commit execution permit: %w", err)
	}
	return permitID, expiresAt, nil
}

func (r *PostgresRepository) RenewPermit(
	ctx context.Context,
	runID string,
	permitID string,
	leaseHash string,
	duration time.Duration,
) (time.Time, error) {
	var expiresAt time.Time
	err := r.pool.QueryRow(ctx, `
		UPDATE agentstorm_execution_permits
		SET lease_expires_at = now() + ($4 * interval '1 millisecond'), updated_at = now()
		WHERE run_id = $1 AND permit_id = $2 AND lease_token_hash = $3
		  AND released_at IS NULL AND lease_expires_at > now()
		RETURNING lease_expires_at`, runID, permitID, leaseHash, duration.Milliseconds()).Scan(&expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrLeaseLost
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("renew execution permit: %w", err)
	}
	return expiresAt, nil
}

func (r *PostgresRepository) ReleasePermit(ctx context.Context, runID, permitID, leaseHash string) error {
	command, err := r.pool.Exec(ctx, `
		UPDATE agentstorm_execution_permits
		SET released_at = COALESCE(released_at, now()), updated_at = now()
		WHERE run_id = $1 AND permit_id = $2 AND lease_token_hash = $3`, runID, permitID, leaseHash)
	if err != nil {
		return fmt.Errorf("release execution permit: %w", err)
	}
	if command.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

func randomDatabaseIdentifier() (string, error) {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("generate scheduler identifier: %w", err)
	}
	return hex.EncodeToString(buffer), nil
}

func (r *PostgresRepository) ReserveShard(
	ctx context.Context,
	runID string,
	shardIndex int,
	key string,
	hash string,
	objectKey string,
	leaseHash string,
	summary ShardSummary,
) (ShardReservation, error) {
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		return ShardReservation{}, fmt.Errorf("begin shard reservation: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }()

	var expectedShards int
	var runStatus RunStatus
	var registrationPayload []byte
	if err := transaction.QueryRow(ctx, `
		SELECT expected_shards, registration, status
		FROM agentstorm_runs
		WHERE id = $1
		FOR UPDATE`, runID).Scan(&expectedShards, &registrationPayload, &runStatus); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ShardReservation{}, ErrNotFound
		}
		return ShardReservation{}, fmt.Errorf("read run for shard: %w", err)
	}
	var registration Registration
	if err := json.Unmarshal(registrationPayload, &registration); err != nil {
		return ShardReservation{}, fmt.Errorf("decode run pricing: %w", err)
	}
	if shardIndex >= expectedShards {
		return ShardReservation{}, validationErrorf("shard index %d is outside expected range [0,%d)", shardIndex, expectedShards)
	}
	if registration.Scheduling != nil && registration.Scheduling.Strategy == "keda" && runStatus == RunCollecting {
		if strings.TrimSpace(leaseHash) == "" {
			return ShardReservation{}, ErrLeaseLost
		}
		var storedLeaseHash string
		if err := transaction.QueryRow(ctx, `
			SELECT lease_token_hash
			FROM agentstorm_shard_queue
			WHERE run_id = $1 AND shard_index = $2 AND state = 'leased' AND lease_expires_at > now()
			FOR UPDATE`, runID, shardIndex).Scan(&storedLeaseHash); errors.Is(err, pgx.ErrNoRows) {
			return ShardReservation{}, ErrLeaseLost
		} else if err != nil {
			return ShardReservation{}, fmt.Errorf("validate shard lease: %w", err)
		}
		if storedLeaseHash != leaseHash {
			return ShardReservation{}, ErrLeaseLost
		}
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
		return ShardReservation{Pricing: registration.Target.Pricing}, nil
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
	return ShardReservation{
		AlreadyComplete: existingStatus == "complete",
		Pricing:         registration.Target.Pricing,
	}, nil
}

func (r *PostgresRepository) FinalizeShard(
	ctx context.Context,
	runID string,
	shardIndex int,
	hash string,
	leaseHash string,
	cases []CaseResult,
	pricing *RunPricing,
) (bool, error) {
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin shard finalization: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }()

	var lockedRunID string
	var runStatus RunStatus
	var registrationPayload []byte
	if err := transaction.QueryRow(ctx, `
		SELECT id, registration, status
		FROM agentstorm_runs
		WHERE id = $1
		FOR UPDATE`, runID).Scan(&lockedRunID, &registrationPayload, &runStatus); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, ErrNotFound
		}
		return false, fmt.Errorf("lock run for shard finalization: %w", err)
	}
	var registration Registration
	if err := json.Unmarshal(registrationPayload, &registration); err != nil {
		return false, fmt.Errorf("decode run scheduling: %w", err)
	}
	if registration.Scheduling != nil && registration.Scheduling.Strategy == "keda" && runStatus == RunCollecting {
		var storedLeaseHash string
		if err := transaction.QueryRow(ctx, `
			SELECT lease_token_hash
			FROM agentstorm_shard_queue
			WHERE run_id = $1 AND shard_index = $2 AND state = 'leased' AND lease_expires_at > now()
			FOR UPDATE`, runID, shardIndex).Scan(&storedLeaseHash); errors.Is(err, pgx.ErrNoRows) {
			return false, ErrLeaseLost
		} else if err != nil {
			return false, fmt.Errorf("lock shard lease: %w", err)
		}
		if storedLeaseHash != leaseHash {
			return false, ErrLeaseLost
		}
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
		var inputCost, outputCost, totalCost *string
		if usageComplete(item.UsageComplete) {
			inputCost, outputCost, totalCost, err = costsForTokens(
				pricing, item.InputTokens, item.OutputTokens,
			)
		}
		if err != nil {
			return false, fmt.Errorf("calculate case cost: %w", err)
		}
		item.InputCostUSD = inputCost
		item.OutputCostUSD = outputCost
		item.CostUSD = totalCost
		assertionsPayload, err := json.Marshal(item.Assertions)
		if err != nil {
			return false, fmt.Errorf("marshal case assertions: %w", err)
		}
		attemptsPayload, err := json.Marshal(item.Attempts)
		if err != nil {
			return false, fmt.Errorf("marshal case attempts: %w", err)
		}
		itemHash, err := contentHash(item)
		if err != nil {
			return false, fmt.Errorf("hash case result: %w", err)
		}
		var inserted string
		err = transaction.QueryRow(ctx, `
			INSERT INTO agentstorm_case_results
				(run_id, case_id, iteration, shard_index, idempotency_key, payload_hash,
				 success, latency_ms, input_tokens, output_tokens, failure_kind, output, error,
				 tool_path, assertions, input_cost_usd, output_cost_usd, cost_usd,
				 failure_category, error_code, attempts, usage_complete)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
			        $16, $17, $18, $19, $20, $21, $22)
			ON CONFLICT DO NOTHING
			RETURNING case_id`, runID, item.CaseID, item.Iteration, shardIndex, item.IdempotencyKey,
			itemHash, item.Success, item.LatencyMS, item.InputTokens, item.OutputTokens,
			item.FailureKind, item.Output, item.Error, item.ToolPath, assertionsPayload,
			item.InputCostUSD, item.OutputCostUSD, item.CostUSD, item.FailureCategory,
			item.ErrorCode, attemptsPayload, usageComplete(item.UsageComplete)).Scan(&inserted)
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
	if registration.Scheduling != nil && registration.Scheduling.Strategy == "keda" {
		if _, err := transaction.Exec(ctx, `
			UPDATE agentstorm_shard_queue
			SET state = 'complete', lease_owner = NULL, lease_token_hash = NULL,
			    lease_expires_at = NULL, updated_at = now()
			WHERE run_id = $1 AND shard_index = $2`, runID, shardIndex); err != nil {
			return false, fmt.Errorf("complete queued shard: %w", err)
		}
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
			count(*) FILTER (WHERE NOT success AND failure_category = 'evaluation'),
			count(*) FILTER (WHERE NOT success AND failure_category IN ('provider', 'tool', 'harness')),
			COALESCE(sum(jsonb_array_length(attempts)), 0)::bigint,
			COALESCE(sum(GREATEST(jsonb_array_length(attempts) - 1, 0)), 0)::bigint,
			count(*) FILTER (WHERE jsonb_array_length(attempts) > 1),
			count(*) FILTER (WHERE success AND jsonb_array_length(attempts) > 1),
			COALESCE(sum((
				SELECT count(*) FROM jsonb_array_elements(attempts) AS item
				WHERE COALESCE(item->>'injected_fault', '') <> ''
			)), 0)::bigint,
			COALESCE(sum((
				SELECT count(*) FROM jsonb_array_elements(attempts) AS item
				WHERE item->>'error_code' = 'circuit_open'
			)), 0)::bigint,
			COALESCE(percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ms), 0),
			COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms), 0),
			COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms), 0),
			COALESCE(sum(input_tokens), 0),
			COALESCE(sum(output_tokens), 0),
			CASE WHEN count(*) > 0 AND count(input_cost_usd) = count(*) THEN sum(input_cost_usd)::text END,
			CASE WHEN count(*) > 0 AND count(output_cost_usd) = count(*) THEN sum(output_cost_usd)::text END,
			CASE WHEN count(*) > 0 AND count(cost_usd) = count(*) THEN sum(cost_usd)::text END,
			COALESCE(bool_and(usage_complete), true)
		FROM agentstorm_case_results
		WHERE run_id = $1`, runID).Scan(
		&aggregate.Total,
		&aggregate.Succeeded,
		&aggregate.Failed,
		&aggregate.QualityFailures,
		&aggregate.InfrastructureFailures,
		&aggregate.AttemptCount,
		&aggregate.RetryCount,
		&aggregate.RetriedCases,
		&aggregate.RetrySuccesses,
		&aggregate.InjectedFaults,
		&aggregate.CircuitRejections,
		&aggregate.P50MS,
		&aggregate.P95MS,
		&aggregate.P99MS,
		&aggregate.InputTokens,
		&aggregate.OutputTokens,
		&aggregate.InputCostUSD,
		&aggregate.OutputCostUSD,
		&aggregate.CostUSD,
		&aggregate.UsageComplete,
	); err != nil {
		return fmt.Errorf("aggregate run: %w", err)
	}
	if aggregate.Total > 0 {
		aggregate.SuccessRate = float64(aggregate.Succeeded) / float64(aggregate.Total)
		aggregate.FailureRate = float64(aggregate.Failed) / float64(aggregate.Total)
		aggregate.QualityFailureRate = float64(aggregate.QualityFailures) / float64(aggregate.Total)
		aggregate.InfrastructureFailureRate = float64(aggregate.InfrastructureFailures) / float64(aggregate.Total)
	}
	if aggregate.RetriedCases > 0 {
		aggregate.RetrySuccessRate = float64(aggregate.RetrySuccesses) / float64(aggregate.RetriedCases)
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
			(run_id, total, succeeded, failed, success_rate, failure_rate,
			 quality_failures, quality_failure_rate, infrastructure_failures, infrastructure_failure_rate,
			 attempt_count, retry_count, retried_cases, retry_successes, retry_success_rate,
			 injected_faults, circuit_rejections, p50_ms, p95_ms, p99_ms,
			 input_tokens, output_tokens, thresholds_passed,
			 input_cost_usd, output_cost_usd, cost_usd, usage_complete)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
		        $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27)
		ON CONFLICT (run_id) DO UPDATE SET
			total = EXCLUDED.total,
			succeeded = EXCLUDED.succeeded,
			failed = EXCLUDED.failed,
			success_rate = EXCLUDED.success_rate,
			failure_rate = EXCLUDED.failure_rate,
			quality_failures = EXCLUDED.quality_failures,
			quality_failure_rate = EXCLUDED.quality_failure_rate,
			infrastructure_failures = EXCLUDED.infrastructure_failures,
			infrastructure_failure_rate = EXCLUDED.infrastructure_failure_rate,
			attempt_count = EXCLUDED.attempt_count,
			retry_count = EXCLUDED.retry_count,
			retried_cases = EXCLUDED.retried_cases,
			retry_successes = EXCLUDED.retry_successes,
			retry_success_rate = EXCLUDED.retry_success_rate,
			injected_faults = EXCLUDED.injected_faults,
			circuit_rejections = EXCLUDED.circuit_rejections,
			p50_ms = EXCLUDED.p50_ms,
			p95_ms = EXCLUDED.p95_ms,
			p99_ms = EXCLUDED.p99_ms,
			input_tokens = EXCLUDED.input_tokens,
			output_tokens = EXCLUDED.output_tokens,
			thresholds_passed = EXCLUDED.thresholds_passed,
			input_cost_usd = EXCLUDED.input_cost_usd,
			output_cost_usd = EXCLUDED.output_cost_usd,
			cost_usd = EXCLUDED.cost_usd,
			usage_complete = EXCLUDED.usage_complete,
			updated_at = now()`, runID, aggregate.Total, aggregate.Succeeded, aggregate.Failed,
		aggregate.SuccessRate, aggregate.FailureRate, aggregate.QualityFailures, aggregate.QualityFailureRate,
		aggregate.InfrastructureFailures, aggregate.InfrastructureFailureRate, aggregate.AttemptCount,
		aggregate.RetryCount, aggregate.RetriedCases, aggregate.RetrySuccesses, aggregate.RetrySuccessRate,
		aggregate.InjectedFaults, aggregate.CircuitRejections, aggregate.P50MS, aggregate.P95MS,
		aggregate.P99MS, aggregate.InputTokens, aggregate.OutputTokens, aggregate.ThresholdsPassed,
		aggregate.InputCostUSD, aggregate.OutputCostUSD, aggregate.CostUSD, aggregate.UsageComplete); err != nil {
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
		SET status = CASE WHEN status IN ('cancelled', 'harness_failed') THEN status ELSE $2 END,
		    updated_at = now(),
		    completed_at = CASE
		        WHEN status NOT IN ('cancelled', 'harness_failed') AND $2 = 'complete'
		        THEN COALESCE(completed_at, $3)
		        ELSE completed_at
		    END
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
	var expectedShards int
	var terminalReason *string
	var terminalAt *time.Time
	err := r.pool.QueryRow(ctx, `
		SELECT r.id, r.registration, r.status,
		       count(sr.shard_index) FILTER (WHERE sr.status = 'complete'),
		       r.created_at, r.updated_at, r.completed_at,
		       r.expected_shards, r.terminal_reason_code, r.terminal_at,
		       (rs.run_id IS NOT NULL),
		       COALESCE(rs.total, 0), COALESCE(rs.succeeded, 0), COALESCE(rs.failed, 0),
		       COALESCE(rs.success_rate, 0), COALESCE(rs.failure_rate, 0),
		       COALESCE(rs.quality_failures, 0), COALESCE(rs.quality_failure_rate, 0),
		       COALESCE(rs.infrastructure_failures, 0), COALESCE(rs.infrastructure_failure_rate, 0),
		       COALESCE(rs.attempt_count, 0), COALESCE(rs.retry_count, 0),
		       COALESCE(rs.retried_cases, 0), COALESCE(rs.retry_successes, 0),
		       COALESCE(rs.retry_success_rate, 0), COALESCE(rs.injected_faults, 0),
		       COALESCE(rs.circuit_rejections, 0),
		       COALESCE(rs.p50_ms, 0), COALESCE(rs.p95_ms, 0), COALESCE(rs.p99_ms, 0),
		       COALESCE(rs.input_tokens, 0), COALESCE(rs.output_tokens, 0),
		       COALESCE(rs.thresholds_passed, false),
		       rs.input_cost_usd::text, rs.output_cost_usd::text, rs.cost_usd::text,
		       COALESCE(rs.usage_complete, true)
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
		&expectedShards,
		&terminalReason,
		&terminalAt,
		&summaryPresent,
		&summary.Total,
		&summary.Succeeded,
		&summary.Failed,
		&summary.SuccessRate,
		&summary.FailureRate,
		&summary.QualityFailures,
		&summary.QualityFailureRate,
		&summary.InfrastructureFailures,
		&summary.InfrastructureFailureRate,
		&summary.AttemptCount,
		&summary.RetryCount,
		&summary.RetriedCases,
		&summary.RetrySuccesses,
		&summary.RetrySuccessRate,
		&summary.InjectedFaults,
		&summary.CircuitRejections,
		&summary.P50MS,
		&summary.P95MS,
		&summary.P99MS,
		&summary.InputTokens,
		&summary.OutputTokens,
		&summary.ThresholdsPassed,
		&summary.InputCostUSD,
		&summary.OutputCostUSD,
		&summary.CostUSD,
		&summary.UsageComplete,
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
	detail.Partial = detail.Status == RunCancelled || detail.Status == RunHarnessFailed ||
		(detail.Status == RunCollecting && detail.ReceivedShards > 0 && detail.ReceivedShards < expectedShards)
	if terminalReason != nil && terminalAt != nil {
		detail.TerminalReason = &TerminalReason{
			Status: detail.Status, ReasonCode: *terminalReason, At: *terminalAt,
		}
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
		       input_tokens, output_tokens, failure_kind, output, error, tool_path, assertions,
		       input_cost_usd::text, output_cost_usd::text, cost_usd::text,
		       failure_category, error_code, attempts, usage_complete
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
		var assertionsPayload []byte
		var attemptsPayload []byte
		var itemUsageComplete bool
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
			&item.ToolPath,
			&assertionsPayload,
			&item.InputCostUSD,
			&item.OutputCostUSD,
			&item.CostUSD,
			&item.FailureCategory,
			&item.ErrorCode,
			&attemptsPayload,
			&itemUsageComplete,
		); err != nil {
			return CasePage{}, fmt.Errorf("scan case: %w", err)
		}
		if err := json.Unmarshal(assertionsPayload, &item.Assertions); err != nil {
			return CasePage{}, fmt.Errorf("decode case assertions: %w", err)
		}
		if err := json.Unmarshal(attemptsPayload, &item.Attempts); err != nil {
			return CasePage{}, fmt.Errorf("decode case attempts: %w", err)
		}
		item.UsageComplete = &itemUsageComplete
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
		SuccessRate:               candidate.Summary.SuccessRate - baseline.Summary.SuccessRate,
		FailureRate:               candidate.Summary.FailureRate - baseline.Summary.FailureRate,
		QualityFailures:           candidate.Summary.QualityFailures - baseline.Summary.QualityFailures,
		QualityFailureRate:        candidate.Summary.QualityFailureRate - baseline.Summary.QualityFailureRate,
		InfrastructureFailures:    candidate.Summary.InfrastructureFailures - baseline.Summary.InfrastructureFailures,
		InfrastructureFailureRate: candidate.Summary.InfrastructureFailureRate - baseline.Summary.InfrastructureFailureRate,
		AttemptCount:              candidate.Summary.AttemptCount - baseline.Summary.AttemptCount,
		RetryCount:                candidate.Summary.RetryCount - baseline.Summary.RetryCount,
		RetriedCases:              candidate.Summary.RetriedCases - baseline.Summary.RetriedCases,
		RetrySuccesses:            candidate.Summary.RetrySuccesses - baseline.Summary.RetrySuccesses,
		RetrySuccessRate:          candidate.Summary.RetrySuccessRate - baseline.Summary.RetrySuccessRate,
		InjectedFaults:            candidate.Summary.InjectedFaults - baseline.Summary.InjectedFaults,
		CircuitRejections:         candidate.Summary.CircuitRejections - baseline.Summary.CircuitRejections,
		P50MS:                     candidate.Summary.P50MS - baseline.Summary.P50MS,
		P95MS:                     candidate.Summary.P95MS - baseline.Summary.P95MS,
		P99MS:                     candidate.Summary.P99MS - baseline.Summary.P99MS,
		InputTokens:               candidate.Summary.InputTokens - baseline.Summary.InputTokens,
		OutputTokens:              candidate.Summary.OutputTokens - baseline.Summary.OutputTokens,
	}
	delta.P50Percent = percentDelta(baseline.Summary.P50MS, candidate.Summary.P50MS)
	delta.P95Percent = percentDelta(baseline.Summary.P95MS, candidate.Summary.P95MS)
	delta.P99Percent = percentDelta(baseline.Summary.P99MS, candidate.Summary.P99MS)
	if baseline.Summary.CostUSD != nil && candidate.Summary.CostUSD != nil {
		costDelta, err := subtractDecimalStrings(*candidate.Summary.CostUSD, *baseline.Summary.CostUSD)
		if err != nil {
			return Comparison{}, fmt.Errorf("calculate cost delta: %w", err)
		}
		delta.CostUSD = &costDelta
		delta.CostPercent, err = decimalPercentDelta(*baseline.Summary.CostUSD, *candidate.Summary.CostUSD)
		if err != nil {
			return Comparison{}, fmt.Errorf("calculate cost percentage: %w", err)
		}
	}

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
