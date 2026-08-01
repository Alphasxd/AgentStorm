CREATE TABLE IF NOT EXISTS agentstorm_schema_migrations (
    version text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agentstorm_runs (
    id text PRIMARY KEY,
    registration_key text NOT NULL UNIQUE,
    registration_hash text NOT NULL,
    registration jsonb NOT NULL,
    expected_shards integer NOT NULL CHECK (expected_shards > 0),
    status text NOT NULL DEFAULT 'collecting' CHECK (status IN ('collecting', 'complete')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz
);

CREATE TABLE IF NOT EXISTS agentstorm_shard_receipts (
    run_id text NOT NULL REFERENCES agentstorm_runs(id) ON DELETE CASCADE,
    shard_index integer NOT NULL CHECK (shard_index >= 0),
    idempotency_key text NOT NULL UNIQUE,
    payload_hash text NOT NULL,
    object_key text NOT NULL,
    summary jsonb NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'complete')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, shard_index)
);

CREATE TABLE IF NOT EXISTS agentstorm_case_results (
    run_id text NOT NULL REFERENCES agentstorm_runs(id) ON DELETE CASCADE,
    case_id text NOT NULL,
    iteration integer NOT NULL CHECK (iteration >= 0),
    shard_index integer NOT NULL,
    idempotency_key text NOT NULL UNIQUE,
    payload_hash text NOT NULL,
    success boolean NOT NULL,
    latency_ms double precision NOT NULL CHECK (latency_ms >= 0),
    input_tokens bigint NOT NULL CHECK (input_tokens >= 0),
    output_tokens bigint NOT NULL CHECK (output_tokens >= 0),
    failure_kind text NOT NULL DEFAULT '',
    output text,
    error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, case_id, iteration)
);

CREATE INDEX IF NOT EXISTS agentstorm_case_results_failed
    ON agentstorm_case_results (run_id, success, case_id, iteration);

CREATE TABLE IF NOT EXISTS agentstorm_run_summaries (
    run_id text PRIMARY KEY REFERENCES agentstorm_runs(id) ON DELETE CASCADE,
    total bigint NOT NULL,
    succeeded bigint NOT NULL,
    failed bigint NOT NULL,
    success_rate double precision NOT NULL,
    failure_rate double precision NOT NULL,
    p50_ms double precision NOT NULL,
    p95_ms double precision NOT NULL,
    p99_ms double precision NOT NULL,
    input_tokens bigint NOT NULL,
    output_tokens bigint NOT NULL,
    thresholds_passed boolean NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO agentstorm_schema_migrations (version)
VALUES ('001_init')
ON CONFLICT (version) DO NOTHING;
