CREATE TABLE IF NOT EXISTS agentstorm_shard_queue (
    run_id text NOT NULL REFERENCES agentstorm_runs(id) ON DELETE CASCADE,
    shard_index integer NOT NULL CHECK (shard_index >= 0),
    state text NOT NULL DEFAULT 'queued' CHECK (state IN ('queued', 'leased', 'complete', 'cancelled')),
    lease_owner text,
    lease_token_hash text,
    lease_expires_at timestamptz,
    lease_count integer NOT NULL DEFAULT 0 CHECK (lease_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, shard_index)
);

CREATE INDEX IF NOT EXISTS agentstorm_shard_queue_claimable
    ON agentstorm_shard_queue (run_id, state, lease_expires_at, shard_index);

CREATE TABLE IF NOT EXISTS agentstorm_execution_permits (
    permit_id text PRIMARY KEY,
    request_id text NOT NULL UNIQUE,
    run_id text NOT NULL REFERENCES agentstorm_runs(id) ON DELETE CASCADE,
    worker_id text NOT NULL,
    provider text NOT NULL,
    lease_token_hash text NOT NULL,
    lease_expires_at timestamptz NOT NULL,
    released_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS agentstorm_execution_permits_active
    ON agentstorm_execution_permits (provider, lease_expires_at)
    WHERE released_at IS NULL;

CREATE INDEX IF NOT EXISTS agentstorm_execution_permits_cleanup
    ON agentstorm_execution_permits (updated_at);

CREATE TABLE IF NOT EXISTS agentstorm_rate_events (
    request_id text PRIMARY KEY,
    provider text NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS agentstorm_rate_events_window
    ON agentstorm_rate_events (occurred_at, provider);

INSERT INTO agentstorm_schema_migrations (version)
VALUES ('005_m5_scheduling')
ON CONFLICT (version) DO NOTHING;
