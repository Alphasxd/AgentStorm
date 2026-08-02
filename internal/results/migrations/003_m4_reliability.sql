ALTER TABLE agentstorm_runs
    DROP CONSTRAINT IF EXISTS agentstorm_runs_status_check;

ALTER TABLE agentstorm_runs
    ADD CONSTRAINT agentstorm_runs_status_check
        CHECK (status IN ('collecting', 'complete', 'cancelled', 'harness_failed')),
    ADD COLUMN IF NOT EXISTS terminal_key text,
    ADD COLUMN IF NOT EXISTS terminal_hash text,
    ADD COLUMN IF NOT EXISTS terminal_reason_code text,
    ADD COLUMN IF NOT EXISTS terminal_at timestamptz;

ALTER TABLE agentstorm_case_results
    ADD COLUMN IF NOT EXISTS failure_category text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS error_code text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS attempts jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS usage_complete boolean NOT NULL DEFAULT true;

UPDATE agentstorm_case_results
SET failure_category = CASE failure_kind
    WHEN 'assertion' THEN 'evaluation'
    WHEN 'tool' THEN 'tool'
    WHEN 'harness' THEN 'harness'
    WHEN '' THEN ''
    ELSE 'provider'
END
WHERE failure_category = '';

ALTER TABLE agentstorm_run_summaries
    ADD COLUMN IF NOT EXISTS usage_complete boolean NOT NULL DEFAULT true;

INSERT INTO agentstorm_schema_migrations (version)
VALUES ('003_m4_reliability')
ON CONFLICT (version) DO NOTHING;
