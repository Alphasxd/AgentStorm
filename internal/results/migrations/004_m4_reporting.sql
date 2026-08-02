ALTER TABLE agentstorm_run_summaries
    ADD COLUMN IF NOT EXISTS quality_failures bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS quality_failure_rate double precision NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS infrastructure_failures bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS infrastructure_failure_rate double precision NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS attempt_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS retry_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS retried_cases bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS retry_successes bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS retry_success_rate double precision NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS injected_faults bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS circuit_rejections bigint NOT NULL DEFAULT 0;

UPDATE agentstorm_case_results
SET attempts = '[]'::jsonb
WHERE jsonb_typeof(attempts) IS DISTINCT FROM 'array';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'agentstorm_case_results_attempts_array'
          AND conrelid = 'agentstorm_case_results'::regclass
    ) THEN
        ALTER TABLE agentstorm_case_results
            ADD CONSTRAINT agentstorm_case_results_attempts_array
            CHECK (jsonb_typeof(attempts) = 'array');
    END IF;
END
$$;

WITH stats AS (
    SELECT
        run_id,
        count(*) AS total,
        count(*) FILTER (WHERE NOT success AND failure_category = 'evaluation') AS quality_failures,
        count(*) FILTER (
            WHERE NOT success AND failure_category IN ('provider', 'tool', 'harness')
        ) AS infrastructure_failures,
        COALESCE(sum(jsonb_array_length(attempts)), 0)::bigint AS attempt_count,
        COALESCE(sum(GREATEST(jsonb_array_length(attempts) - 1, 0)), 0)::bigint AS retry_count,
        count(*) FILTER (WHERE jsonb_array_length(attempts) > 1) AS retried_cases,
        count(*) FILTER (WHERE success AND jsonb_array_length(attempts) > 1) AS retry_successes,
        COALESCE(sum((
            SELECT count(*) FROM jsonb_array_elements(attempts) AS item
            WHERE COALESCE(item->>'injected_fault', '') <> ''
        )), 0)::bigint AS injected_faults,
        COALESCE(sum((
            SELECT count(*) FROM jsonb_array_elements(attempts) AS item
            WHERE item->>'error_code' = 'circuit_open'
        )), 0)::bigint AS circuit_rejections
    FROM agentstorm_case_results
    GROUP BY run_id
)
UPDATE agentstorm_run_summaries AS summary
SET
    quality_failures = stats.quality_failures,
    quality_failure_rate = CASE
        WHEN stats.total > 0 THEN stats.quality_failures::double precision / stats.total
        ELSE 0
    END,
    infrastructure_failures = stats.infrastructure_failures,
    infrastructure_failure_rate = CASE
        WHEN stats.total > 0 THEN stats.infrastructure_failures::double precision / stats.total
        ELSE 0
    END,
    attempt_count = stats.attempt_count,
    retry_count = stats.retry_count,
    retried_cases = stats.retried_cases,
    retry_successes = stats.retry_successes,
    retry_success_rate = CASE
        WHEN stats.retried_cases > 0 THEN stats.retry_successes::double precision / stats.retried_cases
        ELSE 0
    END,
    injected_faults = stats.injected_faults,
    circuit_rejections = stats.circuit_rejections,
    updated_at = now()
FROM stats
WHERE summary.run_id = stats.run_id;

INSERT INTO agentstorm_schema_migrations (version)
VALUES ('004_m4_reporting')
ON CONFLICT (version) DO NOTHING;
