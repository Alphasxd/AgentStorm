ALTER TABLE agentstorm_case_results
    ADD COLUMN IF NOT EXISTS model_call_count bigint,
    ADD COLUMN IF NOT EXISTS tool_call_count bigint NOT NULL DEFAULT 0;

ALTER TABLE agentstorm_run_summaries
    ADD COLUMN IF NOT EXISTS model_call_count bigint,
    ADD COLUMN IF NOT EXISTS tool_call_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS model_calls_per_successful_agent double precision,
    ADD COLUMN IF NOT EXISTS tool_calls_per_successful_agent double precision;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'agentstorm_case_results_model_call_count_nonnegative'
          AND conrelid = 'agentstorm_case_results'::regclass
    ) THEN
        ALTER TABLE agentstorm_case_results
            ADD CONSTRAINT agentstorm_case_results_model_call_count_nonnegative
            CHECK (model_call_count IS NULL OR model_call_count >= 0);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'agentstorm_case_results_tool_call_count_nonnegative'
          AND conrelid = 'agentstorm_case_results'::regclass
    ) THEN
        ALTER TABLE agentstorm_case_results
            ADD CONSTRAINT agentstorm_case_results_tool_call_count_nonnegative
            CHECK (tool_call_count >= 0);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'agentstorm_run_summaries_agent_call_counts_nonnegative'
          AND conrelid = 'agentstorm_run_summaries'::regclass
    ) THEN
        ALTER TABLE agentstorm_run_summaries
            ADD CONSTRAINT agentstorm_run_summaries_agent_call_counts_nonnegative
            CHECK (
                (model_call_count IS NULL OR model_call_count >= 0)
                AND tool_call_count >= 0
                AND (model_calls_per_successful_agent IS NULL OR model_calls_per_successful_agent >= 0)
                AND (tool_calls_per_successful_agent IS NULL OR tool_calls_per_successful_agent >= 0)
            );
    END IF;
END
$$;

INSERT INTO agentstorm_schema_migrations (version)
VALUES ('006_m6_agent_calls')
ON CONFLICT (version) DO NOTHING;
