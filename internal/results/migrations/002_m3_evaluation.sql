ALTER TABLE agentstorm_case_results
    ADD COLUMN IF NOT EXISTS tool_path text[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS assertions jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS input_cost_usd numeric(30, 12),
    ADD COLUMN IF NOT EXISTS output_cost_usd numeric(30, 12),
    ADD COLUMN IF NOT EXISTS cost_usd numeric(30, 12);

ALTER TABLE agentstorm_run_summaries
    ADD COLUMN IF NOT EXISTS input_cost_usd numeric(30, 12),
    ADD COLUMN IF NOT EXISTS output_cost_usd numeric(30, 12),
    ADD COLUMN IF NOT EXISTS cost_usd numeric(30, 12);

INSERT INTO agentstorm_schema_migrations (version)
VALUES ('002_m3_evaluation')
ON CONFLICT (version) DO NOTHING;
