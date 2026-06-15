-- v15 Phase 2 (item 0038): persist the requested clean-run count for a
-- benchmark spec. Existing v14 singleton specs remain one run.

ALTER TABLE benchmark_spec
    ADD COLUMN requested_run_count INTEGER NOT NULL DEFAULT 1 CHECK (requested_run_count >= 1);
