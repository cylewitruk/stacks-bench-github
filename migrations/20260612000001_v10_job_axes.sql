-- v10 (item 0005-task-kind-platform): decompose the job model into orthogonal
-- axes. The retiring `trigger_kind` (source + intent, entangled) and `job_kind`
-- (result-role) on `job` are replaced by `source` / `intent` / `task_kind` /
-- `build_target`. The fifth axis — the report *surface set* — is derived at
-- claim from `(source, intent, config)`, not stored. `git_ref_kind` is untouched
-- (it survives as its own ref-shape axis).
--
-- Phase 1 is **additive + behaviour-preserving**: the new columns land + back-fill
-- existing rows; the pipeline still runs off the old enums (Phase 2 rewires
-- reads/writes, Phase 3+ drops the old columns). The columns are **nullable**
-- here — the un-rewired creation path doesn't set them yet; Phase 2 back-fills
-- any stragglers + sets NOT NULL.
--
-- `CREATE TYPE` (not `ALTER TYPE ADD VALUE`) so this runs cleanly in the
-- migration transaction. The back-fill mirrors `JobAxes::from_legacy` (the tested
-- source of truth) `CASE`-for-`CASE`.

CREATE TYPE job_source AS ENUM (
    'github_webhook', 'github_comment', 'slack', 'cli', 'scheduler', 'daemon'
);
CREATE TYPE job_intent AS ENUM (
    'adhoc_benchmark', 'baseline_benchmark', 'block_validation', 'cache_warm'
);
CREATE TYPE task_kind AS ENUM (
    'benchmark', 'block_validation', 'build_only'
);
CREATE TYPE build_target AS ENUM (
    'stacks_bench', 'stacks_inspect'
);

ALTER TABLE job
    ADD COLUMN source       job_source,
    ADD COLUMN intent       job_intent,
    ADD COLUMN task_kind    task_kind,
    ADD COLUMN build_target build_target;

-- Back-fill existing rows (mirrors `JobAxes::from_legacy`). Every legacy job is a
-- `stacks_bench` `benchmark`; `source` from `trigger_kind`, `intent` from
-- `job_kind`. Idempotent via `WHERE source IS NULL`.
UPDATE job SET
    source = CASE trigger_kind
        WHEN 'pr_comment'  THEN 'github_comment'::job_source
        WHEN 'branch_push' THEN 'github_webhook'::job_source
        WHEN 'tag_created' THEN 'github_webhook'::job_source
        WHEN 'slack_adhoc' THEN 'slack'::job_source
        WHEN 'scheduled'   THEN 'scheduler'::job_source
        WHEN 'manual'      THEN 'cli'::job_source
    END,
    intent = CASE job_kind
        WHEN 'baseline' THEN 'baseline_benchmark'::job_intent
        WHEN 'ad_hoc'   THEN 'adhoc_benchmark'::job_intent
    END,
    task_kind    = 'benchmark'::task_kind,
    build_target = 'stacks_bench'::build_target
WHERE source IS NULL;
