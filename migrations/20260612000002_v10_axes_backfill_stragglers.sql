-- v10 Phase 2b (item 0005): re-run the legacy → axes back-fill for any
-- **straggler** rows the Phase-1 migration missed — jobs created by a pre-2a
-- binary in the window between the Phase-1 migration and the 2a creation rewire
-- leave the axes NULL.
--
-- This is a **deployment-safety** step: from 2b on, `query_as::<_, Job>` decodes
-- the axes as NON-NULL fields, so a single NULL-axis row would fail every job
-- read. Back-filling here (before the 2b binary deploys) closes that trap.
-- `SET NOT NULL` still lands in 2c.
--
-- Robust predicate (any axis NULL, not just `source`); mirrors
-- `JobAxes::from_legacy`. Idempotent — a no-op once every row has axes.
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
WHERE source IS NULL OR intent IS NULL OR task_kind IS NULL OR build_target IS NULL;
