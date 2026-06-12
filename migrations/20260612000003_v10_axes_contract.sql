-- v10 Phase 2c (item 0005): contract step — the job-model axes are now the
-- source of truth. Retire the legacy `(trigger_kind, job_kind)` columns on
-- `job` and make the four axes NON-NULL.
--
-- Safe as one step on a single-daemon, migrate-at-startup deployment: the prior
-- binary is stopped before this runs, and the new binary (which references only
-- the axes) serves after. The 2a creation rewire + 2b straggler back-fill have
-- already populated every live row, so `SET NOT NULL` can't fail on real data.

-- 1. Drop every index that references `job_kind` (column or partial predicate).
--    Postgres won't drop a column an index depends on; do it explicitly so the
--    recreate below is the deliberate, reviewable successor set.
DROP INDEX IF EXISTS job_repo_kind_idx;            -- (github_repo_id, job_kind)
DROP INDEX IF EXISTS job_baseline_exact_idx;       -- v7 workload-keyed exact
DROP INDEX IF EXISTS job_baseline_ref_timeline_idx;-- v7 workload-keyed nearest
DROP INDEX IF EXISTS job_baseline_commit_idx;      -- slice-8, superseded by v7
DROP INDEX IF EXISTS job_baseline_timeline_idx;    -- slice-8, superseded by v7

-- 2. Drop the retired columns. `trigger_kind` is now wholly carried by `source`
--    (+ `git_ref_kind`); `job_kind` by `intent`. The `trigger_kind` TYPE stays
--    (still used by `trigger_policy`); the now-orphan `job_kind` TYPE is left
--    for a later cleanup so this slice touches only the `job` table.
ALTER TABLE job DROP COLUMN trigger_kind;
ALTER TABLE job DROP COLUMN job_kind;

-- 3. The axes are guaranteed populated — enforce it.
ALTER TABLE job ALTER COLUMN source       SET NOT NULL;
ALTER TABLE job ALTER COLUMN intent       SET NOT NULL;
ALTER TABLE job ALTER COLUMN task_kind    SET NOT NULL;
ALTER TABLE job ALTER COLUMN build_target SET NOT NULL;

-- 4. Recreate the live baseline-lookup indexes, now predicated on the `intent`
--    axis so `find_baseline_for`'s `intent = 'baseline_benchmark'` filter is
--    index-backed (mirrors the retired v7 pair, swapping the predicate column).
--    The slice-8 repo-leading pair is NOT recreated — it never served the
--    repo-agnostic baseline query (v7 superseded it).
CREATE INDEX job_baseline_exact_idx
    ON job (git_commit_hash, workload_key)
    WHERE intent = 'baseline_benchmark' AND status = 'completed';

CREATE INDEX job_baseline_ref_timeline_idx
    ON job (git_ref_display, workload_key, git_committed_at DESC)
    WHERE intent = 'baseline_benchmark' AND status = 'completed';

-- 5. Per-repo job listing, now keyed on the `intent` axis (successor to the
--    retired `job_repo_kind_idx`).
CREATE INDEX job_repo_intent_idx
    ON job (github_repo_id, intent);
