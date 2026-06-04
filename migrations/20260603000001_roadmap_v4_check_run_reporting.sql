-- roadmap-v4 Phase 2: Check Run reporting.
--
-- The runner now drives a GitHub Check Run alongside (or instead of) the PR
-- comment for `/benchmark` jobs, and posts a commit-level check for baseline
-- jobs. Like the comment id (`comment_posted` event + `github_comment_id`),
-- the check run's id + html_url are persisted on a `check_run_created` timeline
-- event and read back on re-claim — so a crashed-then-reclaimed job UPDATEs the
-- existing check (and can rebuild the "started, see check ↗" comment from the
-- stored URL) rather than creating a duplicate. The URL is stored because the
-- GitHub create response is the only place it's returned cheaply.
--
-- `ADD VALUE IF NOT EXISTS` is transaction-safe on PG 12+ (we're on 18) and
-- idempotent; the new value is not USED in this migration.

ALTER TYPE job_event_kind ADD VALUE IF NOT EXISTS 'check_run_created';

ALTER TABLE job_event ADD COLUMN github_check_run_id  BIGINT;
ALTER TABLE job_event ADD COLUMN github_check_run_url TEXT;

-- Read-back-on-reclaim lookup: newest check-run event for a job.
CREATE INDEX job_event_check_run_idx
    ON job_event (job_id, occurred_at DESC)
    WHERE github_check_run_id IS NOT NULL;
