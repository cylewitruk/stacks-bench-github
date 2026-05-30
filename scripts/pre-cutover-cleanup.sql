-- Pre-cutover cleanup (Phase 2, slice 11).
--
-- Run ONCE, during the cutover quiet window, AFTER the legacy `jobs`
-- queue has drained and BEFORE starting the services on the new `v2`
-- backend. Clears the new pipeline's accumulated state (slices 5–10 ran
-- the processor in shadow mode, so `job` / `github_webhook` hold rows
-- that pre-date the cutover) so post-cutover behaviour starts from a
-- known-empty state.
--
-- Usage (against the production database, as the DB owner):
--   psql "$DATABASE_URL" -f scripts/pre-cutover-cleanup.sql
--
-- CASCADE on `job` removes its dependents: job_event, job_metric,
-- job_result, github_webhook_job, github_user_job, github_pull_request_job.
TRUNCATE job CASCADE;

-- Wipe the accumulated inbox so the processor starts from empty (its
-- github_webhook_job links were already removed by the TRUNCATE above).
TRUNCATE github_webhook CASCADE;

-- PRESERVED (NOT truncated): allowed_installer, github_installation,
-- github_installation_repo, github_repo (+ lineage), supported_repo_root,
-- target_repo_policy, source_repo_policy, trigger_policy, github_user,
-- github_user_role, github_pull_request — all the Phase 1 state the
-- post-cutover pipeline keeps using.
--
-- The legacy `jobs` table is left alone here; it is dropped in slice 12
-- after a soak period.
