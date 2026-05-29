-- Slice 9: add `processed_pull_request` to `github_webhook_outcome`.
--
-- Slice 9 wires the processor to create real `job` rows for the three
-- triggers with a clean `trigger_kind` (`pr_comment`, `branch_push`,
-- `tag_created`) — those accept paths flip from `would_enqueue_job` to
-- `enqueued_job`.
--
-- The `pull_request.{opened,reopened,synchronize}` accept path is NOT a
-- job-creating path: there is no `trigger_kind` for PR-event auto-bench,
-- and auto-benching every PR push is a separate product decision. That
-- path still materialises/updates the `github_pull_request` row (so a
-- later `/benchmark` comment can link to it) but enqueues nothing. To
-- keep the outcome honest — neither a no-op `ignored_action` nor a
-- now-misleading `would_enqueue_job` — it terminates as the new
-- `processed_pull_request`. Same status bucket as `enqueued_job` /
-- `processed_installation` (= `processed`).
--
-- ALTER TYPE ... ADD VALUE is non-transactional in Postgres < 12; we're
-- on 15+ and the new value is not referenced in this migration, so it's
-- safe inside the migration tx.
ALTER TYPE github_webhook_outcome
    ADD VALUE 'processed_pull_request';

-- Slice 9 idempotency guard: at-most-one job per webhook.
--
-- Job creation is the first NON-idempotent side effect in the classify
-- pipeline (every other handler upserts). The inbox is at-least-once:
-- if `complete()` fails after a job is created, or a slow-but-alive
-- processor's claim lease is swept and the row re-claimed, the webhook
-- can be reprocessed — and `github_webhook_job` (slice 8) only had
-- `UNIQUE (job_id)`, which a fresh job UUID never trips, so a retry
-- would mint a SECOND job for the same webhook.
--
-- Slice 9 enqueues exactly one job per accepted webhook (multi-trigger
-- fan-out is deferred), so this UNIQUE constraint makes that the
-- structural truth and lets `create_job_with_links` use
-- `ON CONFLICT (github_webhook_id) DO NOTHING` to treat a retry as an
-- idempotent "already enqueued" success.
--
-- This TEMPORARILY tightens the slice-8 "many jobs per webhook"
-- allowance (PK `(github_webhook_id, job_id)` + `UNIQUE (job_id)`).
-- When fan-out is implemented, the slice that adds it drops this
-- constraint and switches the idempotency key to something per-trigger
-- (e.g. `UNIQUE (github_webhook_id, trigger_policy_id)`).
ALTER TABLE github_webhook_job
    ADD CONSTRAINT github_webhook_job_webhook_uniq UNIQUE (github_webhook_id);
