-- Pre-slice-6 design checkpoint: add `would_enqueue_job` to
-- `github_webhook_outcome` so Phase 1 shadow-accepted decisions
-- (slice 5 `/benchmark`, push, and tag-trigger accept paths) are
-- queryable in DB rather than collapsed into `ignored_action`.
--
-- Slice 5 handlers terminate as `ignored_action` on every accept
-- branch today, which makes "would the new pipeline have enqueued?"
-- impossible to answer with a SQL query — we have to grep tracing
-- logs. Once this outcome lands, the four accept paths
-- (IssueCommentHandler /benchmark, PullRequestHandler, PushHandler,
-- CreateHandler) emit `would_enqueue_job`, and slice 9 changes them
-- to `enqueued_job` once new-schema jobs land. Same status bucket
-- as `enqueued_job` and `processed_installation` (= `processed`).
--
-- ALTER TYPE ... ADD VALUE is non-transactional in Postgres < 12.
-- We're on 15+ so this is safe inside the migration tx.
ALTER TYPE github_webhook_outcome
    ADD VALUE 'would_enqueue_job';
