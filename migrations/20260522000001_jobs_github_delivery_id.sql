-- Idempotency: GitHub redelivers webhooks on 5xx and on operator-triggered
-- "Redeliver" clicks. Track the delivery id (UUID-shaped string from the
-- `X-GitHub-Delivery` header) so we never enqueue the same delivery twice.
--
-- A partial unique index allows multiple NULLs, so historical rows and
-- synthetic events without a delivery id (tests) stay legal.

ALTER TABLE jobs ADD COLUMN github_delivery_id TEXT;

CREATE UNIQUE INDEX jobs_github_delivery_id_uniq
    ON jobs (github_delivery_id)
    WHERE github_delivery_id IS NOT NULL;
