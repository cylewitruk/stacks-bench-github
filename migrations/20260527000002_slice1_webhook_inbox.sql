-- Slice 1: webhook inbox table. Additive — no legacy `jobs` columns or
-- grants are touched. See docs/roadmap.md.
--
-- Lands:
--   - github_webhook table (the inbox / queue between handler and processor)
--   - github_webhook_claim_idx partial index for processor claims
--
-- github_installation_id column is INTENTIONALLY DEFERRED to slice 3.
-- The FK target (github_installation table) doesn't exist yet, and no
-- code path writes that column until slice 3 anyway. Slice 3 will
-- ALTER TABLE to add it with the FK in place.
--
-- payload retention contract (enforced by application code, not SQL):
--   REQUIRED while status IN ('received', 'processing', 'retryable_error')
--   MAY be cleared on terminal ignored/denied/failed rows. NEVER clear
--   from a retryable_error row — that breaks retry.

CREATE TABLE github_webhook (
    id BIGSERIAL PRIMARY KEY,
    delivery_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    action TEXT,
    payload_installation_id BIGINT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload JSONB,
    payload_size_bytes INTEGER NOT NULL CHECK (payload_size_bytes >= 0),
    status github_webhook_status NOT NULL DEFAULT 'received',
    outcome github_webhook_outcome,
    claimed_at TIMESTAMPTZ,
    claim_token UUID,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT,
    processed_at TIMESTAMPTZ
);

-- Processor claim path. Covers both initial claims (status='received')
-- and backoff-retries (status='retryable_error'). (next_attempt_at, id)
-- gives deterministic ordering for same-timestamp rows.
CREATE INDEX github_webhook_claim_idx
    ON github_webhook (next_attempt_at, id)
    WHERE status IN ('received', 'retryable_error');
