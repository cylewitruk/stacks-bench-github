-- Slice 8: new-schema job tables (additive only — no production
-- writers yet; slice 9 wires the processor's job-creation path).
--
-- Adds:
--   - `claimed` value to job_status (per pre-Phase-2 checkpoint, positioned
--     AFTER 'queued' to match the target-schema enum order)
--   - `job` (subject identity + orchestrator hot-path fields; claim handoff
--     columns claim_token + claimed_at)
--   - `job_event` (append-only timeline; provenance lives in detail JSONB)
--   - `job_result`, `job_metric` (write-once outcome companions)
--   - `github_pull_request_job`, `github_webhook_job`, `github_user_job`
--     (subject relations)
--   - All claim/listing/baseline indexes per the target schema
--
-- `claim_token` + `claimed_at` invariants (app-layer enforced, not DB CHECKs):
--   status='queued'  ⇒ claim_token IS NULL AND claimed_at IS NULL
--   status='claimed' ⇒ claim_token IS NOT NULL AND claimed_at IS NOT NULL
--   status IN ('running','completed','failed','cancelled')
--                    : claim_token + claimed_at PRESERVED as audit
-- Stuck-claim sweep: 'claimed' rows past the lease → 'queued', clearing both.
--
-- IMPORTANT: `job_status` is a shared Postgres type. The legacy `jobs`
-- table references the same enum, so `ALTER TYPE ADD VALUE` exposes the
-- new value globally. Slice 8 also adds `JobStatus::Claimed` to the
-- Rust enum so sqlx deserialisation can't crash on a row carrying the
-- new value. The plan is that legacy code never WRITES 'claimed', but
-- the READ path must be defensive.

ALTER TYPE job_status
    ADD VALUE 'claimed' AFTER 'queued';

-- ─── job (subject identity + orchestrator hot path) ───────────────────
CREATE TABLE job (
    id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    github_installation_id   BIGINT NOT NULL REFERENCES github_installation (id),
    github_repo_id           BIGINT NOT NULL REFERENCES github_repo (id),
    status                   job_status NOT NULL DEFAULT 'queued',
    job_kind                 job_kind NOT NULL,
    trigger_kind             trigger_kind NOT NULL,
    git_ref_kind             git_ref_kind NOT NULL,
    git_ref_display          TEXT NOT NULL,
    git_commit_hash          TEXT,
    git_committed_at         TIMESTAMPTZ,
    claim_token              UUID,
    claimed_at               TIMESTAMPTZ,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Composite FK to github_installation_repo guarantees the
    -- (install, repo) pair is *known*; active-membership check
    -- (revoked_at IS NULL) is an app-layer gate at enqueue/claim time.
    FOREIGN KEY (github_installation_id, github_repo_id)
        REFERENCES github_installation_repo (github_installation_id, github_repo_id)
);

CREATE TRIGGER job_set_updated_at
    BEFORE UPDATE ON job
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();

-- Orchestrator claim path. Partial index keeps the queued subset small;
-- (created_at, id) gives deterministic FIFO ordering.
CREATE INDEX job_queued_idx
    ON job (created_at, id)
    WHERE status = 'queued';

-- Per-repo job listing by kind.
CREATE INDEX job_repo_kind_idx
    ON job (github_repo_id, job_kind);

-- Fork-point baseline lookup for /benchmark comparisons.
CREATE INDEX job_baseline_commit_idx
    ON job (github_repo_id, git_commit_hash)
    WHERE job_kind = 'baseline' AND status = 'completed';

-- Baseline trend chart.
CREATE INDEX job_baseline_timeline_idx
    ON job (github_repo_id, git_ref_display, git_committed_at DESC)
    WHERE job_kind = 'baseline' AND status = 'completed';

-- ─── job_event (append-only timeline) ─────────────────────────────────
-- The queued event's detail JSONB carries all queueing provenance
-- (triggering user, bench args, github_webhook reference). Detail
-- schema is a tagged Rust enum keyed off job.trigger_kind.
-- github_comment_id populated only on comment_posted / comment_updated.
CREATE TABLE job_event (
    id                       BIGSERIAL PRIMARY KEY,
    job_id                   UUID NOT NULL REFERENCES job (id) ON DELETE CASCADE,
    event_kind               job_event_kind NOT NULL,
    event_status             job_event_status NOT NULL,
    occurred_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    github_comment_id        BIGINT,
    remark                   TEXT,
    detail                   JSONB
);

CREATE INDEX job_event_job_occurred_at_idx
    ON job_event (job_id, occurred_at);

CREATE INDEX job_event_comment_idx
    ON job_event (job_id, occurred_at DESC)
    WHERE github_comment_id IS NOT NULL;

-- ─── job_metric (write-once promoted bench metrics) ───────────────────
-- Adding/removing columns requires a coordinated change with stacks-bench.
CREATE TABLE job_metric (
    job_id                   UUID PRIMARY KEY REFERENCES job (id) ON DELETE CASCADE,
    envelope_duration_us     BIGINT NOT NULL CHECK (envelope_duration_us >= 0),
    replay_duration_us       BIGINT NOT NULL CHECK (replay_duration_us >= 0),
    total_duration_us        BIGINT NOT NULL CHECK (total_duration_us >= 0),
    setup_duration_us        BIGINT NOT NULL CHECK (setup_duration_us >= 0),
    execution_duration_us    BIGINT NOT NULL CHECK (execution_duration_us >= 0),
    commit_duration_us       BIGINT NOT NULL CHECK (commit_duration_us >= 0),
    clarity_runtime          BIGINT NOT NULL CHECK (clarity_runtime >= 0),
    transactions             BIGINT NOT NULL CHECK (transactions >= 0),
    read_length              BIGINT NOT NULL CHECK (read_length >= 0),
    write_length             BIGINT NOT NULL CHECK (write_length >= 0),
    measured_blocks          BIGINT NOT NULL CHECK (measured_blocks >= 0),
    warmup_blocks            BIGINT NOT NULL CHECK (warmup_blocks >= 0),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─── job_result (raw run.json + archive path) ─────────────────────────
-- run_json NULL for jobs that failed before producing it.
CREATE TABLE job_result (
    job_id                   UUID PRIMARY KEY REFERENCES job (id) ON DELETE CASCADE,
    run_json                 JSONB,
    archive_dir              TEXT NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─── github_pull_request_job (optional 1:1 PR relation) ──────────────
CREATE TABLE github_pull_request_job (
    job_id                   UUID PRIMARY KEY REFERENCES job (id) ON DELETE CASCADE,
    github_pull_request_id   BIGINT NOT NULL REFERENCES github_pull_request (id),
    -- NULL for non-comment-triggered PR jobs (e.g. PR.opened auto-bench
    -- if ever added). For pr_comment trigger_kind this is the comment.id.
    triggering_comment_id    BIGINT,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX github_pull_request_job_pr_idx
    ON github_pull_request_job (github_pull_request_id);

-- ─── github_webhook_job (ingest link) ─────────────────────────────────
-- Many jobs per webhook is allowed (rare; same delivery causing
-- multiple trigger matches); at most one webhook per job (UNIQUE
-- on job_id). Under the inbox model the webhook row is inserted
-- earlier by the handler; the processor later creates the job +
-- link in a single transaction.
CREATE TABLE github_webhook_job (
    github_webhook_id        BIGINT NOT NULL REFERENCES github_webhook (id) ON DELETE CASCADE,
    job_id                   UUID NOT NULL REFERENCES job (id) ON DELETE CASCADE,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (github_webhook_id, job_id),
    UNIQUE (job_id)
);

-- ─── github_user_job (triggering user ownership) ──────────────────────
-- Owner is set by the processor at job-creation time for triggers
-- with a responsible user (pr_comment, manual). Absent for triggers
-- with no responsible user (branch_push, tag_created, scheduled).
CREATE TABLE github_user_job (
    github_user_id           BIGINT NOT NULL REFERENCES github_user (id),
    job_id                   UUID NOT NULL REFERENCES job (id) ON DELETE CASCADE,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (github_user_id, job_id),
    UNIQUE (job_id)
);

CREATE INDEX github_user_job_user_idx
    ON github_user_job (github_user_id, created_at DESC);
