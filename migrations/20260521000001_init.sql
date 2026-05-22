-- Initial schema: a single jobs table acting as the work queue.
-- The orchestrator claims rows with `SELECT ... FOR UPDATE SKIP LOCKED`.

CREATE TYPE job_status AS ENUM (
    'queued',
    'running',
    'completed',
    'failed',
    'cancelled'
);

CREATE TABLE jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status          job_status NOT NULL DEFAULT 'queued',
    repository      TEXT NOT NULL,
    pr_number       BIGINT NOT NULL,
    head_sha        TEXT NOT NULL DEFAULT '',
    requested_by    TEXT NOT NULL,
    command         TEXT NOT NULL,
    args            JSONB NOT NULL DEFAULT '{}'::jsonb,
    installation_id BIGINT NOT NULL,
    comment_id      BIGINT,
    queued_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    result          JSONB,
    error           TEXT
);

-- Hot path: orchestrator scanning for the next queued job.
CREATE INDEX jobs_status_queued_at_idx ON jobs (status, queued_at);

-- Quick lookup of a PR's history.
CREATE INDEX jobs_repo_pr_idx ON jobs (repository, pr_number);
