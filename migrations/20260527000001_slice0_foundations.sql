-- Slice 0: foundations for the target schema rollout. Additive only —
-- no legacy `jobs` columns/grants are touched. See docs/roadmap.md.
--
-- Lands:
--   - pgcrypto extension (gen_random_uuid() used by future `job.id`)
--   - set_updated_at() trigger helper (used by every mutable table)
--   - all enum types that don't depend on a table existing yet
--
-- job_status is intentionally NOT re-created here; the legacy init
-- migration already defines it with values identical to the target
-- schema, so later slices reuse it.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Bumps updated_at on row mutation. Attached to every table with
-- mutable rows; write-once tables don't use it.
CREATE FUNCTION set_updated_at() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$;

-- Used by github_installation.account_type, github_user.user_type,
-- allowed_installer.account_type.
CREATE TYPE github_account_type AS ENUM (
    'user',
    'organization',
    'bot'
);

CREATE TYPE user_role AS ENUM (
    'admin',
    'trigger_pr_benchmark',
    'view_results'
);

CREATE TYPE job_kind AS ENUM (
    'ad_hoc',
    'baseline'
);

-- Policy-level reason a job was created. Discriminator for
-- job_event.detail when event_kind='queued'.
CREATE TYPE trigger_kind AS ENUM (
    'pr_comment',
    'branch_push',
    'tag_created',
    'scheduled',
    'manual'
);

CREATE TYPE git_ref_kind AS ENUM (
    'branch',
    'tag',
    'commit'
);

CREATE TYPE job_event_kind AS ENUM (
    'queued',
    'claimed',
    'provision_started',
    'provision_finished',
    'phase_build_started',
    'phase_build_finished',
    'phase_bench_started',
    'phase_bench_finished',
    'teardown_started',
    'teardown_finished',
    'comment_posted',
    'comment_updated',
    'completed',
    'failed',
    'cancelled'
);

CREATE TYPE job_event_status AS ENUM (
    'started',
    'in_progress',
    'success',
    'fail'
);

-- Lifecycle dimension for github_webhook rows. Distinct from outcome:
-- status is "where in processing is this?" (used by the processor's
-- claim query and ops dashboards), outcome is "what was the specific
-- decision?".
CREATE TYPE github_webhook_status AS ENUM (
    'received',
    'processing',
    'processed',
    'ignored',
    'denied',
    'retryable_error',
    'failed'
);

-- Closed vocabulary; the specific processor decision attached to a
-- terminal webhook row. Decided by the orchestrator (the processor),
-- NOT the web-facing handler.
--
-- Note: unsupported X-GitHub-Event types are dropped by the handler
-- without inserting a webhook row, so 'ignored_event_type' is
-- intentionally absent.
CREATE TYPE github_webhook_outcome AS ENUM (
    'enqueued_job',
    'ignored_action',
    'ignored_no_command',
    'ignored_unknown_installation',
    'ignored_unsupported_lineage',
    'denied_install_allowlist',
    'denied_target_policy',
    'denied_source_policy',
    'denied_unauthorized',
    'error'
);
