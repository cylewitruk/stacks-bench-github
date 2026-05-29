-- ╔════════════════════════════════════════════════════════════════════════╗
-- ║  TARGET SCHEMA — DESIGN ARTIFACT, NOT A MIGRATION                      ║
-- ╠════════════════════════════════════════════════════════════════════════╣
-- ║  Placed under `_design/` so sqlx-migrate does not pick it up. Do NOT   ║
-- ║  run this directly.                                                    ║
-- ║                                                                        ║
-- ║  Requires Postgres 15+ (NULLS NOT DISTINCT on unique indexes).         ║
-- ║                                                                        ║
-- ║  Design principle: SUBJECT vs PROVENANCE.                              ║
-- ║    SUBJECT  ("what is this job about?") earns typed relational         ║
-- ║      structure — it appears in queries (e.g. "all jobs for PR 123").   ║
-- ║    PROVENANCE ("what envelope caused enqueue?") lives in               ║
-- ║      job_event.detail JSONB — append-only, no query surface beyond     ║
-- ║      audit / inspection.                                               ║
-- ╚════════════════════════════════════════════════════════════════════════╝
-- ─── Generic helpers ─────────────────────────────────────────────────────
CREATE FUNCTION set_updated_at ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

-- ─── Enums ───────────────────────────────────────────────────────────────
-- Used by both github_installation.account_type and github_user.user_type.
CREATE TYPE github_account_type AS ENUM (
    'user',
    'organization',
    'bot'
);

CREATE TYPE user_role AS ENUM (
    'admin', -- IMPLIES all other roles within the same grant scope (install or repo)
    'trigger_pr_benchmark', -- can post /benchmark on PRs
    'view_results' -- read-only
);

-- Pre-Phase-2 checkpoint: `claimed` is the intermediate state between
-- `queued` and `running`. The orchestrator's claim path transitions
-- queued → claimed (with a claim_token), then claimed → running when
-- execution actually starts. Stuck-claim recovery resets `claimed`
-- rows whose claimed_at exceeds the lease back to `queued`.
CREATE TYPE job_status AS ENUM (
    'queued',
    'claimed',
    'running',
    'completed',
    'failed',
    'cancelled'
);

CREATE TYPE job_kind AS ENUM (
    'ad_hoc', -- PR /benchmark, manual CLI
    'baseline' -- develop bench, release-tag bench
);

-- Policy-level reason this job was created. Discriminator for
-- job_event.detail when event_kind='queued'. The specific watched
-- branch / tag pattern is in job.git_ref_display.
CREATE TYPE trigger_kind AS ENUM (
    'pr_comment',
    -- /benchmark on a PR
    'branch_push',
    -- watched branch advanced (e.g. push to develop)
    'tag_created',
    -- watched tag pattern appeared (e.g. release tag)
    'scheduled',
    -- timer fired
    'manual' -- operator CLI / API
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
-- decision?". Retryable vs permanent failure is the key distinction —
-- retryable_error rows are reclaimed by the processor after
-- next_attempt_at; failed rows are terminal (attempts exhausted).
CREATE TYPE github_webhook_status AS ENUM (
    'received', -- in queue, not yet claimed
    'processing', -- claimed by a processor, in flight
    'processed', -- terminal: outcome='enqueued_job'
    'ignored', -- terminal: outcome='ignored_*'
    'denied', -- terminal: outcome='denied_*'
    'retryable_error', -- transient failure, eligible for re-claim after next_attempt_at
    'failed' -- terminal: retries exhausted
);

-- Closed vocabulary; the specific processor decision attached to a
-- terminal webhook row. Decided by the orchestrator (the processor),
-- NOT the web-facing handler — the handler has no GitHub credentials
-- and so cannot evaluate allowlists, lineage, or policies.
--
-- Note: unsupported X-GitHub-Event types are dropped by the handler
-- without ever inserting a webhook row (no FK / token cost). So
-- 'ignored_event_type' is intentionally absent here. Per-action
-- filtering (e.g. issue_comment.deleted) IS persisted as 'ignored_action'
-- because the event type was handled but the action wasn't.
CREATE TYPE github_webhook_outcome AS ENUM (
    'enqueued_job',
    'would_enqueue_job', -- Phase 1 shadow accept: the new pipeline would have created a job at slice 9; legacy handler is still the actual job source
    'processed_installation', -- install/membership/policy state mutated successfully (no job)
    'ignored_action',
    'ignored_no_command',
    'ignored_unknown_installation', -- webhook from an installation we have no row for
    'ignored_unsupported_lineage', -- repo not in any supported_repo_root fork tree
    'denied_install_allowlist', -- installation.created for an account not in allowed_installer
    'denied_target_policy', -- repo not opted in as target for this installation
    'denied_source_policy', -- PR source repo not trusted for code execution
    'denied_unauthorized', -- user lacks the required user_role
    'error'
);

-- ─── GitHub identity ─────────────────────────────────────────────────────
-- Operator-curated allowlist of GH accounts permitted to install our App.
-- Checked by the processor on the installation.created webhook: if the
-- account isn't here (or is_enabled=FALSE), no github_installation row is
-- created and the webhook ends with outcome='denied_install_allowlist'.
-- All subsequent webhooks from that installation then no-op as
-- 'ignored_unknown_installation' since the github_installation row is
-- the gate everything else hangs off.
--
-- github_account_id is the natural PK: GH's stable numeric id for the
-- user/org, immune to renames and case. account_login is display-only
-- and refreshed when we see new payloads. Pre-seeding workflow is the
-- operator CLI resolving login → id via GH API once at add time.
--
-- Processor reads account id from payload.installation.account.id; if
-- absent on a given event variant, falls back to
-- GET /app/installations/{installation_id} to resolve before
-- accepting/denying.
CREATE TABLE allowed_installer (
    github_account_id bigint PRIMARY KEY,
    account_login text NOT NULL,
    account_type github_account_type NOT NULL,
    is_enabled boolean NOT NULL DEFAULT TRUE,
    note text,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE TRIGGER allowed_installer_set_updated_at
    BEFORE UPDATE ON allowed_installer
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at ();

-- Natural PK = GH's numeric installation id. github_account_id carries
-- the underlying account identity for rename-safety; FK to
-- allowed_installer enforces that every installation came through the
-- gate (matches our soft-disable-only lifecycle: an allowed_installer
-- row with installations against it can't be deleted, only disabled).
--
-- deleted_at is the soft-delete column; an uninstall webhook sets it to
-- NOW() rather than DELETEing the row, because slice-4 onwards
-- github_installation_repo (and slice-5+ policies) FK back at us and
-- we want their history preserved. Active iff deleted_at IS NULL AND
-- suspended_at IS NULL — app-layer check.
CREATE TABLE github_installation (
    id bigint PRIMARY KEY,
    github_account_id bigint NOT NULL REFERENCES allowed_installer (github_account_id),
    account_login text NOT NULL,
    account_type github_account_type NOT NULL,
    suspended_at timestamptz,
    deleted_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE TRIGGER github_installation_set_updated_at
    BEFORE UPDATE ON github_installation
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at ();

-- Pure repo identity + lineage cache. Natural PK = GH's numeric repo id
-- (stable across renames). Whether any of our installations has access
-- lives in github_installation_repo — this table is installation-agnostic.
--
-- Lineage fields cache GH's fork relationship for the capability gate
-- (supported_repo_root): processor populates is_fork, parent_github_repo_id,
-- fork_root_github_repo_id from GET /repos/{owner}/{repo} on first
-- encounter, then uses fork_root_github_repo_id as the durable gate
-- for forks-of-forks. NULL on these columns = lineage not yet
-- resolved. parent is kept for visualization only; the gate uses the
-- fork root (GH calls it `source` in the API, but we name it
-- fork_root_* to avoid colliding with the PR-source / source-policy
-- meanings of "source" elsewhere in the schema). Stale lineage
-- (rename/transfer) is rare; lineage_checked_at lets a future
-- periodic re-validator handle drift.
--
-- Coherent shapes the resolver should validate (app-layer, not DB):
--   canonical/non-fork: is_fork=FALSE, parent IS NULL, fork_root IS NULL
--   fork:               is_fork=TRUE,  fork_root IS NOT NULL (≠ id)
-- Half-populated rows should be treated as unresolved, not unsupported.
--
-- Self-referential FK ordering: resolving a fork requires upserting
-- the parent and fork-root github_repo rows BEFORE setting the
-- child's lineage columns. The REST repo response includes the parent
-- and source objects with full identity, so this is one transaction
-- with multiple inserts in topological order, not a separate API call
-- per ancestor.
CREATE TABLE github_repo (
    id bigint PRIMARY KEY,
    owner TEXT NOT NULL,
    name text NOT NULL,
    default_branch text,
    is_fork boolean,
    parent_github_repo_id bigint REFERENCES github_repo (id),
    fork_root_github_repo_id bigint REFERENCES github_repo (id),
    lineage_checked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE TRIGGER github_repo_set_updated_at
    BEFORE UPDATE ON github_repo
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at ();

-- GitHub names compare case-insensitively.
CREATE UNIQUE INDEX github_repo_owner_name_lower_uniq ON github_repo (lower(OWNER), lower(name));

-- Operator-curated set of canonical repos this software knows how to
-- benchmark. A repo is acceptable iff its id is here OR it's a fork
-- whose fork_root_github_repo_id is here. Checked BEFORE creating
-- any github_installation_repo row; rejected repos still get a
-- github_repo cache row (audit) but no membership/policy. Operator
-- pre-seeds via CLI; for stacks-network/stacks-core this is the
-- canonical entry.
--
-- Bootstrap order: the FK to github_repo means the canonical repo's
-- github_repo row must exist before the supported_repo_root row.
-- First-run setup CLI does: resolve owner/name → numeric id via GH
-- API, INSERT github_repo (identity only), then INSERT
-- supported_repo_root.
--
-- This is a capability boundary, not a security gate — different
-- conceptually from allowed_installer (which is tenant-creation
-- security) and from target_repo_policy / source_repo_policy (which
-- are per-tenant opt-ins among lineage-supported repos).
CREATE TABLE supported_repo_root (
    github_repo_id bigint PRIMARY KEY REFERENCES github_repo (id),
    is_enabled boolean NOT NULL DEFAULT TRUE,
    note text,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE TRIGGER supported_repo_root_set_updated_at
    BEFORE UPDATE ON supported_repo_root
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at ();

-- Membership row records that the installation has or had access;
-- active iff revoked_at IS NULL. Soft-revoke on uninstall keeps
-- historical job rows that reference the repo queryable. PK doubles
-- as the anchor for composite FKs (job, target_repo_policy,
-- trigger_policy) that need to assert the (installation, repo) pair
-- is known. Currently-active access is an app-layer check.
CREATE TABLE github_installation_repo (
    github_installation_id bigint NOT NULL REFERENCES github_installation (id),
    github_repo_id bigint NOT NULL REFERENCES github_repo (id),
    granted_at timestamptz NOT NULL DEFAULT NOW(),
    revoked_at timestamptz,
    PRIMARY KEY (github_installation_id, github_repo_id)
);

-- Per-installation opt-in to use a repo as a job TARGET (the repo whose
-- events we'll process and post comments on). Composite FK back to
-- github_installation_repo enforces that the (installation, repo) pair
-- is *known* (membership row exists), but NOT that it's currently active
-- — app code must check github_installation_repo.revoked_at IS NULL
-- separately. Similarly, "re-install doesn't silently resurrect prior
-- policy" is an app-layer invariant: when the processor handles an
-- uninstall webhook it must set is_enabled=FALSE on any policies for the
-- revoked membership in the same transaction as revoked_at=NOW().
--
-- Lifecycle: rows are soft-disabled (is_enabled=FALSE), never deleted.
-- This preserves audit history and is why trigger_policy can safely
-- FK at us without ON DELETE CASCADE. The UI should expose
-- enable/disable, not delete.
CREATE TABLE target_repo_policy (
    github_installation_id bigint NOT NULL,
    github_repo_id bigint NOT NULL,
    is_enabled boolean NOT NULL DEFAULT FALSE,
    note text,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (github_installation_id, github_repo_id),
    FOREIGN KEY (github_installation_id, github_repo_id) REFERENCES github_installation_repo (github_installation_id, github_repo_id)
);

CREATE TRIGGER target_repo_policy_set_updated_at
    BEFORE UPDATE ON target_repo_policy
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at ();

-- Per-installation trust decision to execute code from a repo as the
-- SOURCE of a benchmark (PR's head fork). No membership requirement —
-- source repos can be forks we have no installation on (anonymous git
-- fetch). The installation here is the one that will run the
-- benchmark, NOT the one that owns the source repo.
CREATE TABLE source_repo_policy (
    github_installation_id bigint NOT NULL REFERENCES github_installation (id),
    github_repo_id bigint NOT NULL REFERENCES github_repo (id),
    is_enabled boolean NOT NULL DEFAULT FALSE,
    note text,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (github_installation_id, github_repo_id)
);

CREATE TRIGGER source_repo_policy_set_updated_at
    BEFORE UPDATE ON source_repo_policy
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at ();

-- Lazily upserted on first sighting.
CREATE TABLE github_user (
    id bigint PRIMARY KEY,
    login TEXT NOT NULL,
    user_type github_account_type NOT NULL,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE TRIGGER github_user_set_updated_at
    BEFORE UPDATE ON github_user
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at ();

CREATE UNIQUE INDEX github_user_login_lower_uniq ON github_user (lower(login));

-- Per-(user, installation, optional repo, granted_role) grant.
-- Scope hierarchy: installation is the tenant boundary (matches
-- allowed_installer / target_repo_policy / source_repo_policy /
-- trigger_policy), so every grant is scoped to ONE installation;
-- github_repo_id NULL = install-wide grant, github_repo_id NOT NULL =
-- repo-narrowed grant within that install. A grant for "this user on
-- every installation" requires N rows on purpose — there is no
-- cross-installation grant shape, by design.
--
-- Column is `granted_role` (not `role`) to avoid the reserved-keyword
-- collision with PostgreSQL's CREATE ROLE / GRANT TO ROLE syntax.
--
-- Soft-revoke: revoke sets revoked_at = NOW() rather than DELETEing
-- the row, matching the "operator-curated rows soft-disable only"
-- principle. Re-grants clear revoked_at on the existing row
-- (preserving granted_at). has_role at runtime filters
-- revoked_at IS NULL.
--
-- Role implication: an `admin` grant implies all other roles within
-- the SAME scope (install-wide admin → also has trigger_pr_benchmark
-- on any repo in that install; repo-scoped admin → only on that
-- repo). Encoded in `has_role`'s WHERE clause, not in this table —
-- admin grants are stored as their own rows for auditability.
CREATE TABLE github_user_role (
    id bigserial PRIMARY KEY,
    github_user_id bigint NOT NULL REFERENCES github_user (id),
    github_installation_id bigint NOT NULL REFERENCES github_installation (id),
    github_repo_id bigint REFERENCES github_repo (id),
    granted_role user_role NOT NULL,
    granted_at timestamptz NOT NULL DEFAULT NOW(),
    granted_by_github_user_id bigint REFERENCES github_user (id),
    revoked_at timestamptz
);

CREATE UNIQUE INDEX github_user_role_uniq ON github_user_role (github_user_id, github_installation_id, github_repo_id, granted_role) NULLS NOT DISTINCT;

-- Partial index on the active-grants subset for has_role.
CREATE INDEX github_user_role_active_idx ON github_user_role (github_user_id, github_installation_id, granted_role) WHERE revoked_at IS NULL;

-- ─── Trigger policy ─────────────────────────────────────────────────────
-- Per-installation subscriptions for auto-triggered job kinds
-- (branch_push, tag_created — pr_comment/scheduled/manual don't use
-- this table). The processor lists rows for the affected (installation,
-- repo) on each inbound event and matches in code (handful of rows,
-- in-memory iteration cheaper than per-row SQL regex). match_spec
-- shape varies by trigger_kind:
--   branch_push:  {"branch_name": "develop"}
--   tag_created:  {"tag_pattern": "^release/\\d+\\.\\d+\\.\\d+\\.\\d+\\.\\d+$"}
-- Discipline enforced on the orchestrator side as a Rust enum keyed
-- off trigger_kind.
--
-- Composite FK to target_repo_policy: a trigger policy only makes
-- sense for repos this installation has opted in as targets. Same
-- caveat as target_repo_policy itself — the FK proves the pair is
-- known, app code must check is_enabled / revoked_at at runtime.
CREATE TABLE trigger_policy (
    id bigserial PRIMARY KEY,
    github_installation_id bigint NOT NULL,
    github_repo_id bigint NOT NULL,
    trigger_kind trigger_kind NOT NULL,
    match_spec jsonb NOT NULL,
    bench_args text,
    is_enabled boolean NOT NULL DEFAULT TRUE,
    note text,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    FOREIGN KEY (github_installation_id, github_repo_id) REFERENCES target_repo_policy (github_installation_id, github_repo_id)
);

CREATE TRIGGER trigger_policy_set_updated_at
    BEFORE UPDATE ON trigger_policy
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at ();

CREATE INDEX trigger_policy_install_repo_kind_idx ON trigger_policy (github_installation_id, github_repo_id, trigger_kind)
WHERE
    is_enabled;

-- ─── Webhook inbox ──────────────────────────────────────────────────────
-- Inbox/queue between the web-facing handler (token-less) and the
-- privileged processor (the orchestrator, which holds GitHub App
-- credentials).
--
-- Handler responsibility (minimal):
--   1. HMAC signature verification (invalid → log + drop, never inserted)
--   2. event_type allowlist filter (unsupported event names → log + drop)
--   3. INSERT inbox row with status='received', payload populated
--   4. Return 2xx
--
-- Handler does NOT classify outcomes, resolve installations, check
-- lineage, or evaluate policy — none of that is possible without GH
-- API access. All such decisions happen in the processor.
--
-- Processor responsibility:
--   1. Claim with FOR UPDATE SKIP LOCKED on status IN ('received',
--      'retryable_error') AND next_attempt_at <= NOW()
--   2. Set status='processing', claimed_at, claim_token
--   3. Resolve, classify, possibly enqueue job(s)
--   4. On success: status=processed/ignored/denied, outcome=<specific>
--   5. On transient failure: status='retryable_error', bump attempts,
--      set next_attempt_at via backoff, leave payload intact
--   6. When attempts exhausted: status='failed', last_error set
--
-- Installation identity split:
--   payload_installation_id — raw id from the webhook body, written by
--     the handler with NO FK. The handler can't yet know whether the
--     installation row exists (installation.created flows in here too).
--   github_installation_id  — FK'd resolved tenant identity, set by the
--     processor only AFTER the github_installation row exists or has
--     just been created for an accepted install.
--
-- payload retention contract:
--   REQUIRED while status IN ('received', 'processing', 'retryable_error')
--   MAY be cleared by the processor on terminal status IN ('ignored',
--   'denied', 'failed') where no further use is anticipated. For
--   'failed' rows preserve last_error before clearing. NEVER clear
--   from a retryable_error row — that breaks retry.
--
-- payload_size_bytes is set on insert and survives payload clearing,
-- giving ops visibility into payload size distributions and DoS
-- surface even after retention has wiped JSONB bodies.
--
-- Stuck claim recovery: a processor crash after setting status=
-- 'processing' leaves the row claimed indefinitely. Operational
-- pattern: a sweeper transitions 'processing' rows whose claimed_at
-- is older than a lease timeout back to 'retryable_error' so they
-- can be reclaimed.
--
-- Role split (enforced via DB grants at deployment, NOT in this DDL):
--   sbgh_handler  — INSERT on (delivery_id, event_type, action,
--                   payload_installation_id, payload, payload_size_bytes)
--   sbgh_orch     — full SELECT/UPDATE for claim/processing
CREATE TABLE github_webhook (
    id bigserial PRIMARY KEY,
    delivery_id text NOT NULL UNIQUE,
    event_type text NOT NULL,
    action text,
    payload_installation_id bigint,
    -- ON DELETE SET NULL is dormant under the slice-4+ soft-delete
    -- lifecycle (install rows aren't DELETEd anymore) but kept defensively
    -- so a future hard-delete operator action can't strand orphan rows.
    github_installation_id bigint REFERENCES github_installation (id) ON DELETE SET NULL,
    received_at timestamptz NOT NULL DEFAULT NOW(),
    payload jsonb,
    payload_size_bytes integer NOT NULL CHECK (payload_size_bytes >= 0),
    status github_webhook_status NOT NULL DEFAULT 'received',
    outcome github_webhook_outcome,
    claimed_at timestamptz,
    claim_token uuid,
    next_attempt_at timestamptz NOT NULL DEFAULT NOW(),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error text,
    processed_at timestamptz
);

-- Processor claim path. Partial index keeps it small; (next_attempt_at,
-- id) gives deterministic ordering. Covers both initial claims and
-- backoff-retries from retryable_error.
CREATE INDEX github_webhook_claim_idx ON github_webhook (next_attempt_at, id)
WHERE
    status IN ('received', 'retryable_error');

-- ─── Pull requests ───────────────────────────────────────────────────────
-- Distinct source/target repos for cross-fork PRs. pr_number is unique
-- only within the target repo.
--
-- Soft-close via `closed_at` mirrors the lifecycle pattern used by
-- `github_installation_repo.revoked_at` and `github_user_role.revoked_at`:
-- close sets it to NOW(); reopen clears it back to NULL. PRs are
-- historical subjects so the row is preserved for slice 8+ job FKs
-- even after close.
CREATE TABLE github_pull_request (
    id bigserial PRIMARY KEY,
    target_github_repo_id bigint NOT NULL REFERENCES github_repo (id),
    source_github_repo_id bigint NOT NULL REFERENCES github_repo (id),
    pr_number integer NOT NULL,
    title text NOT NULL,
    author_github_user_id bigint NOT NULL REFERENCES github_user (id),
    closed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (target_github_repo_id, pr_number)
);

CREATE TRIGGER github_pull_request_set_updated_at
    BEFORE UPDATE ON github_pull_request
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at ();

-- ─── Jobs ────────────────────────────────────────────────────────────────
-- Subject identity + orchestrator hot-path fields. Provenance, outputs,
-- and history live in job_event / job_metric / job_result; PR
-- association in github_pull_request_job.
--
-- git_commit_hash and git_committed_at are nullable: processor may
-- enqueue with an unresolved ref; later orchestrator phase resolves at
-- job claim. Partial indexes filter on status='completed', so they're
-- populated at lookup.
--
-- Composite FK on (github_installation_id, github_repo_id) targets
-- github_installation_repo, ensuring the (installation, repo) pair is
-- *known*. Currently-active access (revoked_at IS NULL) is an app-layer
-- check at enqueue/claim time — the FK alone doesn't guarantee the
-- membership hasn't been soft-revoked.
-- claim_token + claimed_at are the orchestrator's claim handoff.
-- Invariants (app-layer enforced, not DB CHECKs):
--   status='queued'                  ⟺ claim_token IS NULL AND claimed_at IS NULL
--   status='claimed'                 ⟺ claim_token IS NOT NULL AND claimed_at IS NOT NULL
--   status IN ('running','completed','failed','cancelled')
--                                    : claim_token + claimed_at PRESERVED as audit
--
-- Lifecycle:
--   queued: initial state (slice 9 inserts here)
--   claimed: orchestrator picked it up via FOR UPDATE SKIP LOCKED;
--     execution hasn't started yet
--   running: orchestrator transitioned claimed → running once it
--     actually started the provision phase. claim_token + claimed_at
--     are NOT cleared — they record which orchestrator instance won
--     the claim and when.
--   completed/failed/cancelled: terminal
--
-- The stuck-claim sweep targets `claimed` rows whose claimed_at
-- exceeds the lease window and resets them back to queued AND
-- clears claim_token + claimed_at (matching the inbox sweep shape).
CREATE TABLE job (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid (),
    github_installation_id bigint NOT NULL REFERENCES github_installation (id),
    github_repo_id bigint NOT NULL REFERENCES github_repo (id),
    status job_status NOT NULL DEFAULT 'queued',
    job_kind job_kind NOT NULL,
    trigger_kind trigger_kind NOT NULL,
    git_ref_kind git_ref_kind NOT NULL,
    git_ref_display text NOT NULL,
    git_commit_hash text,
    git_committed_at timestamptz,
    claim_token uuid,
    claimed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    FOREIGN KEY (github_installation_id, github_repo_id) REFERENCES github_installation_repo (github_installation_id, github_repo_id)
);

CREATE TRIGGER job_set_updated_at
    BEFORE UPDATE ON job
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at ();

-- Orchestrator claim path. (created_at, id) for deterministic ordering.
CREATE INDEX job_queued_idx ON job (created_at, id)
WHERE
    status = 'queued';

-- Per-repo job listing by kind.
CREATE INDEX job_repo_kind_idx ON job (github_repo_id, job_kind);

-- Fork-point baseline lookup for /benchmark comparisons.
CREATE INDEX job_baseline_commit_idx ON job (github_repo_id, git_commit_hash)
WHERE
    job_kind = 'baseline' AND status = 'completed';

-- Baseline trend chart.
CREATE INDEX job_baseline_timeline_idx ON job (github_repo_id, git_ref_display, git_committed_at DESC)
WHERE
    job_kind = 'baseline' AND status = 'completed';

-- ─── Subject relations ──────────────────────────────────────────────────
-- Optional 1:1 relation: this job is about a PR.
CREATE TABLE github_pull_request_job (
    job_id uuid PRIMARY KEY REFERENCES job (id) ON DELETE CASCADE,
    github_pull_request_id bigint NOT NULL REFERENCES github_pull_request (id),
    triggering_comment_id bigint,
    -- NULL for non-comment-triggered PR jobs
    created_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX github_pull_request_job_pr_idx ON github_pull_request_job (github_pull_request_id);

-- Webhook→job ingest link. Many jobs per webhook is allowed; at most
-- one webhook per job (enforced by UNIQUE on job_id).
--
-- Under the inbox model, the webhook row is inserted earlier by the
-- handler; the processor later creates the job and link in a single
-- transaction. So the invariant is "no job exists without its link"
-- (not "webhook row + job row are atomic" — those aren't). A processor
-- crash between webhook claim and job-creation transaction is recovered
-- via the webhook's status going back to 'retryable_error' on the
-- next stuck-claim sweep.
CREATE TABLE github_webhook_job (
    github_webhook_id bigint NOT NULL REFERENCES github_webhook (id) ON DELETE CASCADE,
    job_id uuid NOT NULL REFERENCES job (id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (github_webhook_id, job_id),
    UNIQUE (job_id)
);

-- User→job ownership for UI ("my jobs"). Triggering user lives here as
-- typed FK; full event-actor audit trail still lives in job_event.detail.
-- At most one owner per job (UNIQUE on job_id); a user owns many jobs.
-- Owner is set by the processor at job-creation time for triggers with
-- a responsible user (pr_comment, manual). Absent for triggers with no
-- responsible user (branch_push, tag_created, scheduled).
CREATE TABLE github_user_job (
    github_user_id bigint NOT NULL REFERENCES github_user (id),
    job_id uuid NOT NULL REFERENCES job (id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (github_user_id, job_id),
    UNIQUE (job_id)
);

-- "My jobs" listing for UI.
CREATE INDEX github_user_job_user_idx ON github_user_job (github_user_id, created_at DESC);

-- ─── Outcome companions (write-once) ─────────────────────────────────────
-- Promoted bench metrics. Adding/removing columns requires a coordinated
-- change with stacks-bench.
CREATE TABLE job_metric (
    job_id uuid PRIMARY KEY REFERENCES job (id) ON DELETE CASCADE,
    envelope_duration_us bigint NOT NULL CHECK (envelope_duration_us >= 0),
    replay_duration_us bigint NOT NULL CHECK (replay_duration_us >= 0),
    total_duration_us bigint NOT NULL CHECK (total_duration_us >= 0),
    setup_duration_us bigint NOT NULL CHECK (setup_duration_us >= 0),
    execution_duration_us bigint NOT NULL CHECK (execution_duration_us >= 0),
    commit_duration_us bigint NOT NULL CHECK (commit_duration_us >= 0),
    clarity_runtime bigint NOT NULL CHECK (clarity_runtime >= 0),
    transactions bigint NOT NULL CHECK (transactions >= 0),
    read_length bigint NOT NULL CHECK (read_length >= 0),
    write_length bigint NOT NULL CHECK (write_length >= 0),
    measured_blocks bigint NOT NULL CHECK (measured_blocks >= 0),
    warmup_blocks bigint NOT NULL CHECK (warmup_blocks >= 0),
    created_at timestamptz NOT NULL DEFAULT NOW()
);

-- Raw run.json + archive path. run_json NULL for jobs that failed
-- before producing it.
CREATE TABLE job_result (
    job_id uuid PRIMARY KEY REFERENCES job (id) ON DELETE CASCADE,
    run_json jsonb,
    archive_dir text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT NOW()
);

-- ─── Event log ───────────────────────────────────────────────────────────
-- Append-only job timeline. Derive started_at / finished_at / error
-- from this table.
--
-- The queued event's detail carries all queueing provenance (triggering
-- user, bench args, github_webhook reference). Detail schema is a
-- tagged Rust enum keyed off job.trigger_kind.
--
-- github_comment_id populated only on comment_posted / comment_updated.
CREATE TABLE job_event (
    id bigserial PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES job (id) ON DELETE CASCADE,
    event_kind job_event_kind NOT NULL,
    event_status job_event_status NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT NOW(),
    github_comment_id bigint,
    remark text,
    detail jsonb
);

-- Per-job event timeline.
CREATE INDEX job_event_job_occurred_at_idx ON job_event (job_id, occurred_at);

-- Most-recent posted comment per job.
CREATE INDEX job_event_comment_idx ON job_event (job_id, occurred_at DESC)
WHERE
    github_comment_id IS NOT NULL;

