-- Slice 6: users + roles.
--
-- Two new tables:
--   github_user      — identity-only, lazy-upserted on first sighting.
--   github_user_role — operator-curated per-(user, installation, optional repo, role)
--                      grants. Scope honours the "installation is the tenant
--                      boundary" principle: every grant is scoped to ONE
--                      installation. github_repo_id NULL = install-wide;
--                      github_repo_id NOT NULL = repo-narrowed within that
--                      install. There is no cross-installation grant shape —
--                      granting a user the same role across N installs is
--                      N rows on purpose.
--
-- Slice 6 ACTIVELY USES:
--   - `granted_role = trigger_pr_benchmark` (the `/benchmark` authz gate
--     on issue_comment events)
--   - `granted_role = admin` (implies all roles within the grant's scope —
--     see the post-slice-6 review fix: `has_role(..., trigger_pr_benchmark)`
--     matches `admin` grants too)
--
-- `view_results` is present in the slice-0 enum but unused in Phase 1.
--
-- Soft-revoke semantic (post-slice-6 review fix): `revoked_at` is the
-- soft-delete column. Revokes set `revoked_at = NOW()` rather than
-- DELETEing the row; re-grants clear it back to NULL. Honours the
-- roadmap's "operator-curated rows soft-disable only" principle and
-- preserves the audit trail (who granted what when, who revoked).
-- `has_role` filters `revoked_at IS NULL` at the runtime gate.

-- ─── github_user ──────────────────────────────────────────────────────
-- Natural PK = GH's numeric user id (stable across renames + case).
-- login is display-only and refreshed on every upsert. user_type
-- reuses the github_account_type enum (`user` / `organization` / `bot`).
CREATE TABLE github_user (
    id BIGINT PRIMARY KEY,
    login TEXT NOT NULL,
    user_type github_account_type NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER github_user_set_updated_at
    BEFORE UPDATE ON github_user
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at ();

-- GH logins compare case-insensitively. See pre-slice-6 checkpoint item 6
-- for the accepted-tradeoff rationale on keeping this index.
CREATE UNIQUE INDEX github_user_login_lower_uniq
    ON github_user (lower(login));

-- ─── github_user_role ─────────────────────────────────────────────────
-- Per-(user, installation, optional repo, role) grant.
-- Column name `granted_role` avoids collision with PostgreSQL's reserved
-- CREATE ROLE / GRANT TO ROLE syntax.
--
-- FK to github_installation (NOT NULL): every grant lives inside a tenant.
-- FK to github_repo (NULLABLE): NULL = install-wide grant.
-- FK to github_user for granted_by: NULL allowed (slice 6 CLI operator
-- doesn't track itself as a GH user; future enhancement could).
CREATE TABLE github_user_role (
    id BIGSERIAL PRIMARY KEY,
    github_user_id BIGINT NOT NULL REFERENCES github_user (id),
    github_installation_id BIGINT NOT NULL REFERENCES github_installation (id),
    github_repo_id BIGINT REFERENCES github_repo (id),
    granted_role user_role NOT NULL,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    granted_by_github_user_id BIGINT REFERENCES github_user (id),
    revoked_at TIMESTAMPTZ
);

-- NULLS NOT DISTINCT collapses (user, install, NULL repo, role)
-- duplicates so a second `user grant` with no --repo is rejected
-- rather than silently shadowing the first. Postgres 15+ required.
-- Revoked rows STAY in this index (we re-grant by clearing
-- revoked_at, not by inserting a new row), preserving the original
-- granted_at audit timestamp across revoke/re-grant cycles — the
-- same shape `github_installation_repo` uses.
CREATE UNIQUE INDEX github_user_role_uniq
    ON github_user_role (github_user_id, github_installation_id, github_repo_id, granted_role)
    NULLS NOT DISTINCT;

-- Processor hot-path index: `has_role` lookups filter by (user, install,
-- role) with optional repo wildcard, AND require revoked_at IS NULL.
-- Partial index keeps the active-grants subset small even as the
-- audit history grows over time.
CREATE INDEX github_user_role_active_idx
    ON github_user_role (github_user_id, github_installation_id, granted_role)
    WHERE revoked_at IS NULL;
