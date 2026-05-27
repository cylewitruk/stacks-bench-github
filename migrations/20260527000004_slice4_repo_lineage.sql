-- Slice 4: repo identity + lineage + per-installation membership.
-- Lands:
--   - github_repo (identity + lineage columns, self-referential FKs)
--   - supported_repo_root (operator-curated canonical roots)
--   - github_installation_repo (per-installation membership, soft-revoke)
--   - github_installation.deleted_at (soft-delete the install row itself,
--     replacing slice 3's hard DELETE so the membership FK doesn't break)
--   - grants on all three new tables + the new install column
--
-- See migrations/_design/target_schema.sql for the rationale per column.
--
-- Lineage semantics summary (encoded in column conventions, not constraints):
--   canonical/non-fork: is_fork=FALSE, parent IS NULL, fork_root IS NULL
--   fork (any depth):   is_fork=TRUE,  fork_root IS NOT NULL
-- A repo is in-scope iff its id OR its fork_root_github_repo_id appears
-- in supported_repo_root with is_enabled=TRUE.

-- ─── github_repo ───────────────────────────────────────────────────────
CREATE TABLE github_repo (
    id BIGINT PRIMARY KEY,
    owner TEXT NOT NULL,
    name TEXT NOT NULL,
    default_branch TEXT,
    is_fork BOOLEAN,
    parent_github_repo_id BIGINT REFERENCES github_repo (id),
    fork_root_github_repo_id BIGINT REFERENCES github_repo (id),
    lineage_checked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER github_repo_touch_updated_at
    BEFORE UPDATE ON github_repo
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- GitHub owner/name compare case-insensitively, so the uniqueness has
-- to lowercase both sides. PK is still the numeric id; this is just a
-- secondary index that prevents accidentally double-inserting "Foo/Bar"
-- and "foo/bar" as separate rows.
CREATE UNIQUE INDEX github_repo_owner_name_lower_uniq
    ON github_repo (LOWER(owner), LOWER(name));

-- ─── supported_repo_root ───────────────────────────────────────────────
-- Capability boundary: "the operator has taught the App how to benchmark
-- this repo family." Different from allowed_installer (tenant-creation
-- security) and from per-installation policies (slices 5+). One row per
-- canonical repo; forks inherit eligibility via fork_root_github_repo_id.
CREATE TABLE supported_repo_root (
    github_repo_id BIGINT PRIMARY KEY REFERENCES github_repo (id),
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER supported_repo_root_touch_updated_at
    BEFORE UPDATE ON supported_repo_root
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ─── github_installation.deleted_at (soft-delete) ──────────────────────
-- Slice 3 ran a hard DELETE on installation.deleted. With slice 4's
-- github_installation_repo FK landing here, that hard DELETE would
-- conflict with any active memberships (FK default is RESTRICT). Switch
-- to soft-delete: deleted_at IS NULL means the install is active; set
-- to NOW() to retire it. Memberships still get revoked_at=NOW() in the
-- same transaction so the active-membership predicate
-- (`revoked_at IS NULL AND deleted_at IS NULL`) is consistent.
ALTER TABLE github_installation
    ADD COLUMN deleted_at TIMESTAMPTZ;

-- ─── github_installation_repo ──────────────────────────────────────────
-- Per-installation repo membership. Composite PK doubles as the anchor
-- for slice 5+ policy/job FKs that need to assert the (installation, repo)
-- pair is known. Currently-active access is an app-layer check
-- (`revoked_at IS NULL` joined with the install's `deleted_at IS NULL`).
CREATE TABLE github_installation_repo (
    github_installation_id BIGINT NOT NULL REFERENCES github_installation (id),
    github_repo_id BIGINT NOT NULL REFERENCES github_repo (id),
    granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMPTZ,
    PRIMARY KEY (github_installation_id, github_repo_id)
);

-- Index for the "revoke all memberships for this install" pattern that
-- installation.deleted runs. The composite PK's leading column is
-- installation_id so this is somewhat redundant, but the partial
-- predicate makes it covering for the active-membership scan that
-- slice 5+ policy resolution will run.
CREATE INDEX github_installation_repo_active_idx
    ON github_installation_repo (github_installation_id, github_repo_id)
    WHERE revoked_at IS NULL;
