-- Slice 3: allowed_installer + github_installation + installation FK on the
-- inbox. Additive — no legacy `jobs` columns touched. See docs/roadmap.md.
--
-- Lands:
--   - allowed_installer table (operator-curated allowlist)
--   - github_installation table (one row per accepted GH App installation)
--   - github_webhook.github_installation_id column + FK (deferred from slice 1)
--   - github_webhook_outcome 'processed_installation' enum value
--   - touch trigger on both new tables
--
-- Note on enum ALTER: ADD VALUE has historically been forbidden inside a
-- transaction on older Postgres. Postgres 12+ supports it transactionally;
-- our pinned image is 18, so we're safe. The migrate binary runs each .sql
-- file in its own transaction (sqlx behavior), so any failure rolls back
-- cleanly.

-- ─── new outcome value ─────────────────────────────────────────────────
ALTER TYPE github_webhook_outcome ADD VALUE IF NOT EXISTS 'processed_installation';

-- ─── allowed_installer ─────────────────────────────────────────────────
-- Operator-curated allowlist of GH accounts permitted to install the App.
-- Checked by the processor on `installation.created`: if the account isn't
-- here (or is_enabled=FALSE), no github_installation row is created and the
-- webhook ends with outcome='denied_install_allowlist'.
--
-- github_account_id is the natural PK: GH's stable numeric id for the
-- user/org, immune to renames/case. account_login is display-only and
-- refreshed on every operator `installer allow` run.
CREATE TABLE allowed_installer (
    github_account_id BIGINT PRIMARY KEY,
    account_login TEXT NOT NULL,
    account_type github_account_type NOT NULL,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER allowed_installer_touch_updated_at
    BEFORE UPDATE ON allowed_installer
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ─── github_installation ───────────────────────────────────────────────
-- One row per accepted GH App installation. PK is the GH-assigned
-- numeric installation id, which the inbox uses as a FK and every
-- subsequent webhook reports in its payload.
--
-- FK to allowed_installer enforces "every active installation is backed by
-- a current allowlist row". Soft-disabling the allowlist row leaves the
-- installation in place (we don't cascade-delete operator decisions); the
-- processor's allowlist re-check on subsequent webhooks handles that.
--
-- `suspended_at IS NULL` means active. `suspended_at IS NOT NULL` means
-- GitHub's `installation.suspend` webhook fired — orchestrator treats
-- subsequent payload-bearing events from this installation as ignored.
CREATE TABLE github_installation (
    id BIGINT PRIMARY KEY,
    github_account_id BIGINT NOT NULL REFERENCES allowed_installer (github_account_id),
    account_login TEXT NOT NULL,
    account_type github_account_type NOT NULL,
    suspended_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER github_installation_touch_updated_at
    BEFORE UPDATE ON github_installation
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ─── inbox installation FK (deferred from slice 1) ─────────────────────
-- Now that github_installation exists, add the FK column the slice 1
-- comment promised. Nullable because the inbox row is written before the
-- processor resolves the installation (payload_installation_id stays the
-- raw payload value; this is the resolved/validated FK).
--
-- ON DELETE SET NULL so slice 3's hard DELETE of `github_installation`
-- (on installation.deleted) doesn't trip the FK once writers exist for
-- this column. Webhook history is a separate retention concern from the
-- install row's lifecycle; orphaning resolved_installation_id is the
-- right semantic. Slices 4-5 will replace the hard install delete with
-- soft-revoke, at which point the SET NULL becomes mostly dormant.
--
-- Slice 3 itself has NO writer for this column — populating it requires
-- the same lookup logic the install-handler runs at create time, which
-- slice 4+ can plug in once the inbox/install join is actually needed
-- by ops queries. The column ships now because slice 1 promised it; the
-- semantics are pinned now so slice 4+ doesn't need a schema migration.
ALTER TABLE github_webhook
    ADD COLUMN github_installation_id BIGINT
        REFERENCES github_installation (id) ON DELETE SET NULL;
