-- Slice 5: per-installation policy tables.
-- Lands:
--   - target_repo_policy (operator opt-in: "this install will benchmark this repo as a target")
--   - source_repo_policy (operator opt-in: "this install trusts this repo as PR source — code can run")
--   - trigger_policy     (operator subscription: branch_push / tag_created matchers + bench_args)
--
-- See migrations/_design/target_schema.sql for the rationale.
--
-- Soft-disable semantic: all three tables default `is_enabled` per the
-- per-table comment (target/source default FALSE — operator opts in;
-- trigger defaults TRUE — once a row exists you presumably want it
-- active until explicitly disabled). Never DELETE — rows are retained
-- for audit + as FK targets for slice 8+ job creation.
--
-- FK shape recap:
--   target_repo_policy → github_installation_repo (composite) — proves
--     the install-repo membership is known (active/revoked checked in
--     app code separately).
--   source_repo_policy → github_installation + github_repo separately
--     (source can be a fork without install access; intentionally NOT
--     gated on membership).
--   trigger_policy → target_repo_policy (composite) — a trigger only
--     makes sense for a repo opted in as a target.

-- ─── target_repo_policy ────────────────────────────────────────────────
CREATE TABLE target_repo_policy (
    github_installation_id BIGINT NOT NULL,
    github_repo_id BIGINT NOT NULL,
    is_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (github_installation_id, github_repo_id),
    FOREIGN KEY (github_installation_id, github_repo_id)
        REFERENCES github_installation_repo (github_installation_id, github_repo_id)
);

CREATE TRIGGER target_repo_policy_touch_updated_at
    BEFORE UPDATE ON target_repo_policy
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ─── source_repo_policy ────────────────────────────────────────────────
CREATE TABLE source_repo_policy (
    github_installation_id BIGINT NOT NULL REFERENCES github_installation (id),
    github_repo_id BIGINT NOT NULL REFERENCES github_repo (id),
    is_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (github_installation_id, github_repo_id)
);

CREATE TRIGGER source_repo_policy_touch_updated_at
    BEFORE UPDATE ON source_repo_policy
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ─── trigger_policy ────────────────────────────────────────────────────
-- Multiple rows per (install, repo) — one per trigger_kind + match_spec
-- combo. trigger_kind values used in slice 5: 'branch_push' and
-- 'tag_created'. 'pr_comment' goes through the IssueCommentHandler's
-- /benchmark path directly (no trigger_policy row needed). 'scheduled'
-- and 'manual' are deferred to slices after job creation lands.
--
-- match_spec is JSONB validated at the application boundary against
-- TriggerMatchSpec (a serde tag = "kind" enum keyed on `branch_name`
-- vs `tag_pattern`). The DB doesn't enforce shape — Rust does.
CREATE TABLE trigger_policy (
    id BIGSERIAL PRIMARY KEY,
    github_installation_id BIGINT NOT NULL,
    github_repo_id BIGINT NOT NULL,
    trigger_kind trigger_kind NOT NULL,
    match_spec JSONB NOT NULL,
    bench_args TEXT,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (github_installation_id, github_repo_id)
        REFERENCES target_repo_policy (github_installation_id, github_repo_id)
);

CREATE TRIGGER trigger_policy_touch_updated_at
    BEFORE UPDATE ON trigger_policy
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Hot path for the slice 5 PushHandler / CreateHandler: "give me all
-- enabled triggers for this (install, repo, kind)" — partial index on
-- is_enabled keeps disabled rows out of the index entirely.
CREATE INDEX trigger_policy_install_repo_kind_idx
    ON trigger_policy (github_installation_id, github_repo_id, trigger_kind)
    WHERE is_enabled;
