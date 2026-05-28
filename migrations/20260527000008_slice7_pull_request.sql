-- Slice 7: PR subject model (`github_pull_request`).
--
-- One row per PR, scoped to its target repo (`UNIQUE (target_repo, pr_number)`).
-- Slice 9 will FK at this row via `github_pull_request_job` to link
-- jobs back to the PR that triggered them.
--
-- Cross-fork PRs have distinct target_github_repo_id and
-- source_github_repo_id (the head repo, often a fork of the target).
-- For internal PRs they're the same id.
--
-- Soft-close: `closed_at` is set on `pull_request.closed` and cleared
-- on `pull_request.reopened`. PRs are historical subjects forever
-- (slice 8+ job FKs depend on the row staying), so we never hard-delete.
-- Same lifecycle pattern as `github_installation_repo.revoked_at` and
-- `github_user_role.revoked_at`.
--
-- Mutable fields refreshed by webhook events:
--   - `title` — refreshed on `pull_request.edited`
--   - `closed_at` — set on `.closed`, cleared on `.reopened`
--   - `updated_at` — bumped by the trigger on every UPDATE
-- Immutable after first sighting:
--   - target_github_repo_id, source_github_repo_id, pr_number, author
--   - (GitHub doesn't allow these to change for an existing PR)
CREATE TABLE github_pull_request (
    id BIGSERIAL PRIMARY KEY,
    target_github_repo_id BIGINT NOT NULL REFERENCES github_repo (id),
    source_github_repo_id BIGINT NOT NULL REFERENCES github_repo (id),
    pr_number INTEGER NOT NULL,
    title TEXT NOT NULL,
    author_github_user_id BIGINT NOT NULL REFERENCES github_user (id),
    closed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (target_github_repo_id, pr_number)
);

CREATE TRIGGER github_pull_request_set_updated_at
    BEFORE UPDATE ON github_pull_request
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at ();

-- Per-repo PR listing for ops queries (and slice 9's `/benchmark`
-- comment-to-PR resolution path).
CREATE INDEX github_pull_request_target_idx
    ON github_pull_request (target_github_repo_id, pr_number);
