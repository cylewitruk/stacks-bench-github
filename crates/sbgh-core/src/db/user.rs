//! `UserStore` — slice 6 data layer for `github_user` (processor lazy
//! upsert + CLI list) and `github_user_role` (operator-curated grants,
//! processor read-only at runtime).
//!
//! Processor surface:
//!
//!   - `upsert_user` — lazy identity row on first sighting (sender of
//!     `issue_comment.created`, author of `pull_request.{opened,…}`). PK
//!     conflict refreshes `login` + `user_type` so renames + type flips
//!     propagate.
//!   - `has_role` — runtime gate: does this user hold this role on this
//!     (install, repo) at this moment? `NULL repo` grants wildcard across every
//!     repo in the install.
//!
//! CLI surface:
//!
//!   - `grant_role` / `revoke_role` — operator writes. Both idempotent on
//!     re-delivery (grant via UNIQUE, revoke via "row may be absent").
//!   - `list_roles` — joined to `github_user` so output shows logins, not just
//!     numeric ids.

use async_trait::async_trait;

use crate::Result;
use crate::models::{GithubAccountType, GithubUser, GithubUserRole, UserRole};

/// Payload for `upsert_user`. The processor reads these three fields
/// out of any sender / author object in a webhook payload.
#[derive(Debug, Clone)]
pub struct NewUser {
    pub id: i64,
    pub login: String,
    pub user_type: GithubAccountType,
}

/// Result of `grant_role`. Distinguishes "newly created" from
/// "matched an existing grant" so the CLI can print the right message
/// and re-delivery / repeated `user grant` invocations are
/// observably idempotent.
#[derive(Debug, Clone)]
pub struct GrantRoleOutcome {
    pub role: GithubUserRole,
    pub created: bool,
}

#[async_trait]
pub trait UserStore: Send + Sync + 'static {
    /// Insert-or-refresh a `github_user` row. On PK conflict updates
    /// `login` + `user_type` (display fields) and `updated_at`. Never
    /// fails on duplicate.
    async fn upsert_user(&self, new: &NewUser) -> Result<GithubUser>;

    /// Look up a user by numeric id. Returns None if no row exists.
    /// Used by the CLI's `user revoke --user-id` emergency path; the
    /// processor doesn't need this (it upserts unconditionally).
    async fn lookup_user(&self, user_id: i64) -> Result<Option<GithubUser>>;

    /// Look up a user by login (case-insensitive). Used by `user list
    /// --login foo` and as a sanity check in CLI tests; the processor
    /// keys on numeric id and never goes through login.
    async fn lookup_user_by_login(&self, login: &str) -> Result<Option<GithubUser>>;

    /// Insert a role grant. Idempotent: re-grant of the same
    /// `(user, install, repo, role)` quadruple returns
    /// `created=false` with the existing row. If the existing row
    /// was soft-revoked (`revoked_at IS NOT NULL`), `grant_role`
    /// reactivates it by clearing `revoked_at` — same row, same
    /// `granted_at`, audit preserved. Errors with FK violation if
    /// the user or install doesn't exist; the CLI pre-checks both.
    async fn grant_role(
        &self,
        github_user_id: i64,
        github_installation_id: i64,
        github_repo_id: Option<i64>,
        granted_role: UserRole,
        granted_by_github_user_id: Option<i64>,
    ) -> Result<GrantRoleOutcome>;

    /// Soft-revoke: set `revoked_at = NOW()` on the matching grant.
    /// Returns `Ok(Some(...))` on a real transition (was active),
    /// `Ok(None)` if no matching active row existed — either the
    /// quadruple didn't match anything, OR the matching row was
    /// already revoked. Idempotent for re-runs.
    ///
    /// `github_repo_id = None` matches an install-wide grant;
    /// `Some(id)` matches a repo-scoped grant — NOT the
    /// install-wide one for the same user/install/role pair.
    /// (Revoking an install-wide grant requires `--repo` omitted.)
    async fn revoke_role(
        &self,
        github_user_id: i64,
        github_installation_id: i64,
        github_repo_id: Option<i64>,
        granted_role: UserRole,
    ) -> Result<Option<GithubUserRole>>;

    /// CLI listing. Optional install filter; when None returns every
    /// grant across all installs (small-scale ops). Ordering:
    /// `(install_id, user_id, role)`.
    async fn list_roles(&self, install_id: Option<i64>) -> Result<Vec<GithubUserRole>>;

    /// Slice 6 post-review cascade fix: bulk-soft-revoke every
    /// active repo-scoped grant for `(install_id, repo_id)`. Used by
    /// `InstallationRepositoriesHandler.handle_removed` alongside
    /// `disable_target_and_triggers` and `revoke_membership` so a
    /// `installation_repositories.removed` event cleans up all
    /// downstream state for the removed repo in lockstep.
    ///
    /// Install-wide grants (`github_repo_id IS NULL`) are NOT
    /// touched — they apply to whichever repos the install does or
    /// will have access to, not just the one being removed.
    ///
    /// Returns the number of rows transitioned. Idempotent for
    /// redelivery (`revoked_at IS NULL` predicate skips already-
    /// revoked rows).
    async fn revoke_repo_scoped_grants(
        &self,
        github_installation_id: i64,
        github_repo_id: i64,
    ) -> Result<u64>;

    /// Slice 6 runtime gate, called from `IssueCommentHandler` after
    /// the `/benchmark` payload has been parsed and the PR has been
    /// resolved (so `repo_id` here is the TARGET repo of the PR).
    ///
    /// Returns true iff an ACTIVE grant (`revoked_at IS NULL`) exists
    /// for this `(user, install)` pair where:
    ///   - `granted_role = role` OR `granted_role = admin` (admin implies all
    ///     roles within the same scope), AND
    ///   - `github_repo_id IS NULL` (install-wide) OR `github_repo_id =
    ///     repo_id` (repo-scoped to the exact target repo)
    ///
    /// A repo-scoped grant for a DIFFERENT repo in the same install
    /// does NOT authorize the user on `repo_id`. This is the design
    /// point: `--repo 10` narrows the grant; it doesn't broaden it.
    /// Same scope rule applies to admin grants: a repo-scoped admin
    /// grant only implies on that one repo.
    async fn has_role(
        &self,
        github_user_id: i64,
        github_installation_id: i64,
        repo_id: i64,
        role: UserRole,
    ) -> Result<bool>;
}
