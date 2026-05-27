//! `InstallationStore` — data-layer trait for `allowed_installer`
//! (read-only from the processor; CLI-owner writes) plus
//! `github_installation` (processor read/write) and slice 4's
//! `github_installation_repo` membership lifecycle.
//!
//! The processor calls these in three patterns:
//!
//!   1. `installation.created` — `lookup_allowed(account_id)` to check the
//!      allowlist; on hit, `upsert_installation(...)`.
//!   2. `installation.{suspend,unsuspend}` — `set_suspended(id, ts)`.
//!   3. `installation.deleted` — `delete_installation(id)`, which since slice 4
//!      is a TRANSACTIONAL bulk-revoke of memberships + soft-delete of the
//!      install row (sets `deleted_at = NOW()`). No hard DELETE; we keep the
//!      row so slice 8+ job FKs stay valid.
//!
//!   4. `installation_repositories.{added,removed}` — per-repo
//!      `add_or_restore_membership` / `revoke_membership`. Each method is
//!      single-statement; the handler aggregates per-repo outcomes itself.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::Result;
use crate::models::{
    AllowedInstaller, GithubAccountType, GithubInstallation, GithubInstallationRepo,
};

/// Payload the processor passes when materialising a `github_installation`
/// row from an `installation.created` webhook. PK conflicts (re-delivery,
/// already-existing install) UPDATE the mutable fields; the install's
/// `github_account_id` is immutable.
#[derive(Debug, Clone)]
pub struct NewInstallation {
    pub id: i64,
    pub github_account_id: i64,
    pub account_login: String,
    pub account_type: GithubAccountType,
}

/// Result of `delete_installation`. Slice 4 changed the hard DELETE to a
/// transactional soft-delete + bulk membership revoke; this struct
/// exposes both signals so the processor can pick its outcome and ops
/// can see how much state changed in one call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteInstallationOutcome {
    /// True if a row exists for `installation_id` post-call (regardless
    /// of whether *this* call was the one that set `deleted_at`). False
    /// only when we have no install row at all — the idempotent "we
    /// never accepted this install" case.
    pub install_found: bool,
    /// Number of memberships transitioned from active to revoked by
    /// this specific call. Re-deliveries see 0 (everything already
    /// revoked).
    pub memberships_revoked: u64,
}

#[async_trait]
pub trait InstallationStore: Send + Sync + 'static {
    /// Look up an allowlist row by GH account id. Returns `Some` even
    /// if `is_enabled=FALSE` — the caller checks the flag and decides
    /// whether to deny.
    async fn lookup_allowed(&self, github_account_id: i64) -> Result<Option<AllowedInstaller>>;

    /// Insert or refresh a `github_installation` row. On PK conflict,
    /// updates `account_login` / `account_type` and `updated_at`.
    /// `suspended_at` / `deleted_at` are NOT touched here — each has
    /// its own dedicated path so create-after-suspend or
    /// create-after-delete don't accidentally resurrect terminal state.
    async fn upsert_installation(&self, new: &NewInstallation) -> Result<GithubInstallation>;

    /// Set `suspended_at` to the supplied timestamp (Some → suspend,
    /// None → unsuspend). No-op (returns Ok(None)) if no row exists.
    async fn set_suspended(
        &self,
        installation_id: i64,
        suspended_at: Option<DateTime<Utc>>,
    ) -> Result<Option<GithubInstallation>>;

    /// Slice 4 soft-delete + bulk membership revoke, executed in a single
    /// transaction. Conditional on the install row existing:
    ///   - if NO row exists → returns `install_found=false,
    ///     memberships_revoked=0`
    ///   - if a row exists:
    ///       - sets `deleted_at = NOW()` if currently NULL (sticky on
    ///         re-delivery)
    ///       - sets `revoked_at = NOW()` on every membership whose `revoked_at
    ///         IS NULL`, returning the count
    ///       - returns `install_found=true, memberships_revoked=<count>`
    async fn delete_installation(&self, installation_id: i64) -> Result<DeleteInstallationOutcome>;

    /// Insert a membership row, OR restore it if it was previously
    /// revoked. On PK conflict: clears `revoked_at` (back to active);
    /// `granted_at` is intentionally preserved so the audit trail
    /// shows the original grant timestamp across revoke/re-add cycles.
    ///
    /// Returns `Ok(None)` if the install is missing OR soft-deleted
    /// (`github_installation.deleted_at IS NOT NULL`). This is the
    /// fix for the slice-4 review finding: without the guard, a
    /// delayed `installation_repositories.added` arriving after
    /// `installation.deleted` would restore an active membership on
    /// a retired install, leaving the inconsistent state
    /// `(deleted_at IS NOT NULL, revoked_at IS NULL)`.
    ///
    /// Errors with FK violation if the repo isn't known — caller must
    /// ensure the `github_repo` row exists first.
    async fn add_or_restore_membership(
        &self,
        installation_id: i64,
        github_repo_id: i64,
    ) -> Result<Option<GithubInstallationRepo>>;

    /// Soft-revoke an active membership: sets `revoked_at = NOW()` if
    /// currently NULL. Returns `Ok(Some(...))` on a real transition,
    /// `Ok(None)` if no row matched or it was already revoked
    /// (idempotent for redelivery).
    async fn revoke_membership(
        &self,
        installation_id: i64,
        github_repo_id: i64,
    ) -> Result<Option<GithubInstallationRepo>>;
}
