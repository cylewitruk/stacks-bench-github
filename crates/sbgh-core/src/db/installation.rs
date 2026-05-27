//! `InstallationStore` — slice 3 data-layer trait for `allowed_installer`
//! (read-only from the processor's perspective; CLI-owner writes) and
//! `github_installation` (processor read/write).
//!
//! The processor calls these in two patterns:
//!
//!   1. On `installation.created` — `lookup_allowed(account_id)` to check the
//!      allowlist; on hit, `upsert_installation(...)` to materialise the
//!      install row.
//!   2. On `installation.suspend` / `unsuspend` — `set_suspended(id, ts)` to
//!      flip the timestamp.
//!   3. On `installation.deleted` — `delete_installation(id)`. Slice 3 has no
//!      FK-referencing tables (memberships/policies land in slices 4-5), so
//!      DELETE succeeds cleanly; later slices will replace this with
//!      soft-revoke-and-keep.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::Result;
use crate::models::{AllowedInstaller, GithubAccountType, GithubInstallation};

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

#[async_trait]
pub trait InstallationStore: Send + Sync + 'static {
    /// Look up an allowlist row by GH account id. Returns `Some` even
    /// if `is_enabled=FALSE` — the caller checks the flag and decides
    /// whether to deny (a disabled installer hits the same deny path
    /// as an absent one for `installation.created`).
    async fn lookup_allowed(&self, github_account_id: i64) -> Result<Option<AllowedInstaller>>;

    /// Insert or refresh a `github_installation` row. On PK conflict,
    /// updates `account_login` / `account_type` (in case the account
    /// renamed or switched user↔org) and `updated_at`. `suspended_at`
    /// is intentionally NOT touched here — suspend/unsuspend has its
    /// own entry point so create-after-suspend doesn't accidentally
    /// resurrect a suspended install.
    async fn upsert_installation(&self, new: &NewInstallation) -> Result<GithubInstallation>;

    /// Set `suspended_at` to the supplied timestamp (Some → suspend,
    /// None → unsuspend). No-op (returns Ok(None)) if no row exists
    /// for `installation_id` — the suspend webhook may legitimately
    /// arrive for an install whose `installation.created` we missed
    /// or denied; we don't auto-materialise on suspend.
    async fn set_suspended(
        &self,
        installation_id: i64,
        suspended_at: Option<DateTime<Utc>>,
    ) -> Result<Option<GithubInstallation>>;

    /// Delete the install row. Returns true if a row was removed,
    /// false if no row existed (idempotent for redelivery / unknown
    /// install scenarios). Slices 4-5 will replace this with
    /// soft-revoke of dependent memberships + policies in the same
    /// transaction.
    async fn delete_installation(&self, installation_id: i64) -> Result<bool>;
}
