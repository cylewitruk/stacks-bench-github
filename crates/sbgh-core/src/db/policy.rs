//! `PolicyStore` — slice 5 data-layer trait for the three policy
//! tables: `target_repo_policy`, `source_repo_policy`, `trigger_policy`.
//!
//! Two distinct callers shape the trait surface:
//!
//!   1. **The processor** evaluates policies at webhook-eval time:
//!      `lookup_target_policy(install, repo)`, `lookup_source_policy(install,
//!      repo)`, and `list_enabled_triggers(install, repo, kind)`. Read-only, no
//!      side effects.
//!
//!   2. **Admin writes** seed policies — the operator drives them through the
//!      daemon `/api` (which runs `sbgh_core::admin`): `upsert_target_policy`,
//!      `upsert_source_policy`, `add_trigger_policy`, plus the corresponding
//!      disable / list helpers. Operator-curated; the processor only reads
//!      through the trait (and flips `is_enabled` on install-deleted cleanup),
//!      never seeds. Before roadmap-v3 Phase 6 a narrow DB role enforced that;
//!      now the daemon is the single owner, so it's a code-level boundary.
//!
//! Disable-on-install-deleted is NOT in this trait — it lives in
//! `InstallationStore::delete_installation` so the slice-5 cleanup
//! happens in the same transaction as the membership revoke.

use async_trait::async_trait;

use crate::Result;
use crate::models::{
    SourceRepoPolicy, TargetRepoPolicy, TriggerKind, TriggerMatchSpec, TriggerPolicy,
};

#[async_trait]
pub trait PolicyStore: Send + Sync + 'static {
    // ─── Processor read paths ──────────────────────────────────────────

    /// Look up the target-repo policy for (install, repo). Returns
    /// `None` when no row exists. Caller checks `is_enabled` itself —
    /// the processor distinguishes "operator never opted in" from
    /// "operator soft-disabled" for ops-query reasons (both currently
    /// map to the same `DeniedTargetPolicy` outcome, but slice 6+
    /// may want the distinction).
    async fn lookup_target_policy(
        &self,
        install_id: i64,
        repo_id: i64,
    ) -> Result<Option<TargetRepoPolicy>>;

    /// Look up the source-repo policy for (install, repo). Same
    /// semantics as `lookup_target_policy`.
    async fn lookup_source_policy(
        &self,
        install_id: i64,
        repo_id: i64,
    ) -> Result<Option<SourceRepoPolicy>>;

    /// List all enabled trigger_policy rows for (install, repo, kind).
    /// Hot-path uses the partial index on `is_enabled`. Caller then
    /// matches each row's `match_spec` against the inbound ref in code
    /// (handful of rows per repo; in-memory iteration cheaper than
    /// SQL regex). Returns empty vec when no rows match.
    async fn list_enabled_triggers(
        &self,
        install_id: i64,
        repo_id: i64,
        kind: TriggerKind,
    ) -> Result<Vec<TriggerPolicy>>;

    /// List every enabled, **pinned** trigger_policy row across all (install,
    /// repo) whose parent target_repo_policy is also enabled. The v9 binary
    /// cache pin resolver (item 0025) calls this to compute the protected set.
    /// `pinned_until` is NOT filtered here — the resolver applies expiry
    /// against the current time, so an expired pin is treated as unpinned
    /// without a DB write (and the operator view can still surface it as
    /// `expired`).
    async fn list_pinned_triggers(&self) -> Result<Vec<TriggerPolicy>>;

    // ─── CLI write paths ───────────────────────────────────────────────

    /// Upsert a `target_repo_policy` row with `is_enabled = TRUE`.
    /// Requires the (install, repo) pair to already exist in
    /// `github_installation_repo` (FK enforces it). On PK conflict:
    /// re-enables + refreshes the note.
    async fn upsert_target_policy(
        &self,
        install_id: i64,
        repo_id: i64,
        note: Option<&str>,
    ) -> Result<TargetRepoPolicy>;

    /// Soft-disable a `target_repo_policy` row (sets `is_enabled =
    /// FALSE`). Returns `None` if no row matched.
    async fn disable_target_policy(
        &self,
        install_id: i64,
        repo_id: i64,
    ) -> Result<Option<TargetRepoPolicy>>;

    async fn upsert_source_policy(
        &self,
        install_id: i64,
        repo_id: i64,
        note: Option<&str>,
    ) -> Result<SourceRepoPolicy>;

    async fn disable_source_policy(
        &self,
        install_id: i64,
        repo_id: i64,
    ) -> Result<Option<SourceRepoPolicy>>;

    /// Insert a new `trigger_policy` row. Multiple rows per
    /// (install, repo) are intentional — each is a distinct
    /// trigger_kind + match_spec combination. `match_spec` is
    /// pre-validated as a `TriggerMatchSpec` by the caller (CLI);
    /// the store just serialises it into JSONB.
    async fn add_trigger_policy(
        &self,
        install_id: i64,
        repo_id: i64,
        kind: TriggerKind,
        match_spec: &TriggerMatchSpec,
        bench_args: Option<&str>,
        note: Option<&str>,
    ) -> Result<TriggerPolicy>;

    /// Soft-disable a trigger_policy row by its bigserial id.
    /// Returns `None` if no row matched.
    async fn disable_trigger_policy(&self, trigger_id: i64) -> Result<Option<TriggerPolicy>>;

    /// List ALL trigger_policy rows for (install, repo) regardless of
    /// `is_enabled` — used by `sbgh-cli policy trigger list` so the
    /// operator can see disabled rows too.
    async fn list_triggers(&self, install_id: i64, repo_id: i64) -> Result<Vec<TriggerPolicy>>;

    /// Slice 5 review fix — cascade-disable target_repo_policy + all
    /// matching trigger_policy rows for a (install, repo) pair in one
    /// transaction. Called from
    /// `InstallationRepositoriesHandler.handle_removed` BEFORE the
    /// membership revoke, so a subsequent re-add / stale inbox event
    /// doesn't see a stale-enabled policy and incorrectly pass policy
    /// evaluation.
    ///
    /// Idempotent: predicates filter `is_enabled = TRUE`, so re-running
    /// against already-disabled rows is a no-op. Order matters via the
    /// FK chain: triggers FIRST (they FK to target), then target.
    async fn disable_target_and_triggers(&self, install_id: i64, repo_id: i64) -> Result<()>;
}
