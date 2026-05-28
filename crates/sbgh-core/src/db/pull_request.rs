//! `PullRequestStore` — slice 7 data layer for `github_pull_request`.
//!
//! Processor surface:
//!
//!   - `upsert_pull_request` — materialise (or refresh) a PR row on
//!     `pull_request.opened` / `.reopened` / `.synchronize` / `.edited`, and on
//!     the first `/benchmark` comment when the PR pre-dates the new pipeline
//!     (the slice 7 shared materialisation helper covers both code paths).
//!   - `set_closed_at` — set/clear the soft-close timestamp on
//!     `pull_request.closed` / `.reopened`.
//!   - `lookup_pull_request` — used by slice 9 to find the PR row when linking
//!     a new job.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::Result;
use crate::models::GithubPullRequest;

/// Payload for `upsert_pull_request`. The immutable identity fields
/// (target+source repo ids, pr_number, author) are matched against
/// the `(target_repo, pr_number)` unique key; mutable fields (title)
/// are refreshed on conflict.
///
/// `source_github_repo_id` may equal `target_github_repo_id` for
/// internal PRs; the schema allows it (separate FKs to the same
/// `github_repo` row).
#[derive(Debug, Clone)]
pub struct NewPullRequest {
    pub target_github_repo_id: i64,
    pub source_github_repo_id: i64,
    pub pr_number: i32,
    pub title: String,
    pub author_github_user_id: i64,
}

#[async_trait]
pub trait PullRequestStore: Send + Sync + 'static {
    /// Insert-or-refresh a PR row keyed by `(target_repo, pr_number)`.
    /// On conflict the title is refreshed (mutable) and `updated_at`
    /// bumped; the immutable fields (source_repo, author) are NOT
    /// touched even if the payload-derived values differ — slice 7's
    /// contract is that those fields are immutable across the PR's
    /// lifetime. A future debug branch could log mismatches.
    ///
    /// Errors with FK violation if author or either repo isn't
    /// known — callers must ensure both `github_user` and
    /// `github_repo` rows exist first (slice 6's `upsert_user` and
    /// slice 4's `upsert_repo_identity` are the slice 7 prerequisites).
    async fn upsert_pull_request(&self, new: &NewPullRequest) -> Result<GithubPullRequest>;

    /// Look up a PR by its target repo + number. Returns None if no
    /// row exists. Slice 9 uses this to resolve the PR row when
    /// linking a job.
    async fn lookup_pull_request(
        &self,
        target_github_repo_id: i64,
        pr_number: i32,
    ) -> Result<Option<GithubPullRequest>>;

    /// Set or clear `closed_at`:
    ///   - `Some(ts)` on `pull_request.closed` (idempotent: setting an
    ///     already-closed row updates the timestamp to the new value, which
    ///     matches GH's "close, reopen, close again" lifecycle)
    ///   - `None` on `pull_request.reopened` (clears the timestamp; idempotent
    ///     if already None)
    ///
    /// Returns the affected row, or None if no PR matches the
    /// (target_repo, pr_number) key (we never saw the .opened
    /// event — the caller's choice whether to treat as ignore
    /// or materialise from API first).
    async fn set_closed_at(
        &self,
        target_github_repo_id: i64,
        pr_number: i32,
        closed_at: Option<DateTime<Utc>>,
    ) -> Result<Option<GithubPullRequest>>;
}
