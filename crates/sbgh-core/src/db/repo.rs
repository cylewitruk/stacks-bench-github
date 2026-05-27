//! `RepoStore` — slice 4 data-layer trait for `github_repo` (processor
//! read/write identity + lineage cache) and `supported_repo_root` (the
//! operator-curated capability boundary).
//!
//! The processor calls these in three patterns:
//!
//!   1. `installation_repositories.added` — for each repo in the payload:
//!      `lookup_repo(id)` → cache hit → check lineage. Cache miss →
//!      `upsert_repo_lineage(...)` after fetching `/repos/{owner}/{repo}`,
//!      which writes the repo + its parent + its source in one transaction
//!      (topological order). Then check lineage.
//!   2. Lineage check — `is_supported_lineage(repo_id)` evaluates "self OR
//!      fork_root matches an enabled supported_repo_root row."
//!   3. CLI seed — `upsert_repo_identity(...)` (no lineage walk needed for a
//!      canonical root, just identity) + `upsert_supported_root(...)`.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::Result;
use crate::models::{GithubRepo, SupportedRepoRoot};

/// Identity payload for a repo we know exists on GitHub but haven't yet
/// (or don't need to) resolve lineage for. Used by the CLI seed path
/// and as the "skeleton" inserted for parent/source repos before
/// `upsert_repo_lineage` populates their own lineage on a later
/// encounter.
#[derive(Debug, Clone)]
pub struct NewRepoIdentity {
    pub id: i64,
    pub owner: String,
    pub name: String,
    /// Optional — populated when the operator's CLI bootstrap fetched
    /// the repo and has it available. Leave None when inserting a
    /// parent/source skeleton (later refresh will fill it in).
    pub default_branch: Option<String>,
}

/// Full lineage info from `/repos/{owner}/{repo}`. `parent` is the
/// immediate fork parent (one hop up); `source` is the ultimate root
/// (the chain's terminal non-fork). Both are `None` for canonical
/// non-fork repos.
///
/// Convention: for a fork, both fields are populated (source may equal
/// parent if there's only one hop). For a non-fork, both are None and
/// `is_fork=false`.
#[derive(Debug, Clone)]
pub struct NewRepoLineage {
    pub repo: NewRepoIdentity,
    pub is_fork: bool,
    pub parent: Option<NewRepoIdentity>,
    pub source: Option<NewRepoIdentity>,
}

#[async_trait]
pub trait RepoStore: Send + Sync + 'static {
    /// Look up a repo by GH numeric id. Returns `None` for a cache miss
    /// (the caller fetches via GH API and calls `upsert_repo_lineage`).
    async fn lookup_repo(&self, github_repo_id: i64) -> Result<Option<GithubRepo>>;

    /// Insert (or refresh) just the identity of a repo. Used by the
    /// CLI seed path where we have owner/name/id but no need to walk
    /// fork chains, AND internally as the "skeleton" insert for a
    /// fork's parent/source before their own lineage walk runs.
    ///
    /// On PK conflict: refreshes mutable identity fields
    /// (owner/name/default_branch). Does NOT touch is_fork or lineage
    /// columns — those are owned by `upsert_repo_lineage`.
    async fn upsert_repo_identity(&self, identity: &NewRepoIdentity) -> Result<GithubRepo>;

    /// Insert or refresh a repo with full fork lineage. Runs in a
    /// transaction with topological inserts: source first, then parent
    /// (if different), then the repo itself. `lineage_checked_at` is
    /// set to NOW() on the leaf row so a future refresher can skip
    /// recently-checked repos.
    ///
    /// On PK conflict for the leaf: updates ALL fields including the
    /// lineage columns. Caller is responsible for ensuring the GH API
    /// response was fresh.
    async fn upsert_repo_lineage(&self, lineage: &NewRepoLineage) -> Result<GithubRepo>;

    /// True iff the repo's id OR its `fork_root_github_repo_id` matches
    /// a `supported_repo_root` row with `is_enabled=TRUE`. Returns
    /// false for repos we don't know about (caller should
    /// upsert-then-recheck if it just fetched lineage).
    async fn is_supported_lineage(&self, github_repo_id: i64) -> Result<bool>;

    /// Upsert a `supported_repo_root` row. Requires the corresponding
    /// `github_repo` row to exist (FK). On PK conflict: refreshes
    /// `is_enabled=TRUE` + note. Used by `sbgh-cli repo allow`.
    async fn upsert_supported_root(
        &self,
        github_repo_id: i64,
        note: Option<&str>,
    ) -> Result<SupportedRepoRoot>;

    /// Set `is_enabled=FALSE` on the `supported_repo_root` row for this
    /// repo id (soft-disable, audit-preserving). Returns `None` if no
    /// row matched. Used by `sbgh-cli repo disable`.
    async fn disable_supported_root(
        &self,
        github_repo_id: i64,
    ) -> Result<Option<SupportedRepoRoot>>;

    /// List every `supported_repo_root` joined back to its
    /// `github_repo` identity, for `sbgh-cli repo list`.
    async fn list_supported_roots(&self) -> Result<Vec<SupportedRoot>>;
}

/// Joined view used by `sbgh-cli repo list`. Combines the operator-set
/// columns with the repo identity so the CLI can print owner/name
/// instead of just a numeric id.
#[derive(Debug, Clone)]
pub struct SupportedRoot {
    pub github_repo_id: i64,
    pub owner: String,
    pub name: String,
    pub is_enabled: bool,
    pub note: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
