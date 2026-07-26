//! GitHub API contract.
//!
//! [`GitHubApi`] is the provider boundary owned by `sbgh-github`. Application
//! code depends on this contract rather than Octocrab. [`crate::OctocrabClient`]
//! is the production adapter; the `test_support` module contains the
//! feature-gated fake.

use async_trait::async_trait;
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};

use sbgh_core::models::{GithubAccountType, ResolvedCommit};

use crate::GitHubResult as Result;

/// Characters to percent-encode inside a single path segment of a git
/// ref. Covers the URL-structural / unsafe set — most importantly `#`
/// (fragment) and `%` (escape), which a valid git ref CAN contain and
/// which would otherwise corrupt the request. `/` is NOT in the set: we
/// encode per-segment and keep slashes as path separators so GitHub's
/// `commits/{ref}` greedy-captures the multi-segment ref.
const REF_SEGMENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}')
    .add(b'|')
    .add(b'^')
    .add(b'\\')
    .add(b'[')
    .add(b']');

/// Percent-encode a (possibly slashy) git ref for use in a URL path,
/// encoding URL-unsafe characters within each segment while preserving
/// `/` as a path separator. Public so report renderers building
/// github.com URLs encode refs the same way the API client does.
pub fn encode_ref_path(git_ref: &str) -> String {
    git_ref
        .split('/')
        .map(|seg| utf8_percent_encode(seg, REF_SEGMENT).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

/// Build the `compare` basehead path segment
/// `{base_ref}...{head_owner}:{head_ref}` (roadmap-v7 merge-base lookup).
/// Each ref is percent-encoded per-segment (slashy refs like `feat/foo` keep
/// their `/`); the `...` and `:` separators stay literal. The `{owner}:{ref}`
/// head form is cross-fork-safe (and correct for same-repo PRs).
#[doc(hidden)]
pub fn compare_basehead(base_ref: &str, head_owner: &str, head_ref: &str) -> String {
    format!(
        "{}...{}:{}",
        encode_ref_path(base_ref),
        encode_ref_path(head_owner),
        encode_ref_path(head_ref),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostedComment {
    pub id: i64,
}

/// A created/updated Check Run. `html_url` is the link the PR comment points
/// at; persisting both (roadmap-v4) closes the post-create/pre-comment reclaim
/// gap. `html_url` is `Option` because GitHub's response field is nullable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostedCheckRun {
    pub id: i64,
    pub html_url: Option<String>,
}

/// Lifecycle state for a check create/update. We only ever *complete* with a
/// non-blocking conclusion — benchmark reporting is informational, never a
/// hard pass/fail on a PR.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckRunState {
    InProgress,
    Completed(CheckRunConclusion),
}

/// A check's terminal conclusion. We report on whether the benchmark **ran**,
/// not on whether the numbers are "good": `success` when it completed and
/// produced results, `failure` when it failed to run (setup error, VM died,
/// panic, timeout). A slow/regressed-but-completed run is still `success` —
/// perf is data, not a gate. The check is non-required, so a `failure` is a
/// visible signal without blocking the merge. `skipped`/`neutral` are for the
/// Phase-3 placeholder paths (considered-not-run / not-yet-run). `cancelled` is
/// a *deliberately-stopped* run (operator shutdown/abort or crash-orphan
/// recovery) — GitHub renders it neutral-gray, distinct from a red `failure`,
/// so a stopped run doesn't read as a broken benchmark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckRunConclusion {
    Success,
    Failure,
    Neutral,
    Skipped,
    Cancelled,
}

/// Markdown payload shown in the check's Checks-tab panel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckRunOutput {
    pub title: String,
    pub summary: String,
    pub text: Option<String>,
}

/// The mutable payload pushed on a check create/update: its
/// status/conclusion plus the markdown panel. Bundled so create + update
/// share one shape (and to keep the trait methods' arity in check).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckRunUpdate {
    pub state: CheckRunState,
    pub output: CheckRunOutput,
}

/// Subset of the `/repos/{owner}/{repo}` response the processor needs
/// for slice 4 lineage resolution. `parent` is the immediate fork
/// parent; `source` is the ultimate non-fork root (GitHub fills both on
/// the same response for forks). Both are `None` for canonical repos.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoSummary {
    pub id: i64,
    pub owner: String,
    pub name: String,
    pub default_branch: Option<String>,
    pub is_fork: bool,
    pub parent: Option<RepoRef>,
    pub source: Option<RepoRef>,
}

/// Minimal identity for a parent/source ancestor. We don't need the
/// full Repository payload — just enough to insert the identity row
/// and link the FK from a child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoRef {
    pub id: i64,
    pub owner: String,
    pub name: String,
}

#[async_trait]
pub trait GitHubApi: Send + Sync + 'static {
    async fn create_pr_comment(
        &self,
        installation_id: i64,
        repository: &str,
        pr_number: u64,
        body: &str,
    ) -> Result<PostedComment>;

    async fn update_pr_comment(
        &self,
        installation_id: i64,
        repository: &str,
        comment_id: i64,
        body: &str,
    ) -> Result<PostedComment>;

    /// Return the head commit SHA of an open PR.
    async fn pr_head_sha(
        &self,
        installation_id: i64,
        repository: &str,
        pr_number: u64,
    ) -> Result<String>;

    /// Fetch a repo's identity + fork lineage in one call. Used by the
    /// slice 4 `installation_repositories.added` handler to resolve
    /// each repo's lineage before deciding whether to create membership.
    async fn get_repository(
        &self,
        installation_id: i64,
        owner: &str,
        name: &str,
    ) -> Result<RepoSummary>;

    /// Fetch a PR's head + base repo info in one call. Used by the
    /// slice 5 IssueCommentHandler /benchmark branch: the
    /// `issue_comment` payload only carries the PR's url, not its
    /// head/base repo ids — those are needed to evaluate
    /// target/source policies. `repository` is `"owner/name"` form
    /// (matches the payload's `repository.full_name`).
    async fn get_pull_request(
        &self,
        installation_id: i64,
        repository: &str,
        pr_number: u64,
    ) -> Result<PullRequestSummary>;

    /// Resolve a git ref (branch/tag/SHA) to its commit SHA + authored
    /// date. Used by the daemon to resolve `tag_created` jobs at
    /// claim time — the `create` webhook carries the tag name but no
    /// SHA. GitHub dereferences annotated tags to the underlying commit.
    /// `repository` is `"owner/name"` form.
    ///
    /// Ref encoding: `GET /repos/{owner}/{repo}/commits/{ref}`
    /// greedy-captures everything after `/commits/` as the ref (the
    /// GitHub docs spell refs as `heads/NAME` / `tags/NAME` directly in
    /// the path), so slashes are kept as path separators. The implementor
    /// percent-encodes the OTHER URL-unsafe characters a valid git ref
    /// can contain (`#`, `%`, …) per segment — see `encode_ref_path`.
    async fn resolve_commit(
        &self,
        installation_id: i64,
        repository: &str,
        git_ref: &str,
    ) -> Result<ResolvedCommit>;

    /// roadmap-v7: the **merge-base** (fork point) of the base branch and the
    /// PR head, via `GET
    /// /repos/{base}/compare/{base_ref}...{head_owner}:{head_ref}`.
    /// The `{head_owner}:{head_ref}` head form is cross-fork-safe (and correct
    /// for same-repo PRs, where `head_owner == base_owner`). Returns the
    /// merge-base commit + its date, or `None` when there's no common ancestor
    /// or the refs/repo don't resolve (a `404`) — the caller then degrades
    /// to absolute-only, since v7 reporting is non-fatal.
    async fn compare_commits(
        &self,
        installation_id: i64,
        base_repository: &str,
        base_ref: &str,
        head_owner: &str,
        head_ref: &str,
    ) -> Result<Option<ResolvedCommit>>;

    /// Create a Check Run on `head_sha` in `repository` (`owner/name`).
    /// `name` is the check's display name; `external_id` is our stable
    /// reconcile key (the job id). Returns the id + `html_url`.
    async fn create_check_run(
        &self,
        installation_id: i64,
        repository: &str,
        head_sha: &str,
        name: &str,
        external_id: &str,
        update: CheckRunUpdate,
    ) -> Result<PostedCheckRun>;

    /// Update an existing Check Run by id (status/conclusion/output).
    async fn update_check_run(
        &self,
        installation_id: i64,
        repository: &str,
        check_run_id: i64,
        update: CheckRunUpdate,
    ) -> Result<PostedCheckRun>;

    /// Reconcile primitive (roadmap-v4): find an existing Check Run on
    /// `head_sha` whose `external_id` matches — used on the retry path to
    /// reuse a run created just before a crash, rather than creating a
    /// duplicate. Narrows server-side to **our App (`app_id`) + check `name`**
    /// per the "List check runs for a Git reference" filters, then matches
    /// `external_id` exactly. `None` if no prior run exists.
    async fn find_check_run_by_external_id(
        &self,
        installation_id: i64,
        repository: &str,
        head_sha: &str,
        name: &str,
        app_id: i64,
        external_id: &str,
    ) -> Result<Option<PostedCheckRun>>;

    /// The App's own numeric id (`GET /app`, App-JWT auth). Resolved once at
    /// startup to scope the Check Run reconcile filter to our own runs.
    async fn current_app_id(&self) -> Result<i64>;
}

/// Subset of the `/repos/{owner}/{repo}/pulls/{number}` response the
/// slice 5 PR + comment handlers need. `head` is the source repo +
/// branch; `base` is the target repo + branch. Both `RepoRef`s have
/// `id`, `owner`, `name` — enough to (a) upsert identity and (b)
/// look up policy rows by the FK columns.
///
/// Slice 7 added `title` and `author` so the shared PR materialisation
/// helper (`materialise_pr` in webhook_processor) can populate
/// `github_pull_request` from a single API call when the `/benchmark`
/// comment predates the new pipeline's `pull_request.opened` event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullRequestSummary {
    pub number: u64,
    pub head: PullRequestSide,
    pub base: PullRequestSide,
    pub title: String,
    pub author: PullRequestAuthor,
}

/// PR author identity. Slice 7 needs id + login + account type so the
/// shared materialisation helper can lazy-upsert the user before
/// inserting the PR row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullRequestAuthor {
    pub id: i64,
    pub login: String,
    pub account_type: GithubAccountType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullRequestSide {
    pub repo: RepoRef,
    pub sha: String,
    pub branch: String,
}
