//! GitHub API client abstraction.
//!
//! The trait `GitHubApi` is the boundary between our handler/orchestrator and
//! GitHub itself; everything in the rest of the codebase depends on the trait,
//! not on `octocrab`. `OctocrabClient` is the real implementation; a fake
//! implementation lives under the `testing` feature for use in tests.

use async_trait::async_trait;
use octocrab::Octocrab;
use octocrab::models::CommentId;

use crate::github::auth::InstallationTokenCache;
use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostedComment {
    pub id: i64,
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
}

/// Subset of the `/repos/{owner}/{repo}/pulls/{number}` response the
/// slice 5 PR + comment handlers need. `head` is the source repo +
/// branch; `base` is the target repo + branch. Both `RepoRef`s have
/// `id`, `owner`, `name` — enough to (a) upsert identity and (b)
/// look up policy rows by the FK columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullRequestSummary {
    pub number: u64,
    pub head: PullRequestSide,
    pub base: PullRequestSide,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullRequestSide {
    pub repo: RepoRef,
    pub sha: String,
    pub branch: String,
}

/// Production `GitHubApi` implementation backed by `octocrab`.
#[derive(Clone)]
pub struct OctocrabClient {
    tokens: InstallationTokenCache,
}

impl OctocrabClient {
    pub fn new(tokens: InstallationTokenCache) -> Self {
        Self { tokens }
    }

    async fn installation_client(&self, installation_id: i64) -> Result<Octocrab> {
        let token = self
            .tokens
            .token_for(installation_id)
            .await?;
        let client = Octocrab::builder()
            .personal_token(token)
            .build()?;
        Ok(client)
    }
}

#[async_trait]
impl GitHubApi for OctocrabClient {
    async fn create_pr_comment(
        &self,
        installation_id: i64,
        repository: &str,
        pr_number: u64,
        body: &str,
    ) -> Result<PostedComment> {
        let (owner, repo) = split_repo(repository)?;
        let client = self
            .installation_client(installation_id)
            .await?;
        let comment = client
            .issues(owner, repo)
            .create_comment(pr_number, body)
            .await?;
        Ok(PostedComment { id: comment.id.0 as i64 })
    }

    async fn update_pr_comment(
        &self,
        installation_id: i64,
        repository: &str,
        comment_id: i64,
        body: &str,
    ) -> Result<PostedComment> {
        let (owner, repo) = split_repo(repository)?;
        let client = self
            .installation_client(installation_id)
            .await?;
        let comment = client
            .issues(owner, repo)
            .update_comment(CommentId(comment_id as u64), body)
            .await?;
        Ok(PostedComment { id: comment.id.0 as i64 })
    }

    async fn pr_head_sha(
        &self,
        installation_id: i64,
        repository: &str,
        pr_number: u64,
    ) -> Result<String> {
        let (owner, repo) = split_repo(repository)?;
        let client = self
            .installation_client(installation_id)
            .await?;
        let pr = client
            .pulls(owner, repo)
            .get(pr_number)
            .await?;
        Ok(pr.head.sha)
    }

    async fn get_repository(
        &self,
        installation_id: i64,
        owner: &str,
        name: &str,
    ) -> Result<RepoSummary> {
        let client = self
            .installation_client(installation_id)
            .await?;
        let repo = client
            .repos(owner, name)
            .get()
            .await?;
        Ok(repo_summary_from_octocrab(&repo))
    }

    async fn get_pull_request(
        &self,
        installation_id: i64,
        repository: &str,
        pr_number: u64,
    ) -> Result<PullRequestSummary> {
        let (owner, repo) = split_repo(repository)?;
        let client = self
            .installation_client(installation_id)
            .await?;
        let pr = client
            .pulls(owner, repo)
            .get(pr_number)
            .await?;
        // octocrab's PullRequest exposes head/base as distinct concrete
        // types (Head vs Base) even though they share fields. Extract
        // the bits we care about manually rather than fight the
        // generics.
        let head_repo = pr
            .head
            .repo
            .as_ref()
            .ok_or_else(|| Error::Config("PR head missing repo (orphaned ref?)".into()))?;
        let base_repo = pr
            .base
            .repo
            .as_ref()
            .ok_or_else(|| Error::Config("PR base missing repo".into()))?;
        Ok(PullRequestSummary {
            number: pr_number,
            head: PullRequestSide {
                repo: repo_ref_from_octocrab(head_repo),
                sha: pr.head.sha.clone(),
                branch: pr.head.ref_field.clone(),
            },
            base: PullRequestSide {
                repo: repo_ref_from_octocrab(base_repo),
                sha: pr.base.sha.clone(),
                branch: pr.base.ref_field.clone(),
            },
        })
    }
}

fn repo_summary_from_octocrab(repo: &octocrab::models::Repository) -> RepoSummary {
    RepoSummary {
        id: repo.id.0 as i64,
        owner: repo
            .owner
            .as_ref()
            .map(|o| o.login.clone())
            .unwrap_or_default(),
        name: repo.name.clone(),
        default_branch: repo.default_branch.clone(),
        is_fork: repo.fork.unwrap_or(false),
        parent: repo
            .parent
            .as_deref()
            .map(repo_ref_from_octocrab),
        source: repo
            .source
            .as_deref()
            .map(repo_ref_from_octocrab),
    }
}

fn repo_ref_from_octocrab(repo: &octocrab::models::Repository) -> RepoRef {
    RepoRef {
        id: repo.id.0 as i64,
        owner: repo
            .owner
            .as_ref()
            .map(|o| o.login.clone())
            .unwrap_or_default(),
        name: repo.name.clone(),
    }
}

fn split_repo(full_name: &str) -> Result<(&str, &str)> {
    full_name
        .split_once('/')
        .ok_or_else(|| Error::Config(format!("invalid repository: {full_name}")))
}
