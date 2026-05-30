//! GitHub API client abstraction.
//!
//! The trait `GitHubApi` is the boundary between our handler/orchestrator and
//! GitHub itself; everything in the rest of the codebase depends on the trait,
//! not on `octocrab`. `OctocrabClient` is the real implementation; a fake
//! implementation lives under the `testing` feature for use in tests.

use async_trait::async_trait;
use octocrab::Octocrab;
use octocrab::models::CommentId;
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};

use crate::github::auth::InstallationTokenCache;
use crate::models::ResolvedCommit;
use crate::{Error, Result};

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
/// `/` as a path separator.
fn encode_ref_path(git_ref: &str) -> String {
    git_ref
        .split('/')
        .map(|seg| utf8_percent_encode(seg, REF_SEGMENT).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

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

    /// Resolve a git ref (branch/tag/SHA) to its commit SHA + authored
    /// date. Used by the orchestrator to resolve `tag_created` jobs at
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
    pub account_type: crate::models::GithubAccountType,
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
        let author = pr.user.as_ref();
        // GH's REST response uses `User`/`Organization`/`Bot`; map to
        // our typed enum at the boundary (same rationale as elsewhere
        // — bogus type → Error rather than silent default).
        let account_type = match author.r#type.as_str() {
            "User" => crate::models::GithubAccountType::User,
            "Organization" => crate::models::GithubAccountType::Organization,
            "Bot" => crate::models::GithubAccountType::Bot,
            other => {
                return Err(Error::Config(format!("unsupported PR author type: {other}")));
            }
        };
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
            title: pr.title.clone(),
            author: crate::github::PullRequestAuthor {
                id: author.id.0 as i64,
                login: author.login.clone(),
                account_type,
            },
        })
    }

    async fn resolve_commit(
        &self,
        installation_id: i64,
        repository: &str,
        git_ref: &str,
    ) -> Result<ResolvedCommit> {
        let (owner, repo) = split_repo(repository)?;
        let client = self
            .installation_client(installation_id)
            .await?;
        // GET /repos/{owner}/{repo}/commits/{ref} — GitHub resolves the
        // ref (dereferencing annotated tags) to the underlying commit.
        // octocrab formats the ref into the route verbatim, so we
        // percent-encode URL-unsafe characters first (a valid git ref
        // can contain `#`/`%`/…), preserving `/` as a path separator.
        let commit = client
            .commits(owner, repo)
            .get(encode_ref_path(git_ref))
            .await?;
        // Prefer the committer date (when the commit landed); fall back
        // to the author date. Either may be absent on unusual commits —
        // `committed_at` is Optional, so a missing date is fine.
        let committed_at = commit
            .commit
            .committer
            .as_ref()
            .and_then(|c| c.date)
            .or_else(|| {
                commit
                    .commit
                    .author
                    .as_ref()
                    .and_then(|a| a.date)
            });
        Ok(ResolvedCommit { hash: commit.sha, committed_at })
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

#[cfg(test)]
mod tests {
    use super::encode_ref_path;

    #[test]
    fn encode_ref_preserves_slashes() {
        // `/` is a path separator (GitHub greedy-captures the ref), so a
        // slashy release tag passes through unchanged.
        assert_eq!(encode_ref_path("tags/release/1.2"), "tags/release/1.2");
    }

    #[test]
    fn encode_ref_escapes_url_structural_chars() {
        // `#` (fragment) and `%` (escape) are valid in git refs but MUST
        // be encoded so the request targets the right ref. Slashes stay.
        assert_eq!(encode_ref_path("tags/v1#foo"), "tags/v1%23foo");
        // A literal `%2F` in the ref name → the `%` is encoded to `%25`
        // so GitHub decodes it back to a literal `%` (not a slash).
        assert_eq!(encode_ref_path("tags/v1%2Ffoo"), "tags/v1%252Ffoo");
        assert_eq!(encode_ref_path("tags/a b"), "tags/a%20b");
    }

    #[test]
    fn encode_ref_leaves_ordinary_chars_alone() {
        // Unreserved + path-safe sub-delims are not encoded (keeps refs
        // readable in logs / requests).
        assert_eq!(encode_ref_path("tags/v1.2.3-rc_4"), "tags/v1.2.3-rc_4");
    }
}
