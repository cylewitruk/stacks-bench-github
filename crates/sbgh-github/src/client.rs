use crate::IntoCoreResult as _;
use async_trait::async_trait;
use octocrab::Octocrab;
use octocrab::models::CommentId;
use sbgh_core::github::client::compare_basehead;
use sbgh_core::github::{
    CheckRunConclusion, CheckRunOutput, CheckRunState, CheckRunUpdate, GitHubApi, PostedCheckRun,
    PostedComment, PullRequestAuthor, PullRequestSide, PullRequestSummary, RepoRef, RepoSummary,
    encode_ref_path,
};
use sbgh_core::models::{GithubAccountType, ResolvedCommit};
use sbgh_core::{Error, Result};

use crate::InstallationTokenCache;

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
            .await
            .core()?;
        let client = Octocrab::builder()
            .personal_token(token)
            .build()
            .core()?;
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
            .await
            .core()?;
        let comment = client
            .issues(owner, repo)
            .create_comment(pr_number, body)
            .await
            .core()?;
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
            .await
            .core()?;
        let comment = client
            .issues(owner, repo)
            .update_comment(CommentId(comment_id as u64), body)
            .await
            .core()?;
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
            .await
            .core()?;
        let pr = client
            .pulls(owner, repo)
            .get(pr_number)
            .await
            .core()?;
        let head = pr
            .head
            .as_ref()
            .ok_or_else(|| Error::Config(format!("PR #{pr_number} missing head")))?;
        Ok(head.sha.clone())
    }

    async fn get_repository(
        &self,
        installation_id: i64,
        owner: &str,
        name: &str,
    ) -> Result<RepoSummary> {
        let client = self
            .installation_client(installation_id)
            .await
            .core()?;
        let repo = client
            .repos(owner, name)
            .get()
            .await
            .core()?;
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
            .await
            .core()?;
        let pr = client
            .pulls(owner, repo)
            .get(pr_number)
            .await
            .core()?;
        // octocrab's PullRequest exposes head/base as distinct concrete
        // types (Head vs Base) even though they share fields. Extract
        // the bits we care about manually rather than fight the
        // generics.
        let head = pr
            .head
            .as_ref()
            .ok_or_else(|| Error::Config(format!("PR #{pr_number} missing head")))?;
        let base = pr
            .base
            .as_ref()
            .ok_or_else(|| Error::Config(format!("PR #{pr_number} missing base")))?;
        let head_repo = head
            .repo
            .as_ref()
            .ok_or_else(|| Error::Config("PR head missing repo (orphaned ref?)".into()))?;
        let base_repo = base
            .repo
            .as_ref()
            .ok_or_else(|| Error::Config("PR base missing repo".into()))?;
        let author = pr
            .user
            .as_ref()
            .ok_or_else(|| Error::Config(format!("PR #{pr_number} missing author")))?;
        // GH's REST response uses `User`/`Organization`/`Bot`; map to
        // our typed enum at the boundary (same rationale as elsewhere
        // — bogus type → Error rather than silent default).
        let account_type = match author.r#type.as_str() {
            "User" => GithubAccountType::User,
            "Organization" => GithubAccountType::Organization,
            "Bot" => GithubAccountType::Bot,
            other => {
                return Err(Error::Config(format!("unsupported PR author type: {other}")));
            }
        };
        Ok(PullRequestSummary {
            number: pr_number,
            head: PullRequestSide {
                repo: repo_ref_from_octocrab(head_repo),
                sha: head.sha.clone(),
                branch: head.ref_field.clone(),
            },
            base: PullRequestSide {
                repo: repo_ref_from_octocrab(base_repo),
                sha: base.sha.clone(),
                branch: base.ref_field.clone(),
            },
            title: pr
                .title
                .clone()
                .ok_or_else(|| Error::Config(format!("PR #{pr_number} missing title")))?,
            author: PullRequestAuthor {
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
            .await
            .core()?;
        // GET /repos/{owner}/{repo}/commits/{ref} — GitHub resolves the
        // ref (dereferencing annotated tags) to the underlying commit.
        // octocrab formats the ref into the route verbatim, so we
        // percent-encode URL-unsafe characters first (a valid git ref
        // can contain `#`/`%`/…), preserving `/` as a path separator.
        let commit = client
            .commits(owner, repo)
            .get(encode_ref_path(git_ref))
            .await
            .core()?;
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

    async fn compare_commits(
        &self,
        installation_id: i64,
        base_repository: &str,
        base_ref: &str,
        head_owner: &str,
        head_ref: &str,
    ) -> Result<Option<ResolvedCommit>> {
        let (owner, repo) = split_repo(base_repository)?;
        let client = self
            .installation_client(installation_id)
            .await
            .core()?;
        // GET /repos/{owner}/{repo}/compare/{base_ref}...{head_owner}:{head_ref}
        // The `{owner}:{ref}` head form is cross-fork-safe (and correct for
        // same-repo PRs). Percent-encode each ref segment (preserving `/`) and
        // keep the `...` and `:` separators literal.
        let basehead = compare_basehead(base_ref, head_owner, head_ref);
        let route = format!("/repos/{owner}/{repo}/compare/{basehead}");

        #[derive(serde::Deserialize)]
        struct CompareResp {
            merge_base_commit: Option<CommitNode>,
        }
        #[derive(serde::Deserialize)]
        struct CommitNode {
            sha: String,
            commit: CommitDetail,
        }
        #[derive(serde::Deserialize)]
        struct CommitDetail {
            committer: Option<GitActor>,
            author: Option<GitActor>,
        }
        #[derive(serde::Deserialize)]
        struct GitActor {
            date: Option<chrono::DateTime<chrono::Utc>>,
        }

        // A 404 (repo/ref not found, or no common ancestor) is a clean "no
        // baseline anchor", not an error — return None so the caller shows
        // absolute-only metrics.
        let resp: CompareResp = match client
            .get(route, None::<&()>)
            .await
        {
            Ok(r) => r,
            Err(e) if is_not_found(&e) => return Ok(None),
            Err(e) => return Err(Error::Other(anyhow::Error::new(e))),
        };
        // Prefer the committer date (when the commit landed); fall back to the
        // author date. Either may be absent — `committed_at` is Optional.
        Ok(resp
            .merge_base_commit
            .map(|c| {
                let committed_at = c
                    .commit
                    .committer
                    .and_then(|a| a.date)
                    .or_else(|| {
                        c.commit
                            .author
                            .and_then(|a| a.date)
                    });
                ResolvedCommit { hash: c.sha, committed_at }
            }))
    }

    async fn create_check_run(
        &self,
        installation_id: i64,
        repository: &str,
        head_sha: &str,
        name: &str,
        external_id: &str,
        update: CheckRunUpdate,
    ) -> Result<PostedCheckRun> {
        let (owner, repo) = split_repo(repository)?;
        let client = self
            .installation_client(installation_id)
            .await
            .core()?;
        let (status, conclusion) = status_strings(update.state);
        // Raw POST: octocrab 0.51's `CheckRun` response model types
        // `pull_requests` as the FULL `PullRequest` (needs `node_id`), but the
        // Checks API returns minimal PR refs — so `.send()` fails to
        // deserialize the moment a check is associated with a PR (i.e. PR
        // jobs). Send our own body + minimal response instead.
        let body = CheckRunWrite {
            name: Some(name),
            head_sha: Some(head_sha),
            external_id: Some(external_id),
            status,
            conclusion,
            output: output_body(update.output),
        };
        let route = format!("/repos/{owner}/{repo}/check-runs");
        let resp: CheckRunWriteResp = client
            .post(route, Some(&body))
            .await
            .core()?;
        Ok(PostedCheckRun {
            id: resp.id as i64,
            html_url: resp.html_url,
        })
    }

    async fn update_check_run(
        &self,
        installation_id: i64,
        repository: &str,
        check_run_id: i64,
        update: CheckRunUpdate,
    ) -> Result<PostedCheckRun> {
        let (owner, repo) = split_repo(repository)?;
        let client = self
            .installation_client(installation_id)
            .await
            .core()?;
        let (status, conclusion) = status_strings(update.state);
        let body = CheckRunWrite {
            name: None,
            head_sha: None,
            external_id: None,
            status,
            conclusion,
            output: output_body(update.output),
        };
        let route = format!("/repos/{owner}/{repo}/check-runs/{check_run_id}");
        let resp: CheckRunWriteResp = client
            .patch(route, Some(&body))
            .await
            .core()?;
        Ok(PostedCheckRun {
            id: resp.id as i64,
            html_url: resp.html_url,
        })
    }

    async fn find_check_run_by_external_id(
        &self,
        installation_id: i64,
        repository: &str,
        head_sha: &str,
        name: &str,
        app_id: i64,
        external_id: &str,
    ) -> Result<Option<PostedCheckRun>> {
        let (owner, repo) = split_repo(repository)?;
        let client = self
            .installation_client(installation_id)
            .await
            .core()?;
        // The typed octocrab `CheckRun` model omits `external_id`, so GET the
        // raw list. Narrow server-side to OUR App + check name (the two filters
        // the endpoint supports); `external_id` isn't a server filter, so match
        // it client-side to pick this job's run.
        #[derive(serde::Deserialize)]
        struct ListResp {
            check_runs: Vec<Item>,
        }
        #[derive(serde::Deserialize)]
        struct Item {
            id: u64,
            html_url: Option<String>,
            external_id: Option<String>,
        }
        let route = format!("/repos/{owner}/{repo}/commits/{head_sha}/check-runs");
        let resp: ListResp = client
            .get(route, Some(&[("check_name", name.to_string()), ("app_id", app_id.to_string())]))
            .await
            .core()?;
        Ok(resp
            .check_runs
            .into_iter()
            .find(|r| r.external_id.as_deref() == Some(external_id))
            .map(|r| PostedCheckRun {
                id: r.id as i64,
                html_url: r.html_url,
            }))
    }

    async fn current_app_id(&self) -> Result<i64> {
        self.tokens
            .app_id()
            .await
            .core()
    }
}

/// True for a GitHub `404` — the compare/lookup target (repo, ref, or common
/// ancestor) doesn't exist, which roadmap-v7 treats as "no baseline anchor"
/// rather than a hard error.
fn is_not_found(e: &octocrab::Error) -> bool {
    matches!(
        e,
        octocrab::Error::GitHub { source, .. } if source.status_code.as_u16() == 404
    )
}

/// Map our lifecycle state to GitHub's `status` + optional `conclusion`
/// strings. We only ever complete with a non-blocking conclusion.
fn status_strings(state: CheckRunState) -> (&'static str, Option<&'static str>) {
    use CheckRunConclusion::*;
    match state {
        CheckRunState::InProgress => ("in_progress", None),
        CheckRunState::Completed(Success) => ("completed", Some("success")),
        CheckRunState::Completed(Failure) => ("completed", Some("failure")),
        CheckRunState::Completed(Neutral) => ("completed", Some("neutral")),
        CheckRunState::Completed(Skipped) => ("completed", Some("skipped")),
        CheckRunState::Completed(Cancelled) => ("completed", Some("cancelled")),
    }
}

fn output_body(o: CheckRunOutput) -> CheckRunOutputBody {
    CheckRunOutputBody {
        title: o.title,
        summary: o.summary,
        text: o.text,
    }
}

/// Request body for create/update check-run. `name`/`head_sha`/`external_id`
/// are sent only on create.
#[derive(serde::Serialize)]
struct CheckRunWrite<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    head_sha: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    external_id: Option<&'a str>,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    conclusion: Option<&'static str>,
    output: CheckRunOutputBody,
}

#[derive(serde::Serialize)]
struct CheckRunOutputBody {
    title: String,
    summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
}

/// Minimal slice of the check-run response — just the fields we need, so we
/// never touch the octocrab model's broken `pull_requests` deserialization.
#[derive(serde::Deserialize)]
struct CheckRunWriteResp {
    id: u64,
    html_url: Option<String>,
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
    use super::{CheckRunWriteResp, compare_basehead, encode_ref_path};

    /// roadmap-v7: the compare basehead keeps `...`/`:` literal, preserves
    /// slashy refs, and percent-encodes unsafe chars — the cross-fork form the
    /// fake can't exercise.
    #[test]
    fn compare_basehead_builds_cross_fork_and_slashy_refs() {
        assert_eq!(
            compare_basehead("develop", "cylewitruk", "feat/foo"),
            "develop...cylewitruk:feat/foo"
        );
        assert_eq!(compare_basehead("release/1.0", "o", "feat/a b"), "release/1.0...o:feat/a%20b");
    }

    /// Regression for the octocrab `CheckRun` deser bug: GitHub's check-run
    /// response embeds MINIMAL pull-request refs (no `node_id`) under
    /// `pull_requests` — the exact shape that makes octocrab's typed model fail
    /// to decode *after* the check is already created. Our boundary DTO must
    /// ignore that field and decode just `id` + `html_url`.
    #[test]
    fn check_run_write_resp_ignores_minimal_pull_requests() {
        let body = r#"{
            "id": 12345,
            "head_sha": "abc",
            "html_url": "https://github.com/o/r/runs/12345",
            "status": "in_progress",
            "external_id": "job-1",
            "output": { "title": null, "summary": null, "text": null },
            "pull_requests": [
                { "id": 1, "number": 2,
                  "url": "https://api.github.com/repos/o/r/pulls/2",
                  "head": { "ref": "f", "sha": "abc" },
                  "base": { "ref": "develop2", "sha": "def" } }
            ]
        }"#;
        let parsed: CheckRunWriteResp = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.id, 12345);
        assert_eq!(parsed.html_url.as_deref(), Some("https://github.com/o/r/runs/12345"));
    }

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
