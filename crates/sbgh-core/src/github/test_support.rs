//! Fake `GitHubApi` implementation for tests.
//!
//! Gated behind the `testing` feature so it doesn't ship in release
//! builds. Records every call into a `Vec` so tests can assert what the handler
//! / daemon tried to do, and lets tests pre-program responses (e.g. fixed
//! PR head SHAs).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::Result;
use crate::github::client::{
    CheckRunOutput, CheckRunState, CheckRunUpdate, GitHubApi, PostedCheckRun, PostedComment,
    PullRequestSide, PullRequestSummary, RepoRef, RepoSummary,
};
use crate::models::ResolvedCommit;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FakeCall {
    CreateComment {
        installation_id: i64,
        repository: String,
        pr_number: u64,
        body: String,
        returned_id: i64,
    },
    UpdateComment {
        installation_id: i64,
        repository: String,
        comment_id: i64,
        body: String,
    },
    HeadSha {
        installation_id: i64,
        repository: String,
        pr_number: u64,
    },
    GetRepository {
        installation_id: i64,
        owner: String,
        name: String,
    },
    GetPullRequest {
        installation_id: i64,
        repository: String,
        pr_number: u64,
    },
    ResolveCommit {
        installation_id: i64,
        repository: String,
        git_ref: String,
    },
    CompareCommits {
        installation_id: i64,
        base_repository: String,
        base_ref: String,
        head_owner: String,
        head_ref: String,
    },
    CreateCheckRun {
        installation_id: i64,
        repository: String,
        head_sha: String,
        name: String,
        external_id: String,
        state: CheckRunState,
        output: CheckRunOutput,
        returned_id: i64,
    },
    UpdateCheckRun {
        installation_id: i64,
        repository: String,
        check_run_id: i64,
        state: CheckRunState,
        output: CheckRunOutput,
    },
    FindCheckRun {
        installation_id: i64,
        repository: String,
        head_sha: String,
        name: String,
        app_id: i64,
        external_id: String,
    },
}

#[derive(Debug, Default, Clone)]
pub struct FakeGitHub {
    inner: Arc<Mutex<FakeState>>,
}

#[derive(Debug, Default)]
struct FakeState {
    calls: Vec<FakeCall>,
    next_comment_id: i64,
    head_shas: HashMap<(String, u64), String>,
    /// Pre-programmed responses for `get_repository`. Keyed by
    /// `(owner, name)` so tests can stage the full lineage for a
    /// fork-of-fork chain.
    repos: HashMap<(String, String), RepoSummary>,
    /// Pre-programmed responses for `get_pull_request`. Keyed by
    /// `("owner/name", pr_number)`.
    prs: HashMap<(String, u64), PullRequestSummary>,
    /// Pre-programmed responses for `resolve_commit`. Keyed by
    /// `("owner/name", git_ref)`.
    commits: HashMap<(String, String), ResolvedCommit>,
    /// Pre-programmed `compare_commits` merge-bases, keyed by
    /// `(base_repository, base_ref, head_owner, head_ref)`. A miss returns
    /// `Ok(None)` — modeling GitHub's "no common ancestor" / 404 degrade.
    merge_bases: HashMap<(String, String, String, String), ResolvedCommit>,
    next_check_run_id: i64,
    /// Pre-programmed `find_check_run_by_external_id` hits, keyed by
    /// `(repository, head_sha, name, external_id)` — scoped tightly so a
    /// runner test can't reconcile against the wrong repo/check name.
    existing_check_runs: HashMap<(String, String, String, String), PostedCheckRun>,
    /// Force `create_check_run` / `create_pr_comment` to error — for testing
    /// the non-fatal reporting policy. The call is still recorded.
    fail_create_check_run: bool,
    fail_create_comment: bool,
    /// Force `current_app_id` (`GET /app`) to error — for testing the
    /// reconcile-skip + self-heal path.
    fail_current_app_id: bool,
}

impl FakeGitHub {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(FakeState {
                next_comment_id: 1000,
                next_check_run_id: 5000,
                ..Default::default()
            })),
        }
    }

    /// Make `create_check_run` error (the call is still recorded) — exercises
    /// the non-fatal reporting policy.
    pub fn fail_create_check_run(&self) {
        self.inner
            .lock()
            .unwrap()
            .fail_create_check_run = true;
    }

    /// Make `create_pr_comment` error (the call is still recorded).
    pub fn fail_create_comment(&self) {
        self.inner
            .lock()
            .unwrap()
            .fail_create_comment = true;
    }

    /// Make `current_app_id` (`GET /app`) error.
    pub fn fail_current_app_id(&self) {
        self.inner
            .lock()
            .unwrap()
            .fail_current_app_id = true;
    }

    /// Pre-program a check run that `find_check_run_by_external_id` should
    /// return for `(repository, head_sha, name, external_id)` (the
    /// crash-then-retry reconcile).
    pub fn set_existing_check_run(
        &self,
        repository: &str,
        head_sha: &str,
        name: &str,
        external_id: &str,
        id: i64,
    ) {
        self.inner
            .lock()
            .unwrap()
            .existing_check_runs
            .insert(
                (repository.into(), head_sha.into(), name.into(), external_id.into()),
                PostedCheckRun {
                    id,
                    html_url: Some(format!("https://github.test/checks/{id}")),
                },
            );
    }

    /// Pre-program the head SHA returned for a given repo+PR.
    pub fn set_head_sha(&self, repository: &str, pr_number: u64, sha: &str) {
        let mut s = self.inner.lock().unwrap();
        s.head_shas
            .insert((repository.to_string(), pr_number), sha.to_string());
    }

    /// Pre-program a canonical (non-fork) repo response.
    pub fn set_repo_canonical(&self, owner: &str, name: &str, id: i64) {
        let summary = RepoSummary {
            id,
            owner: owner.into(),
            name: name.into(),
            default_branch: Some("main".into()),
            is_fork: false,
            parent: None,
            source: None,
        };
        self.inner
            .lock()
            .unwrap()
            .repos
            .insert((owner.into(), name.into()), summary);
    }

    /// Pre-program a fork repo response. `source` is the ultimate
    /// non-fork ancestor; for a one-hop fork pass `parent = source`.
    pub fn set_repo_fork(
        &self,
        owner: &str,
        name: &str,
        id: i64,
        parent: RepoRef,
        source: RepoRef,
    ) {
        let summary = RepoSummary {
            id,
            owner: owner.into(),
            name: name.into(),
            default_branch: Some("main".into()),
            is_fork: true,
            parent: Some(parent),
            source: Some(source),
        };
        self.inner
            .lock()
            .unwrap()
            .repos
            .insert((owner.into(), name.into()), summary);
    }

    /// Pre-program a PR response keyed on `("owner/name", pr_number)`.
    /// `base` is the target side; `head` is the source side. Slice 7
    /// kept this helper minimal — title defaults to a placeholder and
    /// author to `("alice", id=42, User)` so existing slice 5/6 tests
    /// don't need to plumb new args. Tests that exercise the slice 7
    /// PR-row materialisation should use `set_pull_request_full`.
    pub fn set_pull_request(
        &self,
        repository: &str,
        pr_number: u64,
        base: PullRequestSide,
        head: PullRequestSide,
    ) {
        self.set_pull_request_full(
            repository,
            pr_number,
            base,
            head,
            "test pr title",
            crate::github::PullRequestAuthor {
                id: 42,
                login: "alice".into(),
                account_type: crate::models::GithubAccountType::User,
            },
        );
    }

    /// Slice 7: pre-program a PR response with explicit title +
    /// author so PR materialisation tests can assert against the
    /// upserted `github_pull_request` row.
    pub fn set_pull_request_full(
        &self,
        repository: &str,
        pr_number: u64,
        base: PullRequestSide,
        head: PullRequestSide,
        title: &str,
        author: crate::github::PullRequestAuthor,
    ) {
        self.inner
            .lock()
            .unwrap()
            .prs
            .insert(
                (repository.into(), pr_number),
                PullRequestSummary {
                    number: pr_number,
                    base,
                    head,
                    title: title.into(),
                    author,
                },
            );
    }

    /// Pre-program the commit a ref (tag/branch) resolves to.
    pub fn set_commit(
        &self,
        repository: &str,
        git_ref: &str,
        sha: &str,
        committed_at: Option<chrono::DateTime<chrono::Utc>>,
    ) {
        self.inner
            .lock()
            .unwrap()
            .commits
            .insert(
                (repository.into(), git_ref.into()),
                ResolvedCommit { hash: sha.into(), committed_at },
            );
    }

    /// Pre-program the merge-base `compare_commits` returns for
    /// `(base_repository, base_ref, head_owner, head_ref)`. Leave a combo
    /// unstaged to model "no common ancestor" (the fake returns `Ok(None)`).
    pub fn set_merge_base(
        &self,
        base_repository: &str,
        base_ref: &str,
        head_owner: &str,
        head_ref: &str,
        sha: &str,
        committed_at: Option<chrono::DateTime<chrono::Utc>>,
    ) {
        self.inner
            .lock()
            .unwrap()
            .merge_bases
            .insert(
                (base_repository.into(), base_ref.into(), head_owner.into(), head_ref.into()),
                ResolvedCommit { hash: sha.into(), committed_at },
            );
    }

    pub fn calls(&self) -> Vec<FakeCall> {
        self.inner
            .lock()
            .unwrap()
            .calls
            .clone()
    }
}

#[async_trait]
impl GitHubApi for FakeGitHub {
    async fn create_pr_comment(
        &self,
        installation_id: i64,
        repository: &str,
        pr_number: u64,
        body: &str,
    ) -> Result<PostedComment> {
        let mut s = self.inner.lock().unwrap();
        let id = s.next_comment_id;
        s.next_comment_id += 1;
        s.calls
            .push(FakeCall::CreateComment {
                installation_id,
                repository: repository.into(),
                pr_number,
                body: body.into(),
                returned_id: id,
            });
        if s.fail_create_comment {
            return Err(crate::Error::Config("FakeGitHub: forced create_comment failure".into()));
        }
        Ok(PostedComment { id })
    }

    async fn update_pr_comment(
        &self,
        installation_id: i64,
        repository: &str,
        comment_id: i64,
        body: &str,
    ) -> Result<PostedComment> {
        let mut s = self.inner.lock().unwrap();
        s.calls
            .push(FakeCall::UpdateComment {
                installation_id,
                repository: repository.into(),
                comment_id,
                body: body.into(),
            });
        Ok(PostedComment { id: comment_id })
    }

    async fn pr_head_sha(
        &self,
        installation_id: i64,
        repository: &str,
        pr_number: u64,
    ) -> Result<String> {
        let mut s = self.inner.lock().unwrap();
        s.calls
            .push(FakeCall::HeadSha {
                installation_id,
                repository: repository.into(),
                pr_number,
            });
        Ok(s.head_shas
            .get(&(repository.to_string(), pr_number))
            .cloned()
            .unwrap_or_else(|| "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".into()))
    }

    async fn get_repository(
        &self,
        installation_id: i64,
        owner: &str,
        name: &str,
    ) -> Result<RepoSummary> {
        let mut s = self.inner.lock().unwrap();
        s.calls
            .push(FakeCall::GetRepository {
                installation_id,
                owner: owner.into(),
                name: name.into(),
            });
        s.repos
            .get(&(owner.into(), name.into()))
            .cloned()
            .ok_or_else(|| {
                crate::Error::Config(format!(
                    "FakeGitHub: no canned response for repo {owner}/{name} (use \
                     set_repo_canonical / set_repo_fork to stage it)"
                ))
            })
    }

    async fn get_pull_request(
        &self,
        installation_id: i64,
        repository: &str,
        pr_number: u64,
    ) -> Result<PullRequestSummary> {
        let mut s = self.inner.lock().unwrap();
        s.calls
            .push(FakeCall::GetPullRequest {
                installation_id,
                repository: repository.into(),
                pr_number,
            });
        s.prs
            .get(&(repository.into(), pr_number))
            .cloned()
            .ok_or_else(|| {
                crate::Error::Config(format!(
                    "FakeGitHub: no canned response for PR {repository}#{pr_number} (use \
                     set_pull_request to stage it)"
                ))
            })
    }

    async fn resolve_commit(
        &self,
        installation_id: i64,
        repository: &str,
        git_ref: &str,
    ) -> Result<ResolvedCommit> {
        let mut s = self.inner.lock().unwrap();
        s.calls
            .push(FakeCall::ResolveCommit {
                installation_id,
                repository: repository.into(),
                git_ref: git_ref.into(),
            });
        s.commits
            .get(&(repository.into(), git_ref.into()))
            .cloned()
            .ok_or_else(|| {
                crate::Error::Config(format!(
                    "FakeGitHub: no canned response for ref {repository}@{git_ref} (use \
                     set_commit to stage it)"
                ))
            })
    }

    async fn compare_commits(
        &self,
        installation_id: i64,
        base_repository: &str,
        base_ref: &str,
        head_owner: &str,
        head_ref: &str,
    ) -> Result<Option<ResolvedCommit>> {
        let mut s = self.inner.lock().unwrap();
        s.calls
            .push(FakeCall::CompareCommits {
                installation_id,
                base_repository: base_repository.into(),
                base_ref: base_ref.into(),
                head_owner: head_owner.into(),
                head_ref: head_ref.into(),
            });
        // A miss models "no common ancestor" / 404 → Ok(None) (not an error),
        // mirroring the real client's degrade.
        Ok(s.merge_bases
            .get(&(base_repository.into(), base_ref.into(), head_owner.into(), head_ref.into()))
            .cloned())
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
        let CheckRunUpdate { state, output } = update;
        let mut s = self.inner.lock().unwrap();
        let id = s.next_check_run_id;
        s.next_check_run_id += 1;
        s.calls
            .push(FakeCall::CreateCheckRun {
                installation_id,
                repository: repository.into(),
                head_sha: head_sha.into(),
                name: name.into(),
                external_id: external_id.into(),
                state,
                output,
                returned_id: id,
            });
        if s.fail_create_check_run {
            return Err(crate::Error::Config("FakeGitHub: forced create_check_run failure".into()));
        }
        Ok(PostedCheckRun {
            id,
            html_url: Some(format!("https://github.test/checks/{id}")),
        })
    }

    async fn update_check_run(
        &self,
        installation_id: i64,
        repository: &str,
        check_run_id: i64,
        update: CheckRunUpdate,
    ) -> Result<PostedCheckRun> {
        let CheckRunUpdate { state, output } = update;
        let mut s = self.inner.lock().unwrap();
        s.calls
            .push(FakeCall::UpdateCheckRun {
                installation_id,
                repository: repository.into(),
                check_run_id,
                state,
                output,
            });
        Ok(PostedCheckRun {
            id: check_run_id,
            html_url: Some(format!("https://github.test/checks/{check_run_id}")),
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
        let mut s = self.inner.lock().unwrap();
        s.calls
            .push(FakeCall::FindCheckRun {
                installation_id,
                repository: repository.into(),
                head_sha: head_sha.into(),
                name: name.into(),
                app_id,
                external_id: external_id.into(),
            });
        Ok(s.existing_check_runs
            .get(&(repository.into(), head_sha.into(), name.into(), external_id.into()))
            .cloned())
    }

    async fn current_app_id(&self) -> Result<i64> {
        if self
            .inner
            .lock()
            .unwrap()
            .fail_current_app_id
        {
            return Err(crate::Error::Config("FakeGitHub: forced GET /app failure".into()));
        }
        Ok(4242)
    }
}
