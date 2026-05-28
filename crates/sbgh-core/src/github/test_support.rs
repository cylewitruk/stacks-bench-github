//! Fake `GitHubApi` implementation for tests.
//!
//! Gated behind the `testing` feature so it doesn't ship in release
//! builds. Records every call into a `Vec` so tests can assert what the handler
//! / orchestrator tried to do, and lets tests pre-program responses (e.g. fixed
//! PR head SHAs).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::Result;
use crate::github::client::{
    GitHubApi, PostedComment, PullRequestSide, PullRequestSummary, RepoRef, RepoSummary,
};

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
}

impl FakeGitHub {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(FakeState {
                next_comment_id: 1000,
                ..Default::default()
            })),
        }
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
    /// `base` is the target side; `head` is the source side.
    pub fn set_pull_request(
        &self,
        repository: &str,
        pr_number: u64,
        base: PullRequestSide,
        head: PullRequestSide,
    ) {
        self.inner
            .lock()
            .unwrap()
            .prs
            .insert(
                (repository.into(), pr_number),
                PullRequestSummary { number: pr_number, base, head },
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
}
