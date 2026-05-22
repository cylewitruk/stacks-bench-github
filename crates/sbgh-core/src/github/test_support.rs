//! Fake `GitHubApi` implementation for tests.
//!
//! Gated behind the `test-support` feature so it doesn't ship in release
//! builds. Records every call into a `Vec` so tests can assert what the handler
//! / orchestrator tried to do, and lets tests pre-program responses (e.g. fixed
//! PR head SHAs).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::Result;
use crate::github::client::{GitHubApi, PostedComment};

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
}
