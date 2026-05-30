//! In-memory `PullRequestStore` for unit tests. Single Mutex
//! serialises access; mirrors the Postgres semantics — title is the
//! only mutable field refreshed by `upsert_pull_request`, `closed_at`
//! is owned by `set_closed_at`, and lookup keys on the
//! `(target_repo, pr_number)` unique index.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::Result;
use crate::db::pull_request::{NewPullRequest, PullRequestStore};
use crate::models::GithubPullRequest;

#[derive(Default)]
pub struct InMemoryPullRequestStore {
    state: Mutex<State>,
    next_id: AtomicI64,
}

#[derive(Default)]
struct State {
    /// Keyed by (target_github_repo_id, pr_number).
    by_key: HashMap<(i64, i32), GithubPullRequest>,
}

impl InMemoryPullRequestStore {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
            next_id: AtomicI64::new(1),
        }
    }

    /// Test helper: pre-seed a PR row, bypassing the upsert path.
    pub fn seed(&self, pr: GithubPullRequest) {
        self.state
            .lock()
            .unwrap()
            .by_key
            .insert((pr.target_github_repo_id, pr.pr_number), pr);
    }

    /// Test helper: peek at a PR row by key.
    pub fn get(&self, target_github_repo_id: i64, pr_number: i32) -> Option<GithubPullRequest> {
        self.state
            .lock()
            .unwrap()
            .by_key
            .get(&(target_github_repo_id, pr_number))
            .cloned()
    }
}

#[async_trait]
impl PullRequestStore for InMemoryPullRequestStore {
    async fn upsert_pull_request(&self, new: &NewPullRequest) -> Result<GithubPullRequest> {
        let mut s = self.state.lock().unwrap();
        let now = Utc::now();
        let key = (new.target_github_repo_id, new.pr_number);
        if let Some(existing) = s.by_key.get_mut(&key) {
            // Title is the only mutable field; closed_at is owned by
            // set_closed_at; immutable identity fields stay put.
            existing.title = new.title.clone();
            existing.updated_at = now;
            return Ok(existing.clone());
        }
        let row = GithubPullRequest {
            id: self
                .next_id
                .fetch_add(1, Ordering::SeqCst),
            target_github_repo_id: new.target_github_repo_id,
            source_github_repo_id: new.source_github_repo_id,
            pr_number: new.pr_number,
            title: new.title.clone(),
            author_github_user_id: new.author_github_user_id,
            closed_at: None,
            created_at: now,
            updated_at: now,
        };
        s.by_key
            .insert(key, row.clone());
        Ok(row)
    }

    async fn lookup_pull_request(
        &self,
        target_github_repo_id: i64,
        pr_number: i32,
    ) -> Result<Option<GithubPullRequest>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .by_key
            .get(&(target_github_repo_id, pr_number))
            .cloned())
    }

    async fn lookup_by_id(&self, id: i64) -> Result<Option<GithubPullRequest>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .by_key
            .values()
            .find(|pr| pr.id == id)
            .cloned())
    }

    async fn set_closed_at(
        &self,
        target_github_repo_id: i64,
        pr_number: i32,
        closed_at: Option<DateTime<Utc>>,
    ) -> Result<Option<GithubPullRequest>> {
        let mut s = self.state.lock().unwrap();
        let row = s
            .by_key
            .get_mut(&(target_github_repo_id, pr_number));
        Ok(row.map(|r| {
            r.closed_at = closed_at;
            r.clone()
        }))
    }
}
