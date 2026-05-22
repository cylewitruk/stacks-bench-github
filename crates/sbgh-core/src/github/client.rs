//! GitHub API client abstraction.
//!
//! The trait `GitHubApi` is the boundary between our handler/orchestrator and
//! GitHub itself; everything in the rest of the codebase depends on the trait,
//! not on `octocrab`. `OctocrabClient` is the real implementation; a fake
//! implementation lives under the `test-support` feature for use in tests.

use async_trait::async_trait;
use octocrab::Octocrab;
use octocrab::models::CommentId;

use crate::github::auth::InstallationTokenCache;
use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostedComment {
    pub id: i64,
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
}

fn split_repo(full_name: &str) -> Result<(&str, &str)> {
    full_name
        .split_once('/')
        .ok_or_else(|| Error::Config(format!("invalid repository: {full_name}")))
}
