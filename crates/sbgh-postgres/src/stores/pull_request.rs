//! Postgres-backed `PullRequestStore`. All single-statement operations.

use crate::IntoCoreResult as _;

use crate::mapping::{Db, IntoDomain};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::Result;
use crate::db::Pool;
use crate::db::pull_request::{NewPullRequest, PullRequestStore};
use crate::models::GithubPullRequest;

#[derive(Clone)]
pub struct PostgresPullRequestStore {
    pool: Pool,
}

impl PostgresPullRequestStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PullRequestStore for PostgresPullRequestStore {
    async fn upsert_pull_request(&self, new: &NewPullRequest) -> Result<GithubPullRequest> {
        // `(target_repo, pr_number)` is the unique key. ON CONFLICT
        // refreshes the mutable title and bumps updated_at; immutable
        // fields (source_repo, author) are deliberately NOT updated
        // — slice 7's contract says these don't change.
        //
        // closed_at is also NOT touched here: an upsert from a
        // synchronize/opened/edited event must NOT silently reopen a
        // closed PR. `set_closed_at` is the dedicated lifecycle path.
        let row = sqlx::query_as::<_, Db<GithubPullRequest>>(
            r#"
            INSERT INTO github_pull_request
                (target_github_repo_id, source_github_repo_id, pr_number,
                 title, author_github_user_id)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (target_github_repo_id, pr_number) DO UPDATE
                SET title      = EXCLUDED.title,
                    updated_at = NOW()
            RETURNING id, target_github_repo_id, source_github_repo_id, pr_number,
                      title, author_github_user_id, closed_at, created_at, updated_at
            "#,
        )
        .bind(new.target_github_repo_id)
        .bind(new.source_github_repo_id)
        .bind(new.pr_number)
        .bind(&new.title)
        .bind(new.author_github_user_id)
        .fetch_one(&self.pool)
        .await
        .core()?;
        Ok(row.into_domain())
    }

    async fn lookup_pull_request(
        &self,
        target_github_repo_id: i64,
        pr_number: i32,
    ) -> Result<Option<GithubPullRequest>> {
        let row = sqlx::query_as::<_, Db<GithubPullRequest>>(
            r#"
            SELECT id, target_github_repo_id, source_github_repo_id, pr_number,
                   title, author_github_user_id, closed_at, created_at, updated_at
              FROM github_pull_request
             WHERE target_github_repo_id = $1
               AND pr_number = $2
            "#,
        )
        .bind(target_github_repo_id)
        .bind(pr_number)
        .fetch_optional(&self.pool)
        .await
        .core()?;
        Ok(row.into_domain())
    }

    async fn lookup_by_id(&self, id: i64) -> Result<Option<GithubPullRequest>> {
        let row = sqlx::query_as::<_, Db<GithubPullRequest>>(
            r#"
            SELECT id, target_github_repo_id, source_github_repo_id, pr_number,
                   title, author_github_user_id, closed_at, created_at, updated_at
              FROM github_pull_request
             WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .core()?;
        Ok(row.into_domain())
    }

    async fn set_closed_at(
        &self,
        target_github_repo_id: i64,
        pr_number: i32,
        closed_at: Option<DateTime<Utc>>,
    ) -> Result<Option<GithubPullRequest>> {
        let row = sqlx::query_as::<_, Db<GithubPullRequest>>(
            r#"
            UPDATE github_pull_request
               SET closed_at = $3
             WHERE target_github_repo_id = $1
               AND pr_number = $2
         RETURNING id, target_github_repo_id, source_github_repo_id, pr_number,
                   title, author_github_user_id, closed_at, created_at, updated_at
            "#,
        )
        .bind(target_github_repo_id)
        .bind(pr_number)
        .bind(closed_at)
        .fetch_optional(&self.pool)
        .await
        .core()?;
        Ok(row.into_domain())
    }
}
