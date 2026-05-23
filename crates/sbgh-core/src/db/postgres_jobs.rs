//! Postgres-backed `JobStore`.

use async_trait::async_trait;
use uuid::Uuid;

use crate::Result;
use crate::db::Pool;
use crate::db::jobs::JobStore;
use crate::models::{Job, NewJob};

#[derive(Clone)]
pub struct PostgresJobStore {
    pool: Pool,
}

impl PostgresJobStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl JobStore for PostgresJobStore {
    async fn enqueue(&self, new: &NewJob) -> Result<Option<Uuid>> {
        // INSERT ... ON CONFLICT DO NOTHING + RETURNING — returns Some(id) for
        // a fresh row, None if `github_delivery_id` collided with an existing
        // job (i.e. a retried delivery).
        //
        // The conflict target's WHERE predicate must match the partial unique
        // index from the migration; without the predicate, Postgres returns
        // "no unique or exclusion constraint matching the ON CONFLICT
        // specification".
        //
        // Single statement (no SELECT, no transaction) keeps the handler's
        // required Postgres grants down to INSERT — see the role-split
        // migration.
        let id: Option<Uuid> = sqlx::query_scalar(
            r#"
            INSERT INTO jobs (
                repository, pr_number, head_sha, requested_by,
                command, args, installation_id, github_delivery_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (github_delivery_id) WHERE github_delivery_id IS NOT NULL
              DO NOTHING
            RETURNING id
            "#,
        )
        .bind(&new.repository)
        .bind(new.pr_number)
        .bind(&new.head_sha)
        .bind(&new.requested_by)
        .bind(&new.command)
        .bind(&new.args)
        .bind(new.installation_id)
        .bind(&new.github_delivery_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(id)
    }

    async fn claim_next(&self) -> Result<Option<Job>> {
        let mut tx = self.pool.begin().await?;

        let id: Option<Uuid> = sqlx::query_scalar(
            r#"
            SELECT id FROM jobs
            WHERE status = 'queued'
            ORDER BY queued_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            "#,
        )
        .fetch_optional(&mut *tx)
        .await?;

        let Some(id) = id else {
            tx.commit().await?;
            return Ok(None);
        };

        let job: Job = sqlx::query_as::<_, Job>(
            r#"
            UPDATE jobs
            SET status = 'running', started_at = NOW()
            WHERE id = $1
            RETURNING *
            "#,
        )
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(Some(job))
    }

    async fn complete(&self, id: Uuid, result: serde_json::Value) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE jobs
            SET status = 'completed', finished_at = NOW(), result = $2
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(&result)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn fail(&self, id: Uuid, error: &str, summary: Option<serde_json::Value>) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE jobs
            SET status = 'failed',
                finished_at = NOW(),
                error = $2,
                result = COALESCE($3, result)
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(error)
        .bind(summary)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn set_comment_id(&self, id: Uuid, comment_id: i64) -> Result<()> {
        sqlx::query("UPDATE jobs SET comment_id = $2 WHERE id = $1")
            .bind(id)
            .bind(comment_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn set_head_sha(&self, id: Uuid, head_sha: &str) -> Result<()> {
        sqlx::query("UPDATE jobs SET head_sha = $2 WHERE id = $1")
            .bind(id)
            .bind(head_sha)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
