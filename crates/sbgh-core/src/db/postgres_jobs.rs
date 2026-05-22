//! Postgres-backed `JobStore`.

use async_trait::async_trait;
use uuid::Uuid;

use crate::Result;
use crate::db::Pool;
use crate::db::jobs::JobStore;
use crate::models::{Job, JobStatus, NewJob};

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
    async fn enqueue(&self, new: &NewJob) -> Result<Option<(Uuid, i64)>> {
        let mut tx = self.pool.begin().await?;

        // INSERT ... ON CONFLICT DO NOTHING + RETURNING — returns Some(id) for
        // a fresh row, None if `github_delivery_id` collided with an existing
        // job (i.e. a retried delivery).
        //
        // The conflict target's WHERE predicate must match the partial unique
        // index from the migration; without the predicate, Postgres returns
        // "no unique or exclusion constraint matching the ON CONFLICT
        // specification".
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
        .fetch_optional(&mut *tx)
        .await?;

        let Some(id) = id else {
            tx.commit().await?;
            return Ok(None);
        };

        let position: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) FROM jobs
            WHERE status = 'queued'
              AND queued_at <= (SELECT queued_at FROM jobs WHERE id = $1)
            "#,
        )
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(Some((id, position)))
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

    async fn queue_position(&self, id: Uuid) -> Result<Option<i64>> {
        let row: Option<(JobStatus, i64)> = sqlx::query_as(
            r#"
            WITH target AS (
                SELECT status, queued_at FROM jobs WHERE id = $1
            )
            SELECT
                target.status,
                (SELECT COUNT(*) FROM jobs
                 WHERE status = 'queued' AND queued_at <= target.queued_at) AS position
            FROM target
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.and_then(|(status, pos)| (status == JobStatus::Queued).then_some(pos)))
    }
}
