use sbgh_core::models::{GithubInstallation, Job};

use crate::mapping::{Db, IntoDomain};
use crate::{Pool, Result};

pub async fn list_installations(pool: &Pool) -> Result<Vec<GithubInstallation>> {
    let rows = sqlx::query_as::<_, Db<GithubInstallation>>(
        "SELECT id, github_account_id, account_login, account_type, suspended_at, deleted_at, \
         created_at, updated_at FROM github_installation ORDER BY created_at DESC",
    )
    .fetch_all(pool)
    .await
    .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
    Ok(rows.into_domain())
}

pub async fn list_jobs(pool: &Pool, status: Option<&str>, limit: i64) -> Result<Vec<Job>> {
    let rows = sqlx::query_as::<_, Db<Job>>(
        r#"
        SELECT * FROM job
         WHERE ($1::text IS NULL OR status = $1::job_status)
         ORDER BY created_at DESC
         LIMIT $2
        "#,
    )
    .bind(status)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
    Ok(rows.into_domain())
}
