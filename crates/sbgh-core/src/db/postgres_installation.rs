//! Postgres-backed `InstallationStore`. Each method is one SQL statement;
//! the `installation.created` flow that combines a lookup + upsert is
//! sequenced by the processor (lookup_allowed → upsert_installation) so
//! a hostile create-event on a denied account doesn't accidentally
//! materialise an install row.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::Result;
use crate::db::Pool;
use crate::db::installation::{InstallationStore, NewInstallation};
use crate::models::{AllowedInstaller, GithubInstallation};

#[derive(Clone)]
pub struct PostgresInstallationStore {
    pool: Pool,
}

impl PostgresInstallationStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl InstallationStore for PostgresInstallationStore {
    async fn lookup_allowed(&self, github_account_id: i64) -> Result<Option<AllowedInstaller>> {
        let row = sqlx::query_as::<_, AllowedInstaller>(
            r#"
            SELECT github_account_id, account_login, account_type,
                   is_enabled, note, created_at, updated_at
              FROM allowed_installer
             WHERE github_account_id = $1
            "#,
        )
        .bind(github_account_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn upsert_installation(&self, new: &NewInstallation) -> Result<GithubInstallation> {
        let row = sqlx::query_as::<_, GithubInstallation>(
            r#"
            INSERT INTO github_installation
                (id, github_account_id, account_login, account_type)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO UPDATE
                SET account_login = EXCLUDED.account_login,
                    account_type  = EXCLUDED.account_type,
                    updated_at    = NOW()
            RETURNING id, github_account_id, account_login, account_type,
                      suspended_at, created_at, updated_at
            "#,
        )
        .bind(new.id)
        .bind(new.github_account_id)
        .bind(&new.account_login)
        .bind(new.account_type)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    async fn set_suspended(
        &self,
        installation_id: i64,
        suspended_at: Option<DateTime<Utc>>,
    ) -> Result<Option<GithubInstallation>> {
        let row = sqlx::query_as::<_, GithubInstallation>(
            r#"
            UPDATE github_installation
               SET suspended_at = $1
             WHERE id = $2
         RETURNING id, github_account_id, account_login, account_type,
                   suspended_at, created_at, updated_at
            "#,
        )
        .bind(suspended_at)
        .bind(installation_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn delete_installation(&self, installation_id: i64) -> Result<bool> {
        let result = sqlx::query("DELETE FROM github_installation WHERE id = $1")
            .bind(installation_id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }
}
