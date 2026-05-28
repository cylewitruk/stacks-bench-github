//! Postgres-backed `UserStore`. All single-statement operations
//! (the slice-6 surface has no multi-table transactions). The `has_role`
//! query is the one to keep an eye on — it's called inline on every
//! `/benchmark` comment, so the
//! `github_user_role_user_install_role_idx` partial index from the
//! slice 6 migration covers the (user, install, role) prefix.

use async_trait::async_trait;

use crate::Result;
use crate::db::Pool;
use crate::db::user::{GrantRoleOutcome, NewUser, UserStore};
use crate::models::{GithubUser, GithubUserRole, UserRole};

#[derive(Clone)]
pub struct PostgresUserStore {
    pool: Pool,
}

impl PostgresUserStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl UserStore for PostgresUserStore {
    async fn upsert_user(&self, new: &NewUser) -> Result<GithubUser> {
        let row = sqlx::query_as::<_, GithubUser>(
            r#"
            INSERT INTO github_user (id, login, user_type)
            VALUES ($1, $2, $3)
            ON CONFLICT (id) DO UPDATE
                SET login      = EXCLUDED.login,
                    user_type  = EXCLUDED.user_type,
                    updated_at = NOW()
            RETURNING id, login, user_type, created_at, updated_at
            "#,
        )
        .bind(new.id)
        .bind(&new.login)
        .bind(new.user_type)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    async fn lookup_user(&self, user_id: i64) -> Result<Option<GithubUser>> {
        let row = sqlx::query_as::<_, GithubUser>(
            r#"
            SELECT id, login, user_type, created_at, updated_at
              FROM github_user
             WHERE id = $1
            "#,
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn lookup_user_by_login(&self, login: &str) -> Result<Option<GithubUser>> {
        let row = sqlx::query_as::<_, GithubUser>(
            r#"
            SELECT id, login, user_type, created_at, updated_at
              FROM github_user
             WHERE lower(login) = lower($1)
            "#,
        )
        .bind(login)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn grant_role(
        &self,
        github_user_id: i64,
        github_installation_id: i64,
        github_repo_id: Option<i64>,
        granted_role: UserRole,
        granted_by_github_user_id: Option<i64>,
    ) -> Result<GrantRoleOutcome> {
        // Soft-revoke aware: on UNIQUE conflict, if the existing row
        // was previously revoked, clear `revoked_at` (re-grant — same
        // row, audit-preserving). If the row is already active, the
        // grant is a no-op (`created=false` with the existing row).
        //
        // We can't make this a single statement with `ON CONFLICT DO
        // UPDATE SET revoked_at = NULL WHERE ... revoked_at IS NOT
        // NULL` because Postgres requires the DO UPDATE target row
        // unconditionally, then the WHERE filter is applied as a
        // post-condition. To distinguish "newly created" from
        // "re-grant of revoked" from "already active", use two
        // explicit roundtrips: try INSERT; on conflict, UPDATE the
        // revoked row (returning) or fall through to SELECT.
        let inserted = sqlx::query_as::<_, GithubUserRole>(
            r#"
            INSERT INTO github_user_role
                (github_user_id, github_installation_id, github_repo_id,
                 granted_role, granted_by_github_user_id)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT DO NOTHING
            RETURNING id, github_user_id, github_installation_id, github_repo_id,
                      granted_role, granted_at, granted_by_github_user_id, revoked_at
            "#,
        )
        .bind(github_user_id)
        .bind(github_installation_id)
        .bind(github_repo_id)
        .bind(granted_role)
        .bind(granted_by_github_user_id)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = inserted {
            return Ok(GrantRoleOutcome { role: row, created: true });
        }

        // Conflict: re-grant path. Unconditional UPDATE clears
        // revoked_at — idempotent for an already-active row (the
        // field is already NULL). `IS NOT DISTINCT FROM` handles
        // the NULL-repo case where `= $3` wouldn't match. `created`
        // stays false on this path; tests can distinguish "was
        // revoked, now reactivated" from "already active" via the
        // returned row's `revoked_at` (always NULL after this
        // succeeds).
        let row = sqlx::query_as::<_, GithubUserRole>(
            r#"
            UPDATE github_user_role
               SET revoked_at = NULL
             WHERE github_user_id = $1
               AND github_installation_id = $2
               AND github_repo_id IS NOT DISTINCT FROM $3
               AND granted_role = $4
         RETURNING id, github_user_id, github_installation_id, github_repo_id,
                   granted_role, granted_at, granted_by_github_user_id, revoked_at
            "#,
        )
        .bind(github_user_id)
        .bind(github_installation_id)
        .bind(github_repo_id)
        .bind(granted_role)
        .fetch_one(&self.pool)
        .await?;
        Ok(GrantRoleOutcome { role: row, created: false })
    }

    async fn revoke_role(
        &self,
        github_user_id: i64,
        github_installation_id: i64,
        github_repo_id: Option<i64>,
        granted_role: UserRole,
    ) -> Result<Option<GithubUserRole>> {
        // Soft-revoke: set revoked_at = NOW() if currently NULL.
        // Returns Ok(Some(...)) on a real transition,
        // Ok(None) if no matching row OR the row was already revoked
        // (idempotent re-run). The predicate `revoked_at IS NULL`
        // collapses both "no row" and "already revoked" into None.
        let row = sqlx::query_as::<_, GithubUserRole>(
            r#"
            UPDATE github_user_role
               SET revoked_at = NOW()
             WHERE github_user_id = $1
               AND github_installation_id = $2
               AND github_repo_id IS NOT DISTINCT FROM $3
               AND granted_role = $4
               AND revoked_at IS NULL
         RETURNING id, github_user_id, github_installation_id, github_repo_id,
                   granted_role, granted_at, granted_by_github_user_id, revoked_at
            "#,
        )
        .bind(github_user_id)
        .bind(github_installation_id)
        .bind(github_repo_id)
        .bind(granted_role)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn list_roles(&self, install_id: Option<i64>) -> Result<Vec<GithubUserRole>> {
        // Returns both active AND revoked rows so operators can see
        // the audit trail via `sbgh-cli user list`. The CLI renders
        // revoked rows distinctly. Filter at the SQL level if the
        // dataset ever grows large enough that we need it.
        let rows = sqlx::query_as::<_, GithubUserRole>(
            r#"
            SELECT id, github_user_id, github_installation_id, github_repo_id,
                   granted_role, granted_at, granted_by_github_user_id, revoked_at
              FROM github_user_role
             WHERE ($1::BIGINT IS NULL OR github_installation_id = $1)
          ORDER BY github_installation_id, github_user_id, granted_role
            "#,
        )
        .bind(install_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn revoke_repo_scoped_grants(
        &self,
        github_installation_id: i64,
        github_repo_id: i64,
    ) -> Result<u64> {
        let result = sqlx::query(
            r#"
            UPDATE github_user_role
               SET revoked_at = NOW()
             WHERE github_installation_id = $1
               AND github_repo_id = $2
               AND revoked_at IS NULL
            "#,
        )
        .bind(github_installation_id)
        .bind(github_repo_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    async fn has_role(
        &self,
        github_user_id: i64,
        github_installation_id: i64,
        repo_id: i64,
        role: UserRole,
    ) -> Result<bool> {
        // Active grants only (`revoked_at IS NULL`). Admin-implies:
        // an `admin` grant in the same scope authorizes any role
        // request. Encoded here rather than at grant-time so admin
        // grants are stored as their own auditable rows.
        let exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                  FROM github_user_role
                 WHERE github_user_id = $1
                   AND github_installation_id = $2
                   AND (granted_role = $3 OR granted_role = 'admin')
                   AND (github_repo_id IS NULL OR github_repo_id = $4)
                   AND revoked_at IS NULL
            )
            "#,
        )
        .bind(github_user_id)
        .bind(github_installation_id)
        .bind(role)
        .bind(repo_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(exists)
    }
}
