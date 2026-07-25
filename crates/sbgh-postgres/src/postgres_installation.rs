//! Postgres-backed `InstallationStore`. Most methods are single-statement;
//! `delete_installation` is the exception — slice 4 turned it into a
//! transactional bulk-membership-revoke + soft-delete because the new
//! `github_installation_repo` FK would otherwise block any hard DELETE
//! once memberships exist.

use crate::IntoCoreResult as _;

use crate::mapping::{Db, IntoDomain};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::Result;
use crate::db::Pool;
use crate::db::installation::{DeleteInstallationOutcome, InstallationStore, NewInstallation};
use crate::models::{AllowedInstaller, GithubInstallation, GithubInstallationRepo};

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
        let row = sqlx::query_as::<_, Db<AllowedInstaller>>(
            r#"
            SELECT github_account_id, account_login, account_type,
                   is_enabled, note, created_at, updated_at
              FROM allowed_installer
             WHERE github_account_id = $1
            "#,
        )
        .bind(github_account_id)
        .fetch_optional(&self.pool)
        .await
        .core()?;
        Ok(row.into_domain())
    }

    async fn upsert_installation(&self, new: &NewInstallation) -> Result<GithubInstallation> {
        let row = sqlx::query_as::<_, Db<GithubInstallation>>(
            r#"
            INSERT INTO github_installation
                (id, github_account_id, account_login, account_type)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO UPDATE
                SET account_login = EXCLUDED.account_login,
                    account_type  = EXCLUDED.account_type,
                    updated_at    = NOW()
            RETURNING id, github_account_id, account_login, account_type,
                      suspended_at, deleted_at, created_at, updated_at
            "#,
        )
        .bind(new.id)
        .bind(new.github_account_id)
        .bind(&new.account_login)
        .bind(Db(new.account_type))
        .fetch_one(&self.pool)
        .await
        .core()?;
        Ok(row.into_domain())
    }

    async fn set_suspended(
        &self,
        installation_id: i64,
        suspended_at: Option<DateTime<Utc>>,
    ) -> Result<Option<GithubInstallation>> {
        let row = sqlx::query_as::<_, Db<GithubInstallation>>(
            r#"
            UPDATE github_installation
               SET suspended_at = $1
             WHERE id = $2
         RETURNING id, github_account_id, account_login, account_type,
                   suspended_at, deleted_at, created_at, updated_at
            "#,
        )
        .bind(suspended_at)
        .bind(installation_id)
        .fetch_optional(&self.pool)
        .await
        .core()?;
        Ok(row.into_domain())
    }

    async fn delete_installation(&self, installation_id: i64) -> Result<DeleteInstallationOutcome> {
        // Lock-then-mutate. Both this path and `add_or_restore_membership`
        // take `SELECT ... FOR UPDATE` on the install row so they
        // serialize against each other: a concurrent add can't slip
        // between our "is this install still active" check and our
        // soft-delete + revoke (which is the race Codex's slice-4 review
        // surfaced — the previous EXISTS-without-lock approach left a
        // window in which an add could materialise an active membership
        // on a row that was being soft-deleted in another transaction).
        //
        // No `deleted_at IS NULL` filter on the SELECT — we need the
        // install_found signal even for redelivery against an
        // already-soft-deleted row, AND we need the lock to be visible
        // to concurrent adds regardless of current state.
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;

        // `FOR UPDATE` must be at the top-level SELECT (Postgres rejects
        // it inside an EXISTS subquery), so probe with a plain SELECT
        // that returns the id and treat `None` as "not found".
        let install_found = sqlx::query_scalar::<_, i64>(
            "SELECT id FROM github_installation WHERE id = $1 FOR UPDATE",
        )
        .bind(installation_id)
        .fetch_optional(&mut *tx)
        .await
        .core()?
        .is_some();

        if !install_found {
            tx.commit().await.core()?;
            return Ok(DeleteInstallationOutcome {
                install_found: false,
                memberships_revoked: 0,
            });
        }

        // Mark deleted_at FIRST. The lock guarantees no concurrent add
        // can observe `deleted_at IS NULL` past this point, but doing
        // the install-level marker before the membership revoke also
        // means any reader without the lock sees "install retired,
        // memberships being cleaned up" rather than the other way around.
        sqlx::query(
            r#"
            UPDATE github_installation
               SET deleted_at = NOW()
             WHERE id = $1
               AND deleted_at IS NULL
            "#,
        )
        .bind(installation_id)
        .execute(&mut *tx)
        .await
        .core()?;

        // Bulk-revoke memberships. Predicate `revoked_at IS NULL` makes
        // this idempotent for redelivery — a second call revokes nothing.
        let revoke_result = sqlx::query(
            r#"
            UPDATE github_installation_repo
               SET revoked_at = NOW()
             WHERE github_installation_id = $1
               AND revoked_at IS NULL
            "#,
        )
        .bind(installation_id)
        .execute(&mut *tx)
        .await
        .core()?;

        // Slice 5: also disable every active policy for this install in
        // the same transaction. Ordering matters via the FK chain:
        // trigger_policy FKs to target_repo_policy (composite), so we
        // disable triggers FIRST in case slice 8+ adds CASCADE-on-disable
        // semantics. All three statements are idempotent (predicate `is_enabled
        // = TRUE` skips already-disabled rows on redelivery).
        sqlx::query(
            r#"
            UPDATE trigger_policy
               SET is_enabled = FALSE
             WHERE github_installation_id = $1
               AND is_enabled = TRUE
            "#,
        )
        .bind(installation_id)
        .execute(&mut *tx)
        .await
        .core()?;
        sqlx::query(
            r#"
            UPDATE target_repo_policy
               SET is_enabled = FALSE
             WHERE github_installation_id = $1
               AND is_enabled = TRUE
            "#,
        )
        .bind(installation_id)
        .execute(&mut *tx)
        .await
        .core()?;
        sqlx::query(
            r#"
            UPDATE source_repo_policy
               SET is_enabled = FALSE
             WHERE github_installation_id = $1
               AND is_enabled = TRUE
            "#,
        )
        .bind(installation_id)
        .execute(&mut *tx)
        .await
        .core()?;

        // Slice 6 post-review fix: also soft-revoke every active
        // `github_user_role` row for this install (both repo-scoped
        // and install-wide grants). Without this, a grant made while
        // the install was active would survive the install delete
        // AND re-create — the operator would have to explicitly
        // revoke every prior grant before reinstalling, or the new
        // install would inherit them silently.
        sqlx::query(
            r#"
            UPDATE github_user_role
               SET revoked_at = NOW()
             WHERE github_installation_id = $1
               AND revoked_at IS NULL
            "#,
        )
        .bind(installation_id)
        .execute(&mut *tx)
        .await
        .core()?;

        tx.commit().await.core()?;
        Ok(DeleteInstallationOutcome {
            install_found: true,
            memberships_revoked: revoke_result.rows_affected(),
        })
    }

    async fn add_or_restore_membership(
        &self,
        installation_id: i64,
        github_repo_id: i64,
    ) -> Result<Option<GithubInstallationRepo>> {
        // `SELECT ... FOR UPDATE` on the install row, gated by
        // `deleted_at IS NULL`. This is the actual race-closer for the
        // Codex-flagged interleave. Postgres READ COMMITTED semantics
        // for `FOR UPDATE` with a predicate: when a concurrent UPDATE
        // commits, the locker re-evaluates the WHERE against the fresh
        // row and silently skips it if the predicate no longer holds
        // — so a concurrent `delete_installation` that sets
        // `deleted_at = NOW()` cleanly causes our SELECT to return 0
        // rows on re-evaluation, and we return Ok(None).
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let active: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT id
              FROM github_installation
             WHERE id = $1
               AND deleted_at IS NULL
              FOR UPDATE
            "#,
        )
        .bind(installation_id)
        .fetch_optional(&mut *tx)
        .await
        .core()?;
        if active.is_none() {
            tx.commit().await.core()?;
            return Ok(None);
        }
        // Lock held. ON CONFLICT clears revoked_at but preserves the
        // original granted_at — the membership's history is "first
        // granted on date X, possibly revoked + re-added since." A
        // re-add doesn't restart the audit clock.
        let row = sqlx::query_as::<_, Db<GithubInstallationRepo>>(
            r#"
            INSERT INTO github_installation_repo
                (github_installation_id, github_repo_id)
            VALUES ($1, $2)
            ON CONFLICT (github_installation_id, github_repo_id) DO UPDATE
                SET revoked_at = NULL
            RETURNING github_installation_id, github_repo_id, granted_at, revoked_at
            "#,
        )
        .bind(installation_id)
        .bind(github_repo_id)
        .fetch_one(&mut *tx)
        .await
        .core()?;
        tx.commit().await.core()?;
        Ok(Some(row.0))
    }

    async fn is_membership_active(
        &self,
        installation_id: i64,
        github_repo_id: i64,
    ) -> Result<bool> {
        // Single EXISTS query joining the install + membership tables
        // with the slice-5 "currently active" predicates on both.
        // Cheaper than two round-trips; Postgres can answer from the
        // (id) PK and the composite PK index without a full scan.
        let active: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                  FROM github_installation i
                  JOIN github_installation_repo m
                    ON m.github_installation_id = i.id
                 WHERE i.id = $1
                   AND m.github_repo_id = $2
                   AND i.deleted_at IS NULL
                   AND i.suspended_at IS NULL
                   AND m.revoked_at IS NULL
            )
            "#,
        )
        .bind(installation_id)
        .bind(github_repo_id)
        .fetch_one(&self.pool)
        .await
        .core()?;
        Ok(active)
    }

    async fn revoke_membership(
        &self,
        installation_id: i64,
        github_repo_id: i64,
    ) -> Result<Option<GithubInstallationRepo>> {
        // `revoked_at IS NULL` guard means a second `removed` event for
        // the same repo is a no-op (returns None) rather than
        // overwriting the original revoke timestamp.
        let row = sqlx::query_as::<_, Db<GithubInstallationRepo>>(
            r#"
            UPDATE github_installation_repo
               SET revoked_at = NOW()
             WHERE github_installation_id = $1
               AND github_repo_id = $2
               AND revoked_at IS NULL
         RETURNING github_installation_id, github_repo_id, granted_at, revoked_at
            "#,
        )
        .bind(installation_id)
        .bind(github_repo_id)
        .fetch_optional(&self.pool)
        .await
        .core()?;
        Ok(row.into_domain())
    }
}
