//! Postgres-backed `PolicyStore`. Single-statement methods throughout —
//! no cross-row transactions needed at this layer. The bulk-disable on
//! `installation.deleted` happens inside `delete_installation` (see
//! `postgres_installation.rs`), not here.

use async_trait::async_trait;

use crate::Result;
use crate::db::Pool;
use crate::db::policy::PolicyStore;
use crate::models::{
    SourceRepoPolicy, TargetRepoPolicy, TriggerKind, TriggerMatchSpec, TriggerPolicy,
};

#[derive(Clone)]
pub struct PostgresPolicyStore {
    pool: Pool,
}

impl PostgresPolicyStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PolicyStore for PostgresPolicyStore {
    async fn lookup_target_policy(
        &self,
        install_id: i64,
        repo_id: i64,
    ) -> Result<Option<TargetRepoPolicy>> {
        let row = sqlx::query_as::<_, TargetRepoPolicy>(
            r#"
            SELECT github_installation_id, github_repo_id, is_enabled, note,
                   created_at, updated_at
              FROM target_repo_policy
             WHERE github_installation_id = $1
               AND github_repo_id = $2
            "#,
        )
        .bind(install_id)
        .bind(repo_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn lookup_source_policy(
        &self,
        install_id: i64,
        repo_id: i64,
    ) -> Result<Option<SourceRepoPolicy>> {
        let row = sqlx::query_as::<_, SourceRepoPolicy>(
            r#"
            SELECT github_installation_id, github_repo_id, is_enabled, note,
                   created_at, updated_at
              FROM source_repo_policy
             WHERE github_installation_id = $1
               AND github_repo_id = $2
            "#,
        )
        .bind(install_id)
        .bind(repo_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn list_enabled_triggers(
        &self,
        install_id: i64,
        repo_id: i64,
        kind: TriggerKind,
    ) -> Result<Vec<TriggerPolicy>> {
        // Slice 5 follow-up review fix: require the parent target_repo_policy
        // to ALSO be enabled. Without this, an operator who runs
        // `sbgh-cli policy target disable` would leave existing
        // `branch_push`/`tag_created` triggers active — the cascade
        // helper is only wired into the `installation_repositories.removed`
        // path, so a manual disable wouldn't otherwise propagate.
        let rows = sqlx::query_as::<_, TriggerPolicy>(
            r#"
            SELECT t.id, t.github_installation_id, t.github_repo_id, t.trigger_kind,
                   t.match_spec, t.bench_args, t.is_enabled, t.note,
                   t.pinned, t.pinned_until, t.created_at, t.updated_at
              FROM trigger_policy t
              JOIN target_repo_policy p
                ON p.github_installation_id = t.github_installation_id
               AND p.github_repo_id = t.github_repo_id
             WHERE t.github_installation_id = $1
               AND t.github_repo_id = $2
               AND t.trigger_kind = $3
               AND t.is_enabled = TRUE
               AND p.is_enabled = TRUE
          ORDER BY t.id
            "#,
        )
        .bind(install_id)
        .bind(repo_id)
        .bind(kind)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn list_pinned_triggers(&self) -> Result<Vec<TriggerPolicy>> {
        // Same parent-target-enabled guard as `list_enabled_triggers`, but
        // global (every install/repo) and filtered to pinned rows. Expiry is
        // applied by the resolver, not here.
        let rows = sqlx::query_as::<_, TriggerPolicy>(
            r#"
            SELECT t.id, t.github_installation_id, t.github_repo_id, t.trigger_kind,
                   t.match_spec, t.bench_args, t.is_enabled, t.note,
                   t.pinned, t.pinned_until, t.created_at, t.updated_at
              FROM trigger_policy t
              JOIN target_repo_policy p
                ON p.github_installation_id = t.github_installation_id
               AND p.github_repo_id = t.github_repo_id
             WHERE t.is_enabled = TRUE
               AND t.pinned = TRUE
               AND p.is_enabled = TRUE
          ORDER BY t.id
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn upsert_target_policy(
        &self,
        install_id: i64,
        repo_id: i64,
        note: Option<&str>,
    ) -> Result<TargetRepoPolicy> {
        let row = sqlx::query_as::<_, TargetRepoPolicy>(
            r#"
            INSERT INTO target_repo_policy (github_installation_id, github_repo_id, is_enabled, note)
            VALUES ($1, $2, TRUE, $3)
            ON CONFLICT (github_installation_id, github_repo_id) DO UPDATE
                SET is_enabled = TRUE,
                    note       = COALESCE(EXCLUDED.note, target_repo_policy.note),
                    updated_at = NOW()
            RETURNING github_installation_id, github_repo_id, is_enabled, note,
                      created_at, updated_at
            "#,
        )
        .bind(install_id)
        .bind(repo_id)
        .bind(note)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    async fn disable_target_policy(
        &self,
        install_id: i64,
        repo_id: i64,
    ) -> Result<Option<TargetRepoPolicy>> {
        let row = sqlx::query_as::<_, TargetRepoPolicy>(
            r#"
            UPDATE target_repo_policy
               SET is_enabled = FALSE, updated_at = NOW()
             WHERE github_installation_id = $1
               AND github_repo_id = $2
         RETURNING github_installation_id, github_repo_id, is_enabled, note,
                   created_at, updated_at
            "#,
        )
        .bind(install_id)
        .bind(repo_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn upsert_source_policy(
        &self,
        install_id: i64,
        repo_id: i64,
        note: Option<&str>,
    ) -> Result<SourceRepoPolicy> {
        let row = sqlx::query_as::<_, SourceRepoPolicy>(
            r#"
            INSERT INTO source_repo_policy (github_installation_id, github_repo_id, is_enabled, note)
            VALUES ($1, $2, TRUE, $3)
            ON CONFLICT (github_installation_id, github_repo_id) DO UPDATE
                SET is_enabled = TRUE,
                    note       = COALESCE(EXCLUDED.note, source_repo_policy.note),
                    updated_at = NOW()
            RETURNING github_installation_id, github_repo_id, is_enabled, note,
                      created_at, updated_at
            "#,
        )
        .bind(install_id)
        .bind(repo_id)
        .bind(note)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    async fn disable_source_policy(
        &self,
        install_id: i64,
        repo_id: i64,
    ) -> Result<Option<SourceRepoPolicy>> {
        let row = sqlx::query_as::<_, SourceRepoPolicy>(
            r#"
            UPDATE source_repo_policy
               SET is_enabled = FALSE, updated_at = NOW()
             WHERE github_installation_id = $1
               AND github_repo_id = $2
         RETURNING github_installation_id, github_repo_id, is_enabled, note,
                   created_at, updated_at
            "#,
        )
        .bind(install_id)
        .bind(repo_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn add_trigger_policy(
        &self,
        install_id: i64,
        repo_id: i64,
        kind: TriggerKind,
        match_spec: &TriggerMatchSpec,
        bench_args: Option<&str>,
        note: Option<&str>,
    ) -> Result<TriggerPolicy> {
        let spec_json = serde_json::to_value(match_spec).expect("TriggerMatchSpec serialises");
        let row = sqlx::query_as::<_, TriggerPolicy>(
            r#"
            INSERT INTO trigger_policy
                (github_installation_id, github_repo_id, trigger_kind,
                 match_spec, bench_args, is_enabled, note)
            VALUES ($1, $2, $3, $4, $5, TRUE, $6)
            RETURNING id, github_installation_id, github_repo_id, trigger_kind,
                      match_spec, bench_args, is_enabled, note,
                      pinned, pinned_until, created_at, updated_at
            "#,
        )
        .bind(install_id)
        .bind(repo_id)
        .bind(kind)
        .bind(spec_json)
        .bind(bench_args)
        .bind(note)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    async fn disable_trigger_policy(&self, trigger_id: i64) -> Result<Option<TriggerPolicy>> {
        let row = sqlx::query_as::<_, TriggerPolicy>(
            r#"
            UPDATE trigger_policy
               SET is_enabled = FALSE, updated_at = NOW()
             WHERE id = $1
         RETURNING id, github_installation_id, github_repo_id, trigger_kind,
                   match_spec, bench_args, is_enabled, note,
                   pinned, pinned_until, created_at, updated_at
            "#,
        )
        .bind(trigger_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn list_triggers(&self, install_id: i64, repo_id: i64) -> Result<Vec<TriggerPolicy>> {
        let rows = sqlx::query_as::<_, TriggerPolicy>(
            r#"
            SELECT id, github_installation_id, github_repo_id, trigger_kind,
                   match_spec, bench_args, is_enabled, note,
                   pinned, pinned_until, created_at, updated_at
              FROM trigger_policy
             WHERE github_installation_id = $1
               AND github_repo_id = $2
          ORDER BY id
            "#,
        )
        .bind(install_id)
        .bind(repo_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn disable_target_and_triggers(&self, install_id: i64, repo_id: i64) -> Result<()> {
        // One transaction so any subsequent reader sees either both
        // disabled or neither. Trigger UPDATE runs first because the
        // FK from trigger_policy → target_repo_policy means trigger
        // rows reference the parent; doing them in the same direction
        // as the FK avoids surprising any future deferred-constraint
        // behavior even though current UPDATEs don't trigger FK checks.
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            UPDATE trigger_policy
               SET is_enabled = FALSE, updated_at = NOW()
             WHERE github_installation_id = $1
               AND github_repo_id = $2
               AND is_enabled = TRUE
            "#,
        )
        .bind(install_id)
        .bind(repo_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"
            UPDATE target_repo_policy
               SET is_enabled = FALSE, updated_at = NOW()
             WHERE github_installation_id = $1
               AND github_repo_id = $2
               AND is_enabled = TRUE
            "#,
        )
        .bind(install_id)
        .bind(repo_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}
