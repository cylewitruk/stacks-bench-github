//! Postgres-backed `RepoStore`. The `upsert_repo_lineage` path is
//! transactional because the self-referential FKs require topological
//! ordering (source → parent → leaf); the rest are single statements.

use async_trait::async_trait;

use crate::Result;
use crate::db::Pool;
use crate::db::repo::{NewRepoIdentity, NewRepoLineage, RepoStore, SupportedRoot};
use crate::models::{GithubRepo, SupportedRepoRoot};

#[derive(Clone)]
pub struct PostgresRepoStore {
    pool: Pool,
}

impl PostgresRepoStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RepoStore for PostgresRepoStore {
    async fn lookup_repo(&self, github_repo_id: i64) -> Result<Option<GithubRepo>> {
        let row = sqlx::query_as::<_, GithubRepo>(
            r#"
            SELECT id, owner, name, default_branch, is_fork,
                   parent_github_repo_id, fork_root_github_repo_id,
                   lineage_checked_at, created_at, updated_at
              FROM github_repo
             WHERE id = $1
            "#,
        )
        .bind(github_repo_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn upsert_repo_identity(&self, identity: &NewRepoIdentity) -> Result<GithubRepo> {
        // Identity-only upsert. Deliberately omits is_fork + lineage
        // columns from the UPDATE side — they're owned by the lineage
        // path and we must not clobber a previously-walked lineage
        // back to NULL just because this call only had identity data.
        let row = sqlx::query_as::<_, GithubRepo>(
            r#"
            INSERT INTO github_repo (id, owner, name, default_branch)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO UPDATE
                SET owner          = EXCLUDED.owner,
                    name           = EXCLUDED.name,
                    default_branch = COALESCE(EXCLUDED.default_branch, github_repo.default_branch),
                    updated_at     = NOW()
            RETURNING id, owner, name, default_branch, is_fork,
                      parent_github_repo_id, fork_root_github_repo_id,
                      lineage_checked_at, created_at, updated_at
            "#,
        )
        .bind(identity.id)
        .bind(&identity.owner)
        .bind(&identity.name)
        .bind(
            identity
                .default_branch
                .as_deref(),
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    async fn upsert_repo_lineage(&self, lineage: &NewRepoLineage) -> Result<GithubRepo> {
        let mut tx = self.pool.begin().await?;

        // Topological order: source first, then parent (if distinct),
        // then leaf. Each ancestor goes through an identity-only upsert
        // so we don't blow away its own lineage if it was previously
        // walked. The leaf is the only row whose lineage columns we
        // write here.
        let source_id = match &lineage.source {
            Some(src) => Some(upsert_identity_in_tx(&mut tx, src).await?),
            None => None,
        };
        let parent_id = match &lineage.parent {
            Some(par) if Some(par.id) != source_id => {
                Some(upsert_identity_in_tx(&mut tx, par).await?)
            }
            // parent == source (one-hop fork) or no parent at all
            Some(par) => Some(par.id),
            None => None,
        };

        // Leaf insert with all lineage columns + lineage_checked_at.
        let row = sqlx::query_as::<_, GithubRepo>(
            r#"
            INSERT INTO github_repo
                (id, owner, name, default_branch, is_fork,
                 parent_github_repo_id, fork_root_github_repo_id,
                 lineage_checked_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
            ON CONFLICT (id) DO UPDATE
                SET owner                    = EXCLUDED.owner,
                    name                     = EXCLUDED.name,
                    default_branch           = COALESCE(EXCLUDED.default_branch, github_repo.default_branch),
                    is_fork                  = EXCLUDED.is_fork,
                    parent_github_repo_id    = EXCLUDED.parent_github_repo_id,
                    fork_root_github_repo_id = EXCLUDED.fork_root_github_repo_id,
                    lineage_checked_at       = NOW(),
                    updated_at               = NOW()
            RETURNING id, owner, name, default_branch, is_fork,
                      parent_github_repo_id, fork_root_github_repo_id,
                      lineage_checked_at, created_at, updated_at
            "#,
        )
        .bind(lineage.repo.id)
        .bind(&lineage.repo.owner)
        .bind(&lineage.repo.name)
        .bind(lineage.repo.default_branch.as_deref())
        .bind(lineage.is_fork)
        .bind(parent_id)
        .bind(source_id)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(row)
    }

    async fn is_supported_lineage(&self, github_repo_id: i64) -> Result<bool> {
        // Single-statement lineage check: join github_repo to
        // supported_repo_root either directly (id matches) OR via
        // fork_root_github_repo_id. is_enabled gates both cases.
        let supported: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                  FROM github_repo r
                  JOIN supported_repo_root s
                    ON s.github_repo_id = r.id
                       OR s.github_repo_id = r.fork_root_github_repo_id
                 WHERE r.id = $1
                   AND s.is_enabled = TRUE
            )
            "#,
        )
        .bind(github_repo_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(supported)
    }

    async fn upsert_supported_root(
        &self,
        github_repo_id: i64,
        note: Option<&str>,
    ) -> Result<SupportedRepoRoot> {
        let row = sqlx::query_as::<_, SupportedRepoRoot>(
            r#"
            INSERT INTO supported_repo_root (github_repo_id, is_enabled, note)
            VALUES ($1, TRUE, $2)
            ON CONFLICT (github_repo_id) DO UPDATE
                SET is_enabled = TRUE,
                    note       = COALESCE(EXCLUDED.note, supported_repo_root.note),
                    updated_at = NOW()
            RETURNING github_repo_id, is_enabled, note, created_at, updated_at
            "#,
        )
        .bind(github_repo_id)
        .bind(note)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    async fn disable_supported_root(
        &self,
        github_repo_id: i64,
    ) -> Result<Option<SupportedRepoRoot>> {
        let row = sqlx::query_as::<_, SupportedRepoRoot>(
            r#"
            UPDATE supported_repo_root
               SET is_enabled = FALSE, updated_at = NOW()
             WHERE github_repo_id = $1
         RETURNING github_repo_id, is_enabled, note, created_at, updated_at
            "#,
        )
        .bind(github_repo_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn list_supported_roots(&self) -> Result<Vec<SupportedRoot>> {
        let rows = sqlx::query_as::<
            _,
            (
                i64,
                String,
                String,
                bool,
                Option<String>,
                chrono::DateTime<chrono::Utc>,
                chrono::DateTime<chrono::Utc>,
            ),
        >(
            r#"
            SELECT s.github_repo_id, r.owner, r.name, s.is_enabled, s.note,
                   s.created_at, s.updated_at
              FROM supported_repo_root s
              JOIN github_repo r ON r.id = s.github_repo_id
          ORDER BY r.owner, r.name
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(id, owner, name, is_enabled, note, created_at, updated_at)| SupportedRoot {
                github_repo_id: id,
                owner,
                name,
                is_enabled,
                note,
                created_at,
                updated_at,
            })
            .collect())
    }
}

async fn upsert_identity_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    identity: &NewRepoIdentity,
) -> Result<i64> {
    // Same shape as upsert_repo_identity, but inside a caller's
    // transaction. Returns just the id since callers only need the FK.
    sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO github_repo (id, owner, name, default_branch)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (id) DO UPDATE
            SET owner          = EXCLUDED.owner,
                name           = EXCLUDED.name,
                default_branch = COALESCE(EXCLUDED.default_branch, github_repo.default_branch),
                updated_at     = NOW()
        RETURNING id
        "#,
    )
    .bind(identity.id)
    .bind(&identity.owner)
    .bind(&identity.name)
    .bind(
        identity
            .default_branch
            .as_deref(),
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(Into::into)
}
