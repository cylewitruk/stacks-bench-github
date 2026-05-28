//! Operator commands for the three slice 5 policy tables.
//!
//! Three nested CLI groups (`policy target`, `policy source`,
//! `policy trigger`), each backed by SQL the CLI runs as DB owner.
//! Symmetric with `installer` / `repo` from slices 3 / 4: a small
//! library surface here, clap subcommand wiring in `main.rs`.
//!
//! Unlike `installer allow` and `repo allow`, these commands don't
//! resolve via the GitHub API — the operator pulls numeric ids from
//! `sbgh-cli installer list` + `sbgh-cli repo list` first, then types
//! them in. Rationale: policies are per-(install, repo) pairs and the
//! cross-product of "which installs the operator means" + "which
//! repos" is small enough that requiring numeric ids avoids
//! ambiguity (whose `octo-org` did the operator mean?).

use sbgh_core::db::Pool;
use sbgh_core::models::{
    SourceRepoPolicy, TargetRepoPolicy, TriggerKind, TriggerMatchSpec, TriggerPolicy,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PolicyError {
    #[error("Database: no row matched for ({install_id}, {repo_id})")]
    NotFound { install_id: i64, repo_id: i64 },
    #[error("Database: no trigger_policy row with id {0}")]
    TriggerNotFound(i64),
    #[error(
        "Database: target_repo_policy row required first for ({install_id}, {repo_id}) — run \
         `policy target allow` before adding triggers"
    )]
    MissingTargetForTrigger { install_id: i64, repo_id: i64 },
    #[error("Invalid match_spec JSON: {0}")]
    InvalidMatchSpec(String),
    #[error("Database: {0}")]
    Db(#[from] sqlx::Error),
}

// ─── target_repo_policy ────────────────────────────────────────────────

pub async fn allow_target_policy(
    pool: &Pool,
    install_id: i64,
    repo_id: i64,
    note: Option<&str>,
) -> Result<TargetRepoPolicy, PolicyError> {
    let row: TargetRepoPolicy = sqlx::query_as(
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
    .fetch_one(pool)
    .await?;
    Ok(row)
}

pub async fn disable_target_policy(
    pool: &Pool,
    install_id: i64,
    repo_id: i64,
) -> Result<TargetRepoPolicy, PolicyError> {
    // Slice 5 follow-up review fix: cascade-disable matching
    // trigger_policy rows in the same transaction. Otherwise an
    // operator who runs `policy target disable` leaves
    // branch_push/tag_created triggers active — runtime gating in
    // `list_enabled_triggers` is the actual safety net, but the
    // cascade here keeps `policy trigger list` reflective of reality.
    let mut tx = pool.begin().await?;
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
    let row: Option<TargetRepoPolicy> = sqlx::query_as(
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
    .fetch_optional(&mut *tx)
    .await?;
    let row = row.ok_or(PolicyError::NotFound { install_id, repo_id })?;
    tx.commit().await?;
    Ok(row)
}

pub async fn list_target_policies(
    pool: &Pool,
    install_id: Option<i64>,
) -> Result<Vec<TargetRepoPolicy>, PolicyError> {
    let rows = match install_id {
        Some(id) => {
            sqlx::query_as::<_, TargetRepoPolicy>(
                r#"
            SELECT github_installation_id, github_repo_id, is_enabled, note,
                   created_at, updated_at
              FROM target_repo_policy
             WHERE github_installation_id = $1
          ORDER BY github_repo_id
            "#,
            )
            .bind(id)
            .fetch_all(pool)
            .await?
        }
        None => {
            sqlx::query_as::<_, TargetRepoPolicy>(
                r#"
            SELECT github_installation_id, github_repo_id, is_enabled, note,
                   created_at, updated_at
              FROM target_repo_policy
          ORDER BY github_installation_id, github_repo_id
            "#,
            )
            .fetch_all(pool)
            .await?
        }
    };
    Ok(rows)
}

// ─── source_repo_policy ────────────────────────────────────────────────

pub async fn allow_source_policy(
    pool: &Pool,
    install_id: i64,
    repo_id: i64,
    note: Option<&str>,
) -> Result<SourceRepoPolicy, PolicyError> {
    let row: SourceRepoPolicy = sqlx::query_as(
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
    .fetch_one(pool)
    .await?;
    Ok(row)
}

pub async fn disable_source_policy(
    pool: &Pool,
    install_id: i64,
    repo_id: i64,
) -> Result<SourceRepoPolicy, PolicyError> {
    let row: Option<SourceRepoPolicy> = sqlx::query_as(
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
    .fetch_optional(pool)
    .await?;
    row.ok_or(PolicyError::NotFound { install_id, repo_id })
}

pub async fn list_source_policies(
    pool: &Pool,
    install_id: Option<i64>,
) -> Result<Vec<SourceRepoPolicy>, PolicyError> {
    let rows = match install_id {
        Some(id) => {
            sqlx::query_as::<_, SourceRepoPolicy>(
                r#"
            SELECT github_installation_id, github_repo_id, is_enabled, note,
                   created_at, updated_at
              FROM source_repo_policy
             WHERE github_installation_id = $1
          ORDER BY github_repo_id
            "#,
            )
            .bind(id)
            .fetch_all(pool)
            .await?
        }
        None => {
            sqlx::query_as::<_, SourceRepoPolicy>(
                r#"
            SELECT github_installation_id, github_repo_id, is_enabled, note,
                   created_at, updated_at
              FROM source_repo_policy
          ORDER BY github_installation_id, github_repo_id
            "#,
            )
            .fetch_all(pool)
            .await?
        }
    };
    Ok(rows)
}

// ─── trigger_policy ────────────────────────────────────────────────────

/// Add a new trigger_policy row. `match_spec_json` is the operator's
/// raw JSON string; we parse it through `TriggerMatchSpec` first so a
/// bad spec is rejected before hitting the DB.
pub async fn add_trigger_policy(
    pool: &Pool,
    install_id: i64,
    repo_id: i64,
    kind: TriggerKind,
    match_spec_json: &str,
    bench_args: Option<&str>,
    note: Option<&str>,
) -> Result<TriggerPolicy, PolicyError> {
    // Validate shape now so a malformed spec is rejected at the CLI
    // boundary rather than blowing up later in the processor.
    let parsed: TriggerMatchSpec = serde_json::from_str(match_spec_json)
        .map_err(|e| PolicyError::InvalidMatchSpec(e.to_string()))?;

    // FK enforces "target_repo_policy must exist for (install, repo)
    // before a trigger can be added." Pre-check so the CLI returns a
    // friendly error instead of a Postgres FK violation string.
    let target_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM target_repo_policy WHERE github_installation_id = $1 AND \
         github_repo_id = $2)",
    )
    .bind(install_id)
    .bind(repo_id)
    .fetch_one(pool)
    .await?;
    if !target_exists {
        return Err(PolicyError::MissingTargetForTrigger { install_id, repo_id });
    }

    let spec_json = serde_json::to_value(&parsed).expect("TriggerMatchSpec serialises");
    let row: TriggerPolicy = sqlx::query_as(
        r#"
        INSERT INTO trigger_policy
            (github_installation_id, github_repo_id, trigger_kind, match_spec, bench_args,
             is_enabled, note)
        VALUES ($1, $2, $3, $4, $5, TRUE, $6)
        RETURNING id, github_installation_id, github_repo_id, trigger_kind,
                  match_spec, bench_args, is_enabled, note, created_at, updated_at
        "#,
    )
    .bind(install_id)
    .bind(repo_id)
    .bind(kind)
    .bind(spec_json)
    .bind(bench_args)
    .bind(note)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

pub async fn disable_trigger_policy(
    pool: &Pool,
    trigger_id: i64,
) -> Result<TriggerPolicy, PolicyError> {
    let row: Option<TriggerPolicy> = sqlx::query_as(
        r#"
        UPDATE trigger_policy
           SET is_enabled = FALSE, updated_at = NOW()
         WHERE id = $1
     RETURNING id, github_installation_id, github_repo_id, trigger_kind,
               match_spec, bench_args, is_enabled, note, created_at, updated_at
        "#,
    )
    .bind(trigger_id)
    .fetch_optional(pool)
    .await?;
    row.ok_or(PolicyError::TriggerNotFound(trigger_id))
}

pub async fn list_trigger_policies(
    pool: &Pool,
    install_id: Option<i64>,
    repo_id: Option<i64>,
) -> Result<Vec<TriggerPolicy>, PolicyError> {
    // Optional filtering by install + repo so operators can scope
    // their queries. Both Nones = list everything (small-scale ops).
    let rows = sqlx::query_as::<_, TriggerPolicy>(
        r#"
        SELECT id, github_installation_id, github_repo_id, trigger_kind,
               match_spec, bench_args, is_enabled, note, created_at, updated_at
          FROM trigger_policy
         WHERE ($1::BIGINT IS NULL OR github_installation_id = $1)
           AND ($2::BIGINT IS NULL OR github_repo_id = $2)
      ORDER BY github_installation_id, github_repo_id, id
        "#,
    )
    .bind(install_id)
    .bind(repo_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}
