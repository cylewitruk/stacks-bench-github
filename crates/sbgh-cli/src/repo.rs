//! Operator commands for the `supported_repo_root` allowlist (slice 4).
//!
//! Same shape as `installer`: resolve owner/name → numeric id via
//! GitHub's unauthenticated `/repos/{owner}/{repo}` endpoint, then
//! upsert (or disable, or list). Insertion needs two SQL writes — the
//! `github_repo` identity row first (FK target), then the
//! `supported_repo_root` row itself. They go in one transaction so a
//! partial failure can't leave a dangling identity row.

use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use sbgh_core::db::Pool;
use sbgh_core::models::SupportedRepoRoot;
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RepoError {
    #[error("GitHub API: repository `{0}` not found")]
    RepoNotFound(String),
    #[error("Database: github_repo_id {0} is not on the supported list")]
    NotOnSupportedList(i64),
    #[error("GitHub API rejected the request ({status}): {body}")]
    GithubRejected { status: u16, body: String },
    #[error("HTTP transport: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Database: {0}")]
    Db(#[from] sqlx::Error),
}

/// Resolve `owner/name` → numeric repo id via GitHub's unauthenticated
/// `/repos/{owner}/{repo}` endpoint, then transactionally upsert the
/// `github_repo` identity row + the `supported_repo_root` row.
///
/// Idempotent on re-run: re-allowing an already-allowed repo re-asserts
/// `is_enabled=TRUE` and refreshes owner/name (in case the repo was
/// renamed) without disturbing fork lineage columns (those are owned
/// by the processor's runtime lineage walk).
pub async fn allow_repo_root(
    pool: &Pool,
    api_base_url: &str,
    owner: &str,
    name: &str,
    note: Option<&str>,
) -> Result<AllowedRepoRoot, RepoError> {
    let resolved = resolve_repo(api_base_url, owner, name).await?;

    let mut tx = pool.begin().await?;

    // Identity-only upsert. Deliberately omits is_fork + lineage
    // columns from the UPDATE side — those are processor-owned and
    // must not be clobbered back to NULL by a CLI re-run.
    sqlx::query(
        r#"
        INSERT INTO github_repo (id, owner, name, default_branch)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (id) DO UPDATE
            SET owner          = EXCLUDED.owner,
                name           = EXCLUDED.name,
                default_branch = COALESCE(EXCLUDED.default_branch, github_repo.default_branch),
                updated_at     = NOW()
        "#,
    )
    .bind(resolved.id)
    .bind(&resolved.owner)
    .bind(&resolved.name)
    .bind(
        resolved
            .default_branch
            .as_deref(),
    )
    .execute(&mut *tx)
    .await?;

    let supported: SupportedRepoRoot = sqlx::query_as(
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
    .bind(resolved.id)
    .bind(note)
    .fetch_one(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(AllowedRepoRoot {
        github_repo_id: supported.github_repo_id,
        owner: resolved.owner,
        name: resolved.name,
        is_enabled: supported.is_enabled,
        note: supported.note,
    })
}

/// Soft-disable by owner/name. Resolves to id via the GH API first so
/// the SQL UPDATE targets the numeric PK (same rename-resilient
/// pattern as `installer disable --login`).
pub async fn disable_repo_root(
    pool: &Pool,
    api_base_url: &str,
    owner: &str,
    name: &str,
) -> Result<AllowedRepoRoot, RepoError> {
    let resolved = resolve_repo(api_base_url, owner, name).await?;
    disable_repo_root_by_id(pool, resolved.id).await
}

/// Soft-disable by numeric repo id. Emergency path: works during GH
/// outages or when the operator pulled the id from `repo list`.
pub async fn disable_repo_root_by_id(
    pool: &Pool,
    github_repo_id: i64,
) -> Result<AllowedRepoRoot, RepoError> {
    let row: Option<(i64, String, String, bool, Option<String>)> = sqlx::query_as(
        r#"
        UPDATE supported_repo_root s
           SET is_enabled = FALSE, updated_at = NOW()
          FROM github_repo r
         WHERE s.github_repo_id = $1
           AND r.id = s.github_repo_id
       RETURNING s.github_repo_id, r.owner, r.name, s.is_enabled, s.note
        "#,
    )
    .bind(github_repo_id)
    .fetch_optional(pool)
    .await?;
    let (id, owner, name, is_enabled, note) =
        row.ok_or(RepoError::NotOnSupportedList(github_repo_id))?;
    Ok(AllowedRepoRoot {
        github_repo_id: id,
        owner,
        name,
        is_enabled,
        note,
    })
}

pub async fn list_repo_roots(pool: &Pool) -> Result<Vec<AllowedRepoRoot>, RepoError> {
    let rows: Vec<(i64, String, String, bool, Option<String>)> = sqlx::query_as(
        r#"
        SELECT s.github_repo_id, r.owner, r.name, s.is_enabled, s.note
          FROM supported_repo_root s
          JOIN github_repo r ON r.id = s.github_repo_id
      ORDER BY r.owner, r.name
        "#,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(id, owner, name, is_enabled, note)| AllowedRepoRoot {
            github_repo_id: id,
            owner,
            name,
            is_enabled,
            note,
        })
        .collect())
}

/// Display-friendly join of `supported_repo_root` + the underlying
/// `github_repo` identity. Used by `sbgh-cli repo list` and as the
/// return value for allow/disable so the CLI can echo the operator's
/// owner/name input without an extra DB round-trip.
#[derive(Debug, Clone)]
pub struct AllowedRepoRoot {
    pub github_repo_id: i64,
    pub owner: String,
    pub name: String,
    pub is_enabled: bool,
    pub note: Option<String>,
}

#[derive(Debug, Clone)]
struct ResolvedRepo {
    id: i64,
    owner: String,
    name: String,
    default_branch: Option<String>,
}

async fn resolve_repo(
    api_base_url: &str,
    owner: &str,
    name: &str,
) -> Result<ResolvedRepo, RepoError> {
    let url = format!("{}/repos/{}/{}", api_base_url.trim_end_matches('/'), owner, name);

    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/vnd.github+json"));
    headers.insert(USER_AGENT, HeaderValue::from_static("sbgh-cli"));

    let resp = reqwest::Client::new()
        .get(&url)
        .headers(headers)
        .send()
        .await?;

    if resp.status().as_u16() == 404 {
        return Err(RepoError::RepoNotFound(format!("{owner}/{name}")));
    }
    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp
            .text()
            .await
            .unwrap_or_default();
        return Err(RepoError::GithubRejected { status, body });
    }

    #[derive(Deserialize)]
    struct RepoResp {
        id: i64,
        name: String,
        default_branch: Option<String>,
        owner: OwnerResp,
    }
    #[derive(Deserialize)]
    struct OwnerResp {
        login: String,
    }
    let body: RepoResp = resp.json().await?;
    Ok(ResolvedRepo {
        id: body.id,
        owner: body.owner.login,
        name: body.name,
        default_branch: body.default_branch,
    })
}
