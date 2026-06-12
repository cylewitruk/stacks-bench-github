//! `/api/jobs` — benchmark run visibility.

use axum::Json;
use axum::extract::{Query, State};
use sbgh_api::JobView;
use sbgh_core::models::Job;
use serde::Deserialize;

use crate::api::conv::enum_str;
use crate::api::error::ApiErr;
use crate::api::state::ApiState;

/// `job_status` enum values — validated before binding so a bad value is a
/// clean 400, not a Postgres cast error.
const JOB_STATUSES: &[&str] = &["queued", "claimed", "running", "completed", "failed", "cancelled"];

#[derive(Debug, Deserialize)]
pub struct ListParams {
    status: Option<String>,
    limit: Option<i64>,
}

fn view(r: Job) -> JobView {
    JobView {
        id: r.id.to_string(),
        install_id: r.github_installation_id,
        repo_id: r.github_repo_id,
        status: enum_str(&r.status),
        source: enum_str(&r.source),
        intent: enum_str(&r.intent),
        task_kind: enum_str(&r.task_kind),
        build_target: enum_str(&r.build_target),
        git_ref_kind: enum_str(&r.git_ref_kind),
        git_ref_display: r.git_ref_display,
        commit: r.git_commit_hash,
        created_at: r.created_at.to_rfc3339(),
    }
}

pub async fn list(
    State(s): State<ApiState>,
    Query(p): Query<ListParams>,
) -> Result<Json<Vec<JobView>>, ApiErr> {
    if let Some(st) = &p.status {
        if !JOB_STATUSES.contains(&st.as_str()) {
            return Err(ApiErr::bad_request(format!("unknown status {st:?}")));
        }
    }
    let limit = p
        .limit
        .unwrap_or(50)
        .clamp(1, 500);

    let rows = sqlx::query_as::<_, Job>(
        r#"
        SELECT * FROM job
         WHERE ($1::text IS NULL OR status = $1::job_status)
         ORDER BY created_at DESC
         LIMIT $2
        "#,
    )
    .bind(&p.status)
    .bind(limit)
    .fetch_all(&s.pool)
    .await?;
    Ok(Json(
        rows.into_iter()
            .map(view)
            .collect(),
    ))
}
