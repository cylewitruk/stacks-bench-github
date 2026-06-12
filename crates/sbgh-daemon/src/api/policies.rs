//! `/api/policies/{target,source,triggers}` — per-installation gating of
//! which repos are benchmark targets / trusted PR sources, and which
//! pushes/tags auto-trigger a baseline.

use axum::Json;
use axum::extract::{Query, State};
use sbgh_api::{
    AddTriggerRequest, AllowPolicyRequest, DisablePolicyRequest, PinTriggerRequest, PolicyView,
    TriggerView,
};
use sbgh_core::admin;
use sbgh_core::models::{SourceRepoPolicy, TargetRepoPolicy, TriggerKind, TriggerPolicy};
use serde::Deserialize;

use crate::api::conv::{enum_str, parse_enum};
use crate::api::error::ApiErr;
use crate::api::extract::ApiJson;
use crate::api::state::ApiState;

#[derive(Debug, Deserialize)]
pub struct InstallFilter {
    install_id: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct TriggerFilter {
    install_id: Option<i64>,
    repo_id: Option<i64>,
}

fn target_view(r: TargetRepoPolicy) -> PolicyView {
    PolicyView {
        install_id: r.github_installation_id,
        repo_id: r.github_repo_id,
        is_enabled: r.is_enabled,
        note: r.note,
    }
}

fn source_view(r: SourceRepoPolicy) -> PolicyView {
    PolicyView {
        install_id: r.github_installation_id,
        repo_id: r.github_repo_id,
        is_enabled: r.is_enabled,
        note: r.note,
    }
}

fn trigger_view(r: TriggerPolicy) -> TriggerView {
    TriggerView {
        id: r.id,
        install_id: r.github_installation_id,
        repo_id: r.github_repo_id,
        kind: enum_str(&r.trigger_kind),
        match_spec: r.match_spec.0,
        bench_args: r.bench_args,
        is_enabled: r.is_enabled,
        note: r.note,
        pinned: r.pinned,
        pinned_until: r
            .pinned_until
            .map(|t| t.to_rfc3339()),
    }
}

// ─── target ────────────────────────────────────────────────────────────

pub async fn list_target(
    State(s): State<ApiState>,
    Query(f): Query<InstallFilter>,
) -> Result<Json<Vec<PolicyView>>, ApiErr> {
    let rows = admin::list_target_policies(&s.pool, f.install_id).await?;
    Ok(Json(
        rows.into_iter()
            .map(target_view)
            .collect(),
    ))
}

pub async fn allow_target(
    State(s): State<ApiState>,
    ApiJson(req): ApiJson<AllowPolicyRequest>,
) -> Result<Json<PolicyView>, ApiErr> {
    let row = admin::allow_target_policy(&s.pool, req.install_id, req.repo_id, req.note.as_deref())
        .await?;
    Ok(Json(target_view(row)))
}

pub async fn disable_target(
    State(s): State<ApiState>,
    ApiJson(req): ApiJson<DisablePolicyRequest>,
) -> Result<Json<PolicyView>, ApiErr> {
    let row = admin::disable_target_policy(&s.pool, req.install_id, req.repo_id).await?;
    Ok(Json(target_view(row)))
}

// ─── source ────────────────────────────────────────────────────────────

pub async fn list_source(
    State(s): State<ApiState>,
    Query(f): Query<InstallFilter>,
) -> Result<Json<Vec<PolicyView>>, ApiErr> {
    let rows = admin::list_source_policies(&s.pool, f.install_id).await?;
    Ok(Json(
        rows.into_iter()
            .map(source_view)
            .collect(),
    ))
}

pub async fn allow_source(
    State(s): State<ApiState>,
    ApiJson(req): ApiJson<AllowPolicyRequest>,
) -> Result<Json<PolicyView>, ApiErr> {
    let row = admin::allow_source_policy(&s.pool, req.install_id, req.repo_id, req.note.as_deref())
        .await?;
    Ok(Json(source_view(row)))
}

pub async fn disable_source(
    State(s): State<ApiState>,
    ApiJson(req): ApiJson<DisablePolicyRequest>,
) -> Result<Json<PolicyView>, ApiErr> {
    let row = admin::disable_source_policy(&s.pool, req.install_id, req.repo_id).await?;
    Ok(Json(source_view(row)))
}

// ─── triggers ──────────────────────────────────────────────────────────

pub async fn list_triggers(
    State(s): State<ApiState>,
    Query(f): Query<TriggerFilter>,
) -> Result<Json<Vec<TriggerView>>, ApiErr> {
    let rows = admin::list_trigger_policies(&s.pool, f.install_id, f.repo_id).await?;
    Ok(Json(
        rows.into_iter()
            .map(trigger_view)
            .collect(),
    ))
}

pub async fn add_trigger(
    State(s): State<ApiState>,
    ApiJson(req): ApiJson<AddTriggerRequest>,
) -> Result<Json<TriggerView>, ApiErr> {
    // Only webhook trigger kinds are valid policies; reject the rest up front so
    // the error matches the message (and never reaches the DB).
    // `add_trigger_policy` enforces the same boundary for non-API callers.
    let kind: TriggerKind = parse_enum(&req.kind)
        .filter(|k| matches!(k, TriggerKind::BranchPush | TriggerKind::TagCreated))
        .ok_or_else(|| ApiErr::bad_request("`kind` must be `branch_push` or `tag_created`"))?;
    let row = admin::add_trigger_policy(
        &s.pool,
        req.install_id,
        req.repo_id,
        kind,
        &req.match_spec.to_string(),
        req.bench_args.as_deref(),
        req.note.as_deref(),
    )
    .await?;
    Ok(Json(trigger_view(row)))
}

#[derive(Debug, Deserialize)]
pub struct TriggerId {
    pub id: i64,
}

pub async fn disable_trigger(
    State(s): State<ApiState>,
    axum::extract::Path(TriggerId { id }): axum::extract::Path<TriggerId>,
) -> Result<Json<TriggerView>, ApiErr> {
    let row = admin::disable_trigger_policy(&s.pool, id).await?;
    Ok(Json(trigger_view(row)))
}

pub async fn pin_trigger(
    State(s): State<ApiState>,
    axum::extract::Path(TriggerId { id }): axum::extract::Path<TriggerId>,
    ApiJson(req): ApiJson<PinTriggerRequest>,
) -> Result<Json<TriggerView>, ApiErr> {
    let pinned_until = parse_pin_until(req.pinned, req.pinned_until.as_deref()).map_err(|e| {
        ApiErr::bad_request(format!(
            "invalid `pinned_until` (use RFC3339, e.g. 2026-07-01T00:00:00Z): {e}"
        ))
    })?;
    let row = admin::pin_trigger_policy(&s.pool, id, req.pinned, pinned_until).await?;
    Ok(Json(trigger_view(row)))
}

/// Parse the optional RFC3339 `pinned_until` for a pin request. **Only when
/// pinning** — an unpin ignores it (the [`PinTriggerRequest`] contract + the
/// admin-side normalization), so a stray/garbage value on `pinned = false` is
/// dropped rather than 400'd. Returns the parse error message on a bad value
/// while pinning.
fn parse_pin_until(
    pinned: bool,
    raw: Option<&str>,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, String> {
    if !pinned {
        return Ok(None);
    }
    raw.map(|s| chrono::DateTime::parse_from_rfc3339(s).map(|dt| dt.with_timezone(&chrono::Utc)))
        .transpose()
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pin_until_only_parses_when_pinning() {
        // Unpin ignores pinned_until — even garbage (the DTO contract): it
        // unpins rather than 400-ing.
        assert!(
            parse_pin_until(false, Some("garbage"))
                .unwrap()
                .is_none()
        );
        assert!(
            parse_pin_until(false, Some("2026-07-01T00:00:00Z"))
                .unwrap()
                .is_none()
        );
        // Pinning: no expiry, a valid expiry, a bad expiry.
        assert!(
            parse_pin_until(true, None)
                .unwrap()
                .is_none()
        );
        assert!(
            parse_pin_until(true, Some("2026-07-01T00:00:00Z"))
                .unwrap()
                .is_some()
        );
        assert!(parse_pin_until(true, Some("not-a-date")).is_err());
    }
}
