use serde::{Deserialize, Serialize};

/// `GET /api/health` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthResponse {
    /// `"ok"` when the server is serving.
    pub status: String,
}

/// `GET /api/whoami` response — the scope the presented token resolved to.
/// Lets an operator/CLI confirm "my cookie reaches the daemon and is
/// recognized, as `<scope>`" without a side effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhoamiResponse {
    /// `"admin"` / `"read"` / `"ingest"`.
    pub scope: String,
}

/// `POST /api/webhooks` response. `result` is `recorded` (new inbox row,
/// `id` set), `duplicate` (idempotent re-submit, no new row), or `ignored`
/// (event type not on the allowlist — `reason` set, not stored).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookSubmitResponse {
    pub result: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub reason: Option<String>,
}

/// `GET /api/webhooks` row — an inbox entry's queryable columns (no
/// payload body, no claim internals).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookSummary {
    pub id: i64,
    pub delivery_id: String,
    pub event_type: String,
    pub action: Option<String>,
    pub installation_id: Option<i64>,
    pub status: String,
    pub outcome: Option<String>,
    pub attempts: i32,
    pub received_at: String,
    pub processed_at: Option<String>,
}

// ─── Admin + listing DTOs (Phase 3b) ───────────────────────────────────
//
// API-shaped views, mapped deliberately from the core/DB models so internal
// columns don't leak. Timestamps are RFC3339 strings; enums are their
// snake_case wire names.

/// `allowed_installer` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallerView {
    pub account_id: i64,
    pub login: String,
    pub account_type: String,
    pub is_enabled: bool,
    pub note: Option<String>,
}

/// `supported_repo_root` row (joined to identity).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoRootView {
    pub repo_id: i64,
    pub owner: String,
    pub name: String,
    pub is_enabled: bool,
    pub note: Option<String>,
}

/// A `target_repo_policy` / `source_repo_policy` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyView {
    pub install_id: i64,
    pub repo_id: i64,
    pub is_enabled: bool,
    pub note: Option<String>,
}

/// A `trigger_policy` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerView {
    pub id: i64,
    pub install_id: i64,
    pub repo_id: i64,
    pub kind: String,
    pub match_spec: serde_json::Value,
    pub bench_args: Option<String>,
    pub is_enabled: bool,
    pub note: Option<String>,
    /// v9 (item 0025): binary-cache pin. `pinned_until` is RFC3339 when set.
    pub pinned: bool,
    pub pinned_until: Option<String>,
}

/// A `github_user` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserView {
    pub id: i64,
    pub login: String,
    pub user_type: String,
}

/// A `github_user_role` grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleView {
    pub id: i64,
    pub user_id: i64,
    pub install_id: i64,
    pub repo_id: Option<i64>,
    pub role: String,
    /// `true` once soft-revoked.
    pub revoked: bool,
}

/// A `github_installation` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallationView {
    pub id: i64,
    pub account_id: i64,
    pub account_login: String,
    pub account_type: String,
    pub suspended: bool,
    pub deleted: bool,
    pub created_at: String,
}

/// A `job` row (run visibility). v10 (0005): the job-model axes
/// (`source`/`intent`/`task_kind`/`build_target`) replace the retired
/// `kind` / `trigger_kind`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobView {
    pub id: String,
    pub install_id: i64,
    pub repo_id: i64,
    pub status: String,
    pub source: String,
    pub intent: String,
    pub task_kind: String,
    pub build_target: String,
    pub git_ref_kind: String,
    pub git_ref_display: String,
    pub commit: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnqueueBlockValidationRequest {
    /// Caller-stable key used to reconcile retries without duplicating work.
    pub idempotency_key: String,
    pub install_id: i64,
    pub repo_id: i64,
    pub commit: String,
    /// Optional operator placement constraint. Ordinary demand is unpinned.
    pub worker_id: Option<String>,
    pub selection: BlockValidationSelectionRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BlockValidationSelectionRequest {
    Recent { block_count: Option<u64> },
    Full,
    Range { start: u64, end: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnqueueJobResponse {
    pub submission_id: String,
    pub disposition: String,
    pub initial_job_ids: Vec<String>,
}

/// Authenticated, submission-scoped report snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmissionReportView {
    pub identity: ReportIdentityView,
    pub lifecycle: ReportLifecycleView,
    pub task: TaskReportView,
    pub artifacts: Vec<ReportArtifactView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportIdentityView {
    pub submission_id: String,
    pub current_job_id: Option<String>,
    pub current_attempt_id: Option<String>,
    pub task_kind: String,
    pub source: String,
    pub repository: String,
    pub commit: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportLifecycleView {
    pub state: String,
    pub phase: Option<String>,
    pub completed_jobs: u32,
    pub total_jobs: u32,
    pub failure: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum TaskReportView {
    Benchmark(BenchmarkReportDetail),
    BuildOnly(BuildOnlyReportDetail),
    BlockValidation(BlockValidationReportDetail),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkReportDetail {
    pub requested_runs: u32,
    pub completed_runs: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildOnlyReportDetail {
    pub cache_outcome: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockValidationReportDetail {
    pub requested: Option<BlockValidationSelectionDetail>,
    pub observed: Option<ObservedValidationIndexDetail>,
    pub resolved_range: Option<ReportRange>,
    pub segments: Vec<ValidationEpochSegmentDetail>,
    pub shard_count: Option<u32>,
    pub max_concurrency: Option<u32>,
    pub verdict: Option<String>,
    pub checked_blocks: Option<u64>,
    pub chainstate_origin: Option<String>,
    pub invalid_blocks: Vec<InvalidBlockDetail>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BlockValidationSelectionDetail {
    Recent { block_count: u64 },
    Full,
    Range { range: ReportRange },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservedValidationIndexDetail {
    pub pre_nakamoto_count: u64,
    pub nakamoto_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationEpochSegmentDetail {
    pub epoch: String,
    pub global_range: ReportRange,
    pub local_range: ReportRange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvalidBlockDetail {
    pub shard: u32,
    pub block: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportArtifactView {
    pub name: String,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FleetSummaryView {
    pub registered_workers: u64,
    pub online_workers: u64,
    pub draining_workers: u64,
    pub active_attempts: u64,
    pub pending_cleanup: u64,
    pub reliable_event_gap_attempts: u64,
    pub staged_artifact_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FleetWorkerView {
    pub worker_id: String,
    pub display_name: String,
    pub enabled: bool,
    pub draining: bool,
    pub capabilities: Vec<String>,
    pub measurement_profile: Option<String>,
    pub worker_session_id: Option<String>,
    pub session_status: Option<String>,
    pub software_version: Option<String>,
    pub last_heartbeat_at: Option<String>,
    pub session_expires_at: Option<String>,
    pub resource_facts: Option<serde_json::Value>,
    pub attempt_id: Option<String>,
    pub job_id: Option<String>,
    pub trace_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FleetOverview {
    pub summary: FleetSummaryView,
    pub workers: Vec<FleetWorkerView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerDrainRequest {
    pub draining: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerCreateRequest {
    pub worker_id: Option<String>,
    pub display_name: String,
    pub capabilities: Vec<String>,
    pub measurement_profile: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerUpdateRequest {
    pub display_name: Option<String>,
    pub capabilities: Option<Vec<String>>,
    pub measurement_profile: Option<Option<String>>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerIdentityRequest {
    pub public_key_pem: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerIdentityView {
    pub identity_key_sha256: String,
    pub created_at: String,
    pub revoked_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerPolicyView {
    pub worker: FleetWorkerView,
    pub identities: Vec<WorkerIdentityView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FleetCancellationResponse {
    pub job_id: String,
    pub cancel_requested: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetRecoveryRequest {
    pub reason: String,
    /// Optional compatible worker on which to place the fresh generation.
    pub worker_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FleetRecoveryResponse {
    pub prior_submission_id: String,
    pub new_submission_id: String,
    pub first_job_id: String,
    pub execution_generation: u64,
}

/// Resolution of an `owner/repo` slug to the ids the policy/role commands
/// need. `install_id` is the **active** installation on `owner`'s account
/// (GitHub Apps install at most once per account); `repo_id` is the
/// `github_repo` row for `owner/repo`. Both must already be known to the
/// daemon — the resolver is a pure lookup, not a GitHub call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolveRepoResponse {
    pub install_id: i64,
    pub account_login: String,
    pub repo_id: i64,
    pub repo_owner: String,
    pub repo_name: String,
}

// ─── Request bodies ────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllowInstallerRequest {
    pub login: String,
    #[serde(default)]
    pub note: Option<String>,
}

/// Exactly one of `login` / `account_id`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct DisableInstallerRequest {
    #[serde(default)]
    pub login: Option<String>,
    #[serde(default)]
    pub account_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllowRepoRequest {
    pub owner: String,
    pub name: String,
    #[serde(default)]
    pub note: Option<String>,
}

/// Exactly one of `owner`+`name` / `repo_id`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct DisableRepoRequest {
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub repo_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllowPolicyRequest {
    pub install_id: i64,
    pub repo_id: i64,
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DisablePolicyRequest {
    pub install_id: i64,
    pub repo_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AddTriggerRequest {
    pub install_id: i64,
    pub repo_id: i64,
    /// `branch_push` or `tag_created`.
    pub kind: String,
    /// JSON match spec validated server-side.
    pub match_spec: serde_json::Value,
    #[serde(default)]
    pub bench_args: Option<String>,
    #[serde(default)]
    pub note: Option<String>,
}

/// Set/clear the binary-cache pin on a trigger policy (v9, item 0025). When
/// `pinned`, this ref's built `stacks-bench` binary is kept past the cache LRU
/// budget. `pinned_until` is an optional RFC3339 expiry (e.g.
/// `2026-07-01T00:00:00Z`), ignored when `pinned` is false.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinTriggerRequest {
    pub pinned: bool,
    #[serde(default)]
    pub pinned_until: Option<String>,
}

/// Exactly one of `login` / `user_id`. `repo` narrows the grant; omit for
/// install-wide. `role` is `admin`, `trigger_pr_benchmark`,
/// `trigger_block_validation`, or `view_results`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleRequest {
    #[serde(default)]
    pub login: Option<String>,
    #[serde(default)]
    pub user_id: Option<i64>,
    pub install: i64,
    #[serde(default)]
    pub repo: Option<i64>,
    pub role: String,
}

/// Result of a role grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrantRoleResult {
    pub role: RoleView,
    /// `true` if a new grant was created (vs. reactivated/already-active).
    pub created: bool,
}

#[cfg(test)]
mod tests {
    use super::{
        BlockValidationSelectionRequest, EnqueueBlockValidationRequest, FleetRecoveryResponse,
    };

    #[test]
    fn fleet_recovery_response_uses_submission_identity_fields() {
        let value = serde_json::to_value(FleetRecoveryResponse {
            prior_submission_id: "prior".into(),
            new_submission_id: "new".into(),
            first_job_id: "job".into(),
            execution_generation: 2,
        })
        .unwrap();

        assert_eq!(value["prior_submission_id"], "prior");
        assert_eq!(value["new_submission_id"], "new");
        assert!(
            value
                .get("prior_group_id")
                .is_none()
        );
        assert!(
            value
                .get("new_group_id")
                .is_none()
        );
    }

    #[test]
    fn block_validation_selector_round_trips_and_rejects_retired_plan_fields() {
        let request = EnqueueBlockValidationRequest {
            idempotency_key: "operator/1".into(),
            install_id: 1,
            repo_id: 2,
            commit: "a".repeat(40),
            worker_id: None,
            selection: BlockValidationSelectionRequest::Recent { block_count: Some(500_000) },
        };
        let encoded = serde_json::to_value(&request).unwrap();
        assert_eq!(encoded["selection"]["kind"], "recent");
        assert_eq!(encoded["selection"]["block_count"], 500_000);
        assert_eq!(
            serde_json::from_value::<EnqueueBlockValidationRequest>(encoded).unwrap(),
            request
        );

        let retired = serde_json::json!({
            "idempotency_key": "operator/1",
            "install_id": 1,
            "repo_id": 2,
            "commit": "a".repeat(40),
            "worker_id": null,
            "selection": {"kind": "full"},
            "requested_shards": 8
        });
        assert!(serde_json::from_value::<EnqueueBlockValidationRequest>(retired).is_err());
    }
}
