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
/// install-wide. `role` is `admin` / `trigger_pr_benchmark` / `view_results`.
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
