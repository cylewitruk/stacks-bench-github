use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use uuid::Uuid;

/// Slice 8 added `Claimed` between `Queued` and `Running` per the
/// pre-Phase-2 checkpoint. Lifecycle for the new-schema `job` table:
/// `queued → claimed → running → completed/failed/cancelled`. The
/// daemon's claim path transitions queued→claimed (with a
/// claim_token + claimed_at), then claimed→running when execution
/// actually starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "job_status", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Claimed,
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// Slice 8 (post-review fix): typed terminal-only status for
/// `JobStore::mark_terminal`. Without this narrowing, the API
/// accepted any `JobStatus` — a caller bug could transition
/// `running → queued` while preserving `claim_token`/`claimed_at`,
/// directly violating the queued-state invariant from the
/// pre-Phase-2 checkpoint. Restricting the type to the three
/// genuinely-terminal values makes the bug impossible at compile
/// time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalJobStatus {
    Completed,
    Failed,
    Cancelled,
}

impl From<TerminalJobStatus> for JobStatus {
    fn from(t: TerminalJobStatus) -> Self {
        match t {
            TerminalJobStatus::Completed => JobStatus::Completed,
            TerminalJobStatus::Failed => JobStatus::Failed,
            TerminalJobStatus::Cancelled => JobStatus::Cancelled,
        }
    }
}

/// Lifecycle dimension for `github_webhook` rows. Mirrors the
/// `github_webhook_status` DB enum. Distinct from `WebhookOutcome`:
/// status is the queue/processing state; outcome is the specific
/// terminal decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "github_webhook_status", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum WebhookStatus {
    Received,
    Processing,
    Processed,
    Ignored,
    Denied,
    RetryableError,
    Failed,
}

/// Specific processor decision attached to a terminal webhook row.
/// Mirrors the `github_webhook_outcome` DB enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "github_webhook_outcome", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum WebhookOutcome {
    EnqueuedJob,
    /// Pre-slice-6 design checkpoint: Phase 1 shadow accept. The new
    /// pipeline's policy/trigger evaluation said "this would enqueue a
    /// job", but no `job` row was created and the legacy handler→jobs
    /// path actually ran the bench. Slice 9 retired this outcome for
    /// new rows: the three job-creating paths (`IssueCommentHandler`
    /// /benchmark, `PushHandler` branch_push, `CreateHandler`
    /// tag_created) now emit `EnqueuedJob`, and the `PullRequestHandler`
    /// accept path emits `ProcessedPullRequest` (PR events don't
    /// enqueue — no `trigger_kind` for them). The variant is retained
    /// for historical rows written before slice 9. Same status bucket
    /// as `EnqueuedJob` / `ProcessedInstallation` (= `Processed`).
    WouldEnqueueJob,
    /// Slice 3+: terminal "we materialised installation state" — the
    /// processor created/updated a `github_installation` row in response
    /// to an `installation.*` event. Distinct from `IgnoredAction` so
    /// ops queries can separate "no-op event" from "install state changed".
    ProcessedInstallation,
    /// Slice 9: terminal "we materialised/updated PR state but did NOT
    /// enqueue a job". A `pull_request.{opened,reopened,synchronize}`
    /// event with accepted policies upserts the `github_pull_request`
    /// row (so a later `/benchmark` comment can link to it) but does
    /// NOT create a job — there is no `trigger_kind` for PR-event
    /// auto-benchmarking, and auto-benching every PR push is a separate
    /// product decision. Distinct from `IgnoredAction` so ops queries
    /// can separate "PR state changed" from a true no-op, and distinct
    /// from `WouldEnqueueJob` because we have decided this path does NOT
    /// enqueue (vs. the slice-5 shadow-accept that implied it would).
    ProcessedPullRequest,
    IgnoredAction,
    IgnoredNoCommand,
    IgnoredUnknownInstallation,
    IgnoredUnsupportedLineage,
    DeniedInstallAllowlist,
    DeniedTargetPolicy,
    DeniedSourcePolicy,
    DeniedUnauthorized,
    Error,
}

impl WebhookOutcome {
    /// Terminal status that pairs with this outcome.
    pub fn terminal_status(self) -> WebhookStatus {
        match self {
            Self::EnqueuedJob
            | Self::WouldEnqueueJob
            | Self::ProcessedInstallation
            | Self::ProcessedPullRequest => WebhookStatus::Processed,
            Self::IgnoredAction
            | Self::IgnoredNoCommand
            | Self::IgnoredUnknownInstallation
            | Self::IgnoredUnsupportedLineage => WebhookStatus::Ignored,
            Self::DeniedInstallAllowlist
            | Self::DeniedTargetPolicy
            | Self::DeniedSourcePolicy
            | Self::DeniedUnauthorized => WebhookStatus::Denied,
            Self::Error => WebhookStatus::Failed,
        }
    }
}

/// GH account kind, mirrors the `github_account_type` DB enum. Bot is
/// included for completeness even though the App is unlikely to be
/// installed by a bot in practice — present so we don't panic if the
/// API ever hands us one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "github_account_type", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum GithubAccountType {
    User,
    Organization,
    Bot,
}

/// Operator-curated allowlist row. PK is the GitHub-assigned numeric
/// account id — stable across renames and case.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct AllowedInstaller {
    pub github_account_id: i64,
    pub account_login: String,
    pub account_type: GithubAccountType,
    pub is_enabled: bool,
    pub note: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A GitHub App installation we've accepted. `id` is GitHub's numeric
/// installation id, used as the FK target by membership / policy /
/// inbox tables. FK to `allowed_installer.github_account_id` enforces
/// "installation must be backed by a current allowlist entry".
///
/// `deleted_at` is the slice 4 soft-delete marker for `installation.deleted`
/// events — the row stays around (so historical job/membership FKs
/// remain valid) but `deleted_at IS NOT NULL` means it's retired. The
/// "currently active" predicate is `deleted_at IS NULL AND suspended_at IS
/// NULL`.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct GithubInstallation {
    pub id: i64,
    pub github_account_id: i64,
    pub account_login: String,
    pub account_type: GithubAccountType,
    pub suspended_at: Option<DateTime<Utc>>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// GitHub repository identity + fork lineage. `id` is GitHub's numeric
/// repo id (stable across renames and transfers).
///
/// Lineage columns are populated from `/repos/{owner}/{repo}` on first
/// encounter. Convention (no SQL constraint enforces it):
///   - canonical/non-fork: `is_fork=false`, parent + fork_root NULL
///   - fork (any depth):   `is_fork=true`,  fork_root IS NOT NULL
///
/// `parent_github_repo_id` is the IMMEDIATE parent;
/// `fork_root_github_repo_id` is the ultimate non-fork ancestor
/// (GitHub's `source` in the REST response). Both are nullable
/// because slices 4-6 may insert a repo for identity-only purposes
/// (PR target/source) before the lineage walk has run.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct GithubRepo {
    pub id: i64,
    pub owner: String,
    pub name: String,
    pub default_branch: Option<String>,
    pub is_fork: Option<bool>,
    pub parent_github_repo_id: Option<i64>,
    pub fork_root_github_repo_id: Option<i64>,
    pub lineage_checked_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Operator-curated canonical-root row. A repo is in-scope iff its id OR
/// its `fork_root_github_repo_id` matches an enabled row here.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct SupportedRepoRoot {
    pub github_repo_id: i64,
    pub is_enabled: bool,
    pub note: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Per-installation repo membership. `revoked_at IS NULL` means active.
/// Composite PK doubles as the FK anchor for slice 5+ policy + slice 8+
/// job tables that need to prove the (install, repo) pair was ever known.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct GithubInstallationRepo {
    pub github_installation_id: i64,
    pub github_repo_id: i64,
    pub granted_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

/// Slice 5: triggers the processor watches for in `branch_push` /
/// `tag_created` event handling. Each `trigger_policy` row carries one
/// of these in `match_spec`. App-layer validated (the DB stores it as
/// arbitrary JSONB; this enum is the contract).
///
/// `branch_name` is an EXACT match against the inbound ref; `tag_pattern`
/// is a Rust regex pattern (matched with `regex::Regex::is_match` at
/// evaluation time). Globs would be more operator-friendly but regex
/// composes better with the eventual job-rerun-on-historical-tag flows
/// slice 9+ may introduce.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TriggerMatchSpec {
    BranchPush { branch_name: String },
    TagCreated { tag_pattern: String },
}

/// `trigger_kind` enum mirror. Slice 5 only USES `BranchPush` and
/// `TagCreated`; `PrComment` is the implicit /benchmark path
/// (no trigger_policy row needed). `Scheduled` and `Manual` are
/// reserved for post-slice-9 work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "trigger_kind", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum TriggerKind {
    PrComment,
    BranchPush,
    TagCreated,
    Scheduled,
    Manual,
}

/// Per-installation target-repo opt-in (the operator says "this install
/// will benchmark PRs against this repo"). Composite PK + FK to
/// `github_installation_repo` enforces "the (install, repo) pair must
/// exist as a membership row" — but currently-active access is a
/// separate app-level join on `revoked_at IS NULL`.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct TargetRepoPolicy {
    pub github_installation_id: i64,
    pub github_repo_id: i64,
    pub is_enabled: bool,
    pub note: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Per-installation source-repo trust (the operator says "this install
/// trusts this repo as the source side of a PR — its code may execute
/// in our benchmark VM"). Unlike `TargetRepoPolicy`, no membership FK
/// — sources can be arbitrary forks the install doesn't own.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct SourceRepoPolicy {
    pub github_installation_id: i64,
    pub github_repo_id: i64,
    pub is_enabled: bool,
    pub note: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Per-installation auto-trigger subscription. Multiple rows per
/// (install, repo) — one per `trigger_kind` + `match_spec` combo.
/// `bench_args` (optional) is forwarded to the eventual job as
/// CLI args once slice 9 starts creating jobs.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct TriggerPolicy {
    pub id: i64,
    pub github_installation_id: i64,
    pub github_repo_id: i64,
    pub trigger_kind: TriggerKind,
    /// Stored as JSONB; deserialise into `TriggerMatchSpec` for typed
    /// matching at evaluation time.
    pub match_spec: sqlx::types::Json<serde_json::Value>,
    pub bench_args: Option<String>,
    pub is_enabled: bool,
    pub note: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Slice 6: the three role values an operator can grant. Mirrors the
/// `user_role` DB enum from slice 0. Phase 1 only acts on
/// `TriggerPrBenchmark` (the `/benchmark` authz gate); `Admin` and
/// `ViewResults` are present for forward-compat but unused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "user_role", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum UserRole {
    Admin,
    TriggerPrBenchmark,
    ViewResults,
}

/// Lazily upserted identity row for a GH user we've encountered.
/// `login` is display-only; the natural key is the numeric `id`.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct GithubUser {
    pub id: i64,
    pub login: String,
    pub user_type: GithubAccountType,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Operator-curated grant: this user has this role on this installation,
/// optionally narrowed to a specific repo within the installation
/// (`github_repo_id IS NULL` = install-wide). `revoked_at IS NULL` = active.
///
/// Soft-revoke (post-slice-6 review fix): a revoke sets `revoked_at`
/// rather than deleting the row, and a subsequent re-grant clears
/// `revoked_at` on the existing row — preserving the original
/// `granted_at` audit timestamp across the cycle.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct GithubUserRole {
    pub id: i64,
    pub github_user_id: i64,
    pub github_installation_id: i64,
    pub github_repo_id: Option<i64>,
    pub granted_role: UserRole,
    pub granted_at: DateTime<Utc>,
    pub granted_by_github_user_id: Option<i64>,
    pub revoked_at: Option<DateTime<Utc>>,
}

/// Slice 7: PR subject row. Materialised on the first
/// `pull_request.opened` (or first `/benchmark` comment referencing
/// the PR if the opened event predates the new pipeline). Soft-close
/// via `closed_at`; closed PRs stay in the table so slice 8+ job FKs
/// remain valid.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct GithubPullRequest {
    pub id: i64,
    pub target_github_repo_id: i64,
    pub source_github_repo_id: i64,
    pub pr_number: i32,
    pub title: String,
    pub author_github_user_id: i64,
    pub closed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// ─── Job tables ───────────────────────────────────────────────────────

/// Row shape of the `job` table. `claim_token` + `claimed_at` invariants
/// (app-layer enforced):
///
///   - `status = Queued` ⇔ both NULL
///   - `status = Claimed` ⇔ both Some
///   - `status IN (Running, Completed, Failed, Cancelled)` → both PRESERVED as
///     audit
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Job {
    pub id: Uuid,
    pub github_installation_id: i64,
    pub github_repo_id: i64,
    pub status: JobStatus,
    pub job_kind: JobKind,
    pub trigger_kind: TriggerKind,
    pub git_ref_kind: GitRefKind,
    pub git_ref_display: String,
    pub git_commit_hash: Option<String>,
    pub git_committed_at: Option<DateTime<Utc>>,
    pub claim_token: Option<Uuid>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Slice 8 insert payload for the new-schema `job` table. Slice 9
/// uses this from the processor's job-creation path; slice 8 only
/// exercises it via integration tests.
#[derive(Debug, Clone)]
pub struct NewJob {
    pub github_installation_id: i64,
    pub github_repo_id: i64,
    pub job_kind: JobKind,
    pub trigger_kind: TriggerKind,
    pub git_ref_kind: GitRefKind,
    pub git_ref_display: String,
    /// Slice 9 leaves this `None` for triggers that enqueue with an
    /// unresolved commit (e.g. branch tip at queue time); the
    /// daemon's claim phase resolves and passes the resolved
    /// values to `mark_running` via `ResolvedCommit`.
    pub git_commit_hash: Option<String>,
    pub git_committed_at: Option<DateTime<Utc>>,
}

/// Slice 8 (post-review fix): typed handoff for the
/// resolve-commit-during-claim path. The daemon's claim phase
/// resolves a branch tip to its concrete commit; `mark_running`
/// writes the present fields atomically with the status transition
/// while holding the claim_token guard.
///
/// Slice 10 (review fix): `committed_at` is `Option` because some
/// resolution paths recover only the SHA (e.g. resolving a PR head via
/// the pulls API yields the commit id but not its authored date). A
/// `None` here leaves `job.git_committed_at` untouched (`mark_running`
/// `COALESCE`s) — far better than fabricating the claim time, which
/// would corrupt baseline-timeline ordering.
#[derive(Debug, Clone)]
pub struct ResolvedCommit {
    pub hash: String,
    pub committed_at: Option<DateTime<Utc>>,
}

/// Slice 8 (post-review fix): atomic-creation request for the
/// `JobStore::create_job_with_links` boundary. Captures everything
/// the slice 9 processor needs to insert in one transaction: the
/// `job` row, the webhook→job ingest link, the optional owner link
/// (absent for branch_push / tag_created / scheduled), the optional
/// PR association, and the queued `job_event` (provenance JSONB
/// keyed off `trigger_kind`).
#[derive(Debug, Clone)]
pub struct JobCreationRequest {
    pub new_job: NewJob,
    pub github_webhook_id: i64,
    /// Owner of the job; populated for pr_comment / manual triggers,
    /// `None` for triggers with no responsible user.
    pub triggering_user_id: Option<i64>,
    /// PR association (target = `new_job.github_repo_id`).
    pub pull_request_link: Option<NewPullRequestLink>,
    /// Provenance JSONB for the queued event. Shape is a tagged Rust
    /// enum keyed off `new_job.trigger_kind` (slice 9 defines).
    pub queued_event_detail: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct NewPullRequestLink {
    pub github_pull_request_id: i64,
    /// `comment.id` for pr_comment trigger; None for others.
    pub triggering_comment_id: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "job_kind", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    AdHoc,
    Baseline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "git_ref_kind", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum GitRefKind {
    Branch,
    Tag,
    Commit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "job_event_kind", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum JobEventKind {
    Queued,
    Claimed,
    ProvisionStarted,
    ProvisionFinished,
    PhaseBuildStarted,
    PhaseBuildFinished,
    PhaseBenchStarted,
    PhaseBenchFinished,
    TeardownStarted,
    TeardownFinished,
    CommentPosted,
    CommentUpdated,
    /// A GitHub Check Run was created (roadmap-v4). Carries
    /// `github_check_run_id` + `github_check_run_url`, read back on re-claim
    /// so the runner updates the existing check instead of duplicating it.
    CheckRunCreated,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "job_event_status", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum JobEventStatus {
    Started,
    InProgress,
    Success,
    Fail,
}

/// Append-only job timeline row. `detail` JSONB shape is a tagged
/// Rust enum keyed off the parent job's `trigger_kind` (slice 9
/// defines the queued-event provenance shape; later phases extend
/// for the other event kinds).
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct JobEvent {
    pub id: i64,
    pub job_id: Uuid,
    pub event_kind: JobEventKind,
    pub event_status: JobEventStatus,
    pub occurred_at: DateTime<Utc>,
    pub github_comment_id: Option<i64>,
    pub github_check_run_id: Option<i64>,
    pub github_check_run_url: Option<String>,
    pub remark: Option<String>,
    pub detail: Option<Json<serde_json::Value>>,
}

/// Slice 9: typed provenance for the `queued` job_event's `detail`
/// JSONB. Tagged by `trigger` (mirrors `job.trigger_kind`) so the
/// audit/inspection reader knows which envelope caused the enqueue
/// without inspecting `job` columns. Per the SUBJECT-vs-PROVENANCE
/// design principle, this is provenance — `bench_args` lives here
/// (and is what slice 10's daemon reads to assemble the run's
/// CLI args) because the `job` table has no dedicated column for it.
///
/// Only the three slice-9 job-creating triggers have variants;
/// `scheduled` / `manual` arrive in later work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "trigger", rename_all = "snake_case")]
pub enum QueuedEventDetail {
    /// `/benchmark` comment on a PR. `bench_args` is the parsed
    /// command tail (subcommand + args), forwarded to the eventual run.
    PrComment {
        sender_id: i64,
        sender_login: String,
        comment_id: i64,
        pr_number: i64,
        subcommand: Option<String>,
        bench_args: Vec<String>,
    },
    /// Watched branch advanced. `trigger_id` is the matched
    /// `trigger_policy.id`; `bench_args` its configured args.
    BranchPush { branch: String, trigger_id: i64, bench_args: Option<String> },
    /// Watched tag pattern appeared. The tag's commit is resolved by
    /// the daemon during the claim phase (the create event
    /// carries no SHA), so no commit fields here.
    TagCreated { tag: String, trigger_id: i64, bench_args: Option<String> },
}

/// Slice 8 insert payload for `job_event`. occurred_at defaults to
/// NOW() so it's not part of the payload.
#[derive(Debug, Clone)]
pub struct NewJobEvent {
    pub job_id: Uuid,
    pub event_kind: JobEventKind,
    pub event_status: JobEventStatus,
    pub github_comment_id: Option<i64>,
    pub github_check_run_id: Option<i64>,
    pub github_check_run_url: Option<String>,
    pub remark: Option<String>,
    pub detail: Option<serde_json::Value>,
}

/// Write-once promoted bench metrics. Adding/removing columns
/// requires a coordinated change with stacks-bench.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct JobMetric {
    pub job_id: Uuid,
    pub envelope_duration_us: i64,
    pub replay_duration_us: i64,
    pub total_duration_us: i64,
    pub setup_duration_us: i64,
    pub execution_duration_us: i64,
    pub commit_duration_us: i64,
    pub clarity_runtime: i64,
    pub transactions: i64,
    pub read_length: i64,
    pub write_length: i64,
    pub measured_blocks: i64,
    pub warmup_blocks: i64,
    pub created_at: DateTime<Utc>,
}

/// Raw run.json + archive path. `run_json` is None for jobs that
/// failed before producing it; `archive_dir` is required so ops
/// can find post-mortem artefacts even on failure.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct JobResult {
    pub job_id: Uuid,
    pub run_json: Option<Json<serde_json::Value>>,
    pub archive_dir: String,
    pub created_at: DateTime<Utc>,
}

/// Slice 8 subject relation: this job is about a PR.
/// `triggering_comment_id` is the slice 7 `comment.id` for
/// pr_comment-triggered jobs; None for other PR jobs (e.g. a
/// future "PR opened auto-bench" feature).
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct GithubPullRequestJob {
    pub job_id: Uuid,
    pub github_pull_request_id: i64,
    pub triggering_comment_id: Option<i64>,
    pub created_at: DateTime<Utc>,
}

/// Slice 8 ingest link: webhook → job. The slice-9 processor inserts
/// the webhook row earlier (via the handler's ingest path), then
/// creates the job + this link inside one transaction.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct GithubWebhookJob {
    pub github_webhook_id: i64,
    pub job_id: Uuid,
    pub created_at: DateTime<Utc>,
}

/// Slice 8 ownership relation: which user triggered the job.
/// Populated for pr_comment / manual triggers; absent for
/// branch_push / tag_created / scheduled triggers (no responsible
/// user).
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct GithubUserJob {
    pub github_user_id: i64,
    pub job_id: Uuid,
    pub created_at: DateTime<Utc>,
}
