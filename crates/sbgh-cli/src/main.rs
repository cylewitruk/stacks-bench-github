//! Operator CLI: schema migrations + role grants + allowed_installer admin.
//!
//! Subcommands:
//!   - `migrate` (default) — apply schema migrations + idempotently (re)apply
//!     the narrow Postgres role grants. Run as `service_completed` before
//!     handler/orchestrator boot. Idempotent on re-run.
//!   - `installer` — manage the operator-curated `allowed_installer` allowlist
//!     that gates which GitHub accounts may install the App.
//!
//! Always runs as the DB **owner** (the `sbgh` account). The handler and
//! orchestrator never run with owner credentials — they each connect as their
//! own narrow role:
//!
//! - `sbgh_handler` → INSERT on a small set of jobs/inbox columns + SELECT on
//!   the columns required for `INSERT ... ON CONFLICT ... RETURNING`. No read
//!   access to head_sha / args / payload / status, no way to fabricate a
//!   completed/processed row.
//! - `sbgh_orch` → SELECT + UPDATE on the full `jobs` and `github_webhook`
//!   tables; SELECT + INSERT/UPDATE on the identity/policy tables it owns.
//!
//! Required env vars (every subcommand):
//!   - `DATABASE_URL` — owner DSN, e.g.
//!     `postgres://sbgh:OWNER_PW@postgres/sbgh`
//!
//! Required env vars (`migrate` only):
//!   - `SBGH_HANDLER_DB_PASSWORD` — password assigned to role `sbgh_handler`
//!   - `SBGH_ORCH_DB_PASSWORD`    — password assigned to role `sbgh_orch`
//!
//! Optional env vars (`installer {allow,disable}` only):
//!   - `SBGH_GH_API_BASE_URL` — defaults to `https://api.github.com`. Override
//!     for GitHub Enterprise or tests.
//!
//! The `installer` subcommand resolves logins via GitHub's unauthenticated
//! `/users/{login}` endpoint (60/hr per IP — plenty for operator one-shots).
//! No App credentials needed.

use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow};
use clap::{Parser, Subcommand};
use sbgh_cli::{
    add_trigger_policy, allow_installer, allow_repo_root, allow_source_policy, allow_target_policy,
    apply_roles, disable_installer, disable_installer_by_account_id, disable_repo_root,
    disable_repo_root_by_id, disable_source_policy, disable_target_policy, disable_trigger_policy,
    grant_role, grant_role_by_user_id, list_installers, list_repo_roots, list_roles,
    list_source_policies, list_target_policies, list_trigger_policies, list_users, revoke_role,
    revoke_role_by_user_id,
};
use sbgh_core::db;
use sbgh_core::models::{TriggerKind, UserRole};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Parser, Debug)]
#[command(version, about = "sbgh operator CLI")]
struct Cli {
    /// Optional env file to source before reading required vars. Useful when
    /// running outside docker. Inside the container, env is set by compose.
    #[arg(long, value_name = "PATH", global = true)]
    env_file: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Apply schema migrations + (re)apply role grants. This is the default
    /// when no subcommand is given (matches the legacy `sbgh-migrate` binary).
    Migrate,

    /// Manage the operator-curated allowlist of GitHub accounts permitted to
    /// install the App.
    Installer {
        #[command(subcommand)]
        action: InstallerAction,
    },

    /// Manage the operator-curated `supported_repo_root` list — the set of
    /// canonical repos this software knows how to benchmark. Forks of
    /// these roots are accepted automatically; everything else is denied
    /// with `ignored_unsupported_lineage` at processing time.
    Repo {
        #[command(subcommand)]
        action: RepoAction,
    },

    /// Manage the slice 5 per-installation policy tables that gate
    /// which PR / push / tag events will trigger a benchmark job.
    /// Three nested groups: `target` (which repos are benchmark
    /// targets), `source` (which repos are trusted as PR sources),
    /// `trigger` (which branches / tag patterns auto-trigger jobs).
    Policy {
        #[command(subcommand)]
        action: PolicyAction,
    },

    /// Slice 6: manage per-installation role grants. `grant` /
    /// `revoke` / `list` mirror the installer/repo/policy shape.
    /// Each grant is `(user, install, optional repo, role)`; `--repo`
    /// narrows to a single repo within the install, omitting it
    /// grants install-wide.
    User {
        #[command(subcommand)]
        action: UserAction,
    },
}

#[derive(Subcommand, Debug)]
enum UserAction {
    /// Grant a role to a user on an installation. Resolves login → id
    /// via the GitHub `/users/{login}` endpoint (same path
    /// `installer allow` uses) and upserts the `github_user` row
    /// before recording the grant.
    ///
    /// `--repo` narrows the grant to one repo within the install;
    /// omit it to grant install-wide. Exactly one of `--login` or
    /// `--user-id` (emergency / GH-outage path) must be supplied.
    Grant {
        #[arg(long, group = "user_grant_target")]
        login: Option<String>,
        #[arg(long, group = "user_grant_target")]
        user_id: Option<i64>,
        #[arg(long)]
        install: i64,
        #[arg(long)]
        repo: Option<i64>,
        #[arg(long, value_enum)]
        role: RoleArg,
    },
    /// Revoke a previously granted role. Match criteria are
    /// (user, install, repo, role) — `--repo` MUST exactly match
    /// the grant being revoked (NULL grants are NOT matched by a
    /// repo-narrowed revoke).
    Revoke {
        #[arg(long, group = "user_revoke_target")]
        login: Option<String>,
        #[arg(long, group = "user_revoke_target")]
        user_id: Option<i64>,
        #[arg(long)]
        install: i64,
        #[arg(long)]
        repo: Option<i64>,
        #[arg(long, value_enum)]
        role: RoleArg,
    },
    /// List grants, optionally filtered by install id. With
    /// `--users` instead, lists every known `github_user` row
    /// (independent of role grants).
    List {
        #[arg(long, conflicts_with = "users")]
        install: Option<i64>,
        #[arg(long)]
        users: bool,
    },
}

/// Clap-facing copy of `sbgh_core::models::UserRole` so the CLI can
/// surface `--help`-friendly choices without making sbgh-core depend
/// on clap. Three variants total; the conversion is trivial.
#[derive(clap::ValueEnum, Clone, Copy, Debug)]
enum RoleArg {
    Admin,
    TriggerPrBenchmark,
    ViewResults,
}

impl From<RoleArg> for UserRole {
    fn from(r: RoleArg) -> Self {
        match r {
            RoleArg::Admin => UserRole::Admin,
            RoleArg::TriggerPrBenchmark => UserRole::TriggerPrBenchmark,
            RoleArg::ViewResults => UserRole::ViewResults,
        }
    }
}

#[derive(Subcommand, Debug)]
enum InstallerAction {
    /// Add (or re-enable) a GitHub account on the allowlist. Resolves the
    /// login → numeric account id via GitHub's unauthenticated
    /// `/users/{login}` endpoint, then upserts the row.
    Allow {
        #[arg(long)]
        login: String,
        #[arg(long)]
        note: Option<String>,
    },
    /// Soft-disable an installer (sets `is_enabled=FALSE`). The row is
    /// kept for audit; re-enable with `allow --login ...`.
    ///
    /// Exactly one of `--login` or `--account-id` must be supplied.
    /// `--login` resolves through GitHub (handles rename/recycling
    /// correctly); `--account-id` skips the API entirely and is the
    /// emergency path when GitHub is unreachable or rate-limited, or
    /// when the operator pulled the id from `installer list`.
    Disable {
        #[arg(long, group = "disable_target")]
        login: Option<String>,
        #[arg(long, group = "disable_target")]
        account_id: Option<i64>,
    },
    /// List every row in `allowed_installer`.
    List,
}

#[derive(Subcommand, Debug)]
enum RepoAction {
    /// Add (or re-enable) a canonical repo on the supported list.
    /// Resolves owner/name → numeric id via GitHub's unauthenticated
    /// `/repos/{owner}/{repo}` endpoint, then upserts both the
    /// identity row (github_repo) and the operator row
    /// (supported_repo_root) in one transaction.
    Allow {
        #[arg(long)]
        owner: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        note: Option<String>,
    },
    /// Soft-disable a supported root (sets is_enabled=FALSE; row kept
    /// for audit; forks of this root will start being denied with
    /// `ignored_unsupported_lineage`).
    ///
    /// Exactly one of `--owner --name` (resolves via GH API) or
    /// `--repo-id` (emergency / GH-outage path) must be supplied.
    Disable {
        #[arg(long, group = "repo_disable_target", requires = "name")]
        owner: Option<String>,
        #[arg(long, group = "repo_disable_target", requires = "owner")]
        name: Option<String>,
        #[arg(long, group = "repo_disable_target")]
        repo_id: Option<i64>,
    },
    /// List every row in `supported_repo_root` joined to its identity.
    List,
}

#[derive(Subcommand, Debug)]
enum PolicyAction {
    /// Per-installation target-repo opt-in: "this install will
    /// benchmark PRs against this repo." Requires a current
    /// `github_installation_repo` membership row (FK).
    Target {
        #[command(subcommand)]
        action: PolicyTargetAction,
    },
    /// Per-installation source-repo trust: "this install trusts this
    /// repo as a PR source — its code may execute in our bench VM."
    /// Unlike target, no membership FK — sources can be arbitrary
    /// forks.
    Source {
        #[command(subcommand)]
        action: PolicySourceAction,
    },
    /// Per-installation auto-trigger subscriptions for `push` /
    /// `create` events. Multiple per (install, repo) — each row is one
    /// trigger_kind + match_spec + bench_args.
    Trigger {
        #[command(subcommand)]
        action: PolicyTriggerAction,
    },
}

#[derive(Subcommand, Debug)]
enum PolicyTargetAction {
    /// Allow (or re-enable) a (install, repo) pair as a benchmark
    /// target. Operator pulls the ids from `installer list` +
    /// `repo list` first.
    Allow {
        #[arg(long)]
        install_id: i64,
        #[arg(long)]
        repo_id: i64,
        #[arg(long)]
        note: Option<String>,
    },
    /// Soft-disable a target policy row.
    Disable {
        #[arg(long)]
        install_id: i64,
        #[arg(long)]
        repo_id: i64,
    },
    /// List target policies. Optional `--install-id` filter.
    List {
        #[arg(long)]
        install_id: Option<i64>,
    },
}

#[derive(Subcommand, Debug)]
enum PolicySourceAction {
    Allow {
        #[arg(long)]
        install_id: i64,
        #[arg(long)]
        repo_id: i64,
        #[arg(long)]
        note: Option<String>,
    },
    Disable {
        #[arg(long)]
        install_id: i64,
        #[arg(long)]
        repo_id: i64,
    },
    List {
        #[arg(long)]
        install_id: Option<i64>,
    },
}

#[derive(Subcommand, Debug)]
enum PolicyTriggerAction {
    /// Add a new trigger_policy row. `--kind` is `branch_push` or
    /// `tag_created`; `--match` is the JSON match_spec (e.g.
    /// `'{"kind":"branch_push","branch_name":"develop"}'`); `--args`
    /// is forwarded to the eventual job as CLI args in slice 9+.
    Add {
        #[arg(long)]
        install_id: i64,
        #[arg(long)]
        repo_id: i64,
        /// `branch_push` or `tag_created` (other trigger_kind variants
        /// are reserved for post-slice-9).
        #[arg(long)]
        kind: String,
        /// JSON match_spec validated against `TriggerMatchSpec` at
        /// insert time.
        #[arg(long = "match")]
        match_spec: String,
        #[arg(long = "args")]
        bench_args: Option<String>,
        #[arg(long)]
        note: Option<String>,
    },
    /// Soft-disable a trigger row by its bigserial id.
    Disable {
        #[arg(long)]
        id: i64,
    },
    /// List triggers. Optional filters; both None = list everything.
    List {
        #[arg(long)]
        install_id: Option<i64>,
        #[arg(long)]
        repo_id: Option<i64>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    load_env(cli.env_file.as_deref())?;
    init_tracing();

    match cli
        .command
        .unwrap_or(Command::Migrate)
    {
        Command::Migrate => run_migrate().await,
        Command::Installer { action } => run_installer(action).await,
        Command::Repo { action } => run_repo(action).await,
        Command::Policy { action } => run_policy(action).await,
        Command::User { action } => run_user(action).await,
    }
}

async fn run_migrate() -> anyhow::Result<()> {
    let database_url =
        std::env::var("DATABASE_URL").context("DATABASE_URL must be set (owner DSN)")?;
    let handler_pw = std::env::var("SBGH_HANDLER_DB_PASSWORD")
        .context("SBGH_HANDLER_DB_PASSWORD must be set")?;
    let orch_pw =
        std::env::var("SBGH_ORCH_DB_PASSWORD").context("SBGH_ORCH_DB_PASSWORD must be set")?;

    tracing::info!("connecting to postgres as owner");
    let pool = db::connect(&database_url)
        .await
        .context("connect to postgres")?;

    tracing::info!("applying schema migrations");
    db::migrate(&pool)
        .await
        .context("schema migrations")?;

    tracing::info!("(re)applying role definitions + grants");
    apply_roles(&pool, &handler_pw, &orch_pw)
        .await
        .context("role/grant setup")?;

    tracing::info!("migrate complete");
    Ok(())
}

async fn run_installer(action: InstallerAction) -> anyhow::Result<()> {
    let database_url =
        std::env::var("DATABASE_URL").context("DATABASE_URL must be set (owner DSN)")?;
    let pool = db::connect(&database_url)
        .await
        .context("connect to postgres")?;

    let api_base =
        std::env::var("SBGH_GH_API_BASE_URL").unwrap_or_else(|_| "https://api.github.com".into());

    match action {
        InstallerAction::Allow { login, note } => {
            let row = allow_installer(&pool, &api_base, &login, note.as_deref())
                .await
                .context("allow installer")?;
            println!(
                "allowed: github_account_id={} login={} type={:?} is_enabled={}",
                row.github_account_id, row.account_login, row.account_type, row.is_enabled,
            );
        }
        InstallerAction::Disable { login, account_id } => {
            // clap's `group` attribute enforces mutual exclusivity; the
            // required-exactly-one constraint is checked here so we can
            // give an operator-friendly error instead of a clap-derived
            // one. `disable_target` matches the group name on the args.
            let row = match (login, account_id) {
                (Some(l), None) => disable_installer(&pool, &api_base, &l)
                    .await
                    .context("disable installer")?,
                (None, Some(id)) => disable_installer_by_account_id(&pool, id)
                    .await
                    .context("disable installer")?,
                (None, None) => {
                    return Err(anyhow!("exactly one of --login or --account-id is required"));
                }
                (Some(_), Some(_)) => {
                    // Unreachable in practice — clap's group enforces
                    // mutex — but keep the arm so the match is total.
                    return Err(anyhow!("--login and --account-id are mutually exclusive"));
                }
            };
            println!(
                "disabled: github_account_id={} login={} is_enabled={}",
                row.github_account_id, row.account_login, row.is_enabled,
            );
        }
        InstallerAction::List => {
            let rows = list_installers(&pool)
                .await
                .context("list installers")?;
            if rows.is_empty() {
                println!("(no installers in allowlist)");
                return Ok(());
            }
            for r in rows {
                println!(
                    "{:<12} {:>12}  {:<8}  {}  note={}",
                    r.account_login,
                    r.github_account_id,
                    format!("{:?}", r.account_type),
                    if r.is_enabled { "ENABLED " } else { "disabled" },
                    r.note
                        .as_deref()
                        .unwrap_or("-"),
                );
            }
        }
    }
    Ok(())
}

async fn run_repo(action: RepoAction) -> anyhow::Result<()> {
    let database_url =
        std::env::var("DATABASE_URL").context("DATABASE_URL must be set (owner DSN)")?;
    let pool = db::connect(&database_url)
        .await
        .context("connect to postgres")?;
    let api_base =
        std::env::var("SBGH_GH_API_BASE_URL").unwrap_or_else(|_| "https://api.github.com".into());

    match action {
        RepoAction::Allow { owner, name, note } => {
            let row = allow_repo_root(&pool, &api_base, &owner, &name, note.as_deref())
                .await
                .context("allow repo root")?;
            println!(
                "allowed: github_repo_id={} {}/{} is_enabled={}",
                row.github_repo_id, row.owner, row.name, row.is_enabled,
            );
        }
        RepoAction::Disable { owner, name, repo_id } => {
            let row = match (owner, name, repo_id) {
                (Some(o), Some(n), None) => disable_repo_root(&pool, &api_base, &o, &n)
                    .await
                    .context("disable repo root")?,
                (None, None, Some(id)) => disable_repo_root_by_id(&pool, id)
                    .await
                    .context("disable repo root")?,
                (None, None, None) => {
                    return Err(anyhow!("exactly one of --owner+--name or --repo-id is required"));
                }
                _ => {
                    // Unreachable in practice — clap's group + `requires`
                    // enforce mutex + together-ness — but keep the arm
                    // so the match is total.
                    return Err(anyhow!("--owner+--name and --repo-id are mutually exclusive"));
                }
            };
            println!(
                "disabled: github_repo_id={} {}/{} is_enabled={}",
                row.github_repo_id, row.owner, row.name, row.is_enabled,
            );
        }
        RepoAction::List => {
            let rows = list_repo_roots(&pool)
                .await
                .context("list repo roots")?;
            if rows.is_empty() {
                println!("(no supported repo roots)");
                return Ok(());
            }
            for r in rows {
                println!(
                    "{:>12}  {}/{:<32}  {}",
                    r.github_repo_id,
                    r.owner,
                    r.name,
                    if r.is_enabled { "ENABLED " } else { "disabled" },
                );
            }
        }
    }
    Ok(())
}

async fn run_policy(action: PolicyAction) -> anyhow::Result<()> {
    let database_url =
        std::env::var("DATABASE_URL").context("DATABASE_URL must be set (owner DSN)")?;
    let pool = db::connect(&database_url)
        .await
        .context("connect to postgres")?;

    match action {
        PolicyAction::Target { action } => run_policy_target(&pool, action).await,
        PolicyAction::Source { action } => run_policy_source(&pool, action).await,
        PolicyAction::Trigger { action } => run_policy_trigger(&pool, action).await,
    }
}

async fn run_policy_target(
    pool: &sbgh_core::db::Pool,
    action: PolicyTargetAction,
) -> anyhow::Result<()> {
    match action {
        PolicyTargetAction::Allow { install_id, repo_id, note } => {
            let row = allow_target_policy(pool, install_id, repo_id, note.as_deref())
                .await
                .context("allow target policy")?;
            println!(
                "allowed: install={} repo={} is_enabled={}",
                row.github_installation_id, row.github_repo_id, row.is_enabled,
            );
        }
        PolicyTargetAction::Disable { install_id, repo_id } => {
            let row = disable_target_policy(pool, install_id, repo_id)
                .await
                .context("disable target policy")?;
            println!(
                "disabled: install={} repo={} is_enabled={}",
                row.github_installation_id, row.github_repo_id, row.is_enabled,
            );
        }
        PolicyTargetAction::List { install_id } => {
            let rows = list_target_policies(pool, install_id)
                .await
                .context("list target policies")?;
            if rows.is_empty() {
                println!("(no target policies)");
                return Ok(());
            }
            for r in rows {
                println!(
                    "{:>12} {:>12}  {}",
                    r.github_installation_id,
                    r.github_repo_id,
                    if r.is_enabled { "ENABLED " } else { "disabled" },
                );
            }
        }
    }
    Ok(())
}

async fn run_policy_source(
    pool: &sbgh_core::db::Pool,
    action: PolicySourceAction,
) -> anyhow::Result<()> {
    match action {
        PolicySourceAction::Allow { install_id, repo_id, note } => {
            let row = allow_source_policy(pool, install_id, repo_id, note.as_deref())
                .await
                .context("allow source policy")?;
            println!(
                "allowed: install={} repo={} is_enabled={}",
                row.github_installation_id, row.github_repo_id, row.is_enabled,
            );
        }
        PolicySourceAction::Disable { install_id, repo_id } => {
            let row = disable_source_policy(pool, install_id, repo_id)
                .await
                .context("disable source policy")?;
            println!(
                "disabled: install={} repo={} is_enabled={}",
                row.github_installation_id, row.github_repo_id, row.is_enabled,
            );
        }
        PolicySourceAction::List { install_id } => {
            let rows = list_source_policies(pool, install_id)
                .await
                .context("list source policies")?;
            if rows.is_empty() {
                println!("(no source policies)");
                return Ok(());
            }
            for r in rows {
                println!(
                    "{:>12} {:>12}  {}",
                    r.github_installation_id,
                    r.github_repo_id,
                    if r.is_enabled { "ENABLED " } else { "disabled" },
                );
            }
        }
    }
    Ok(())
}

async fn run_policy_trigger(
    pool: &sbgh_core::db::Pool,
    action: PolicyTriggerAction,
) -> anyhow::Result<()> {
    match action {
        PolicyTriggerAction::Add {
            install_id,
            repo_id,
            kind,
            match_spec,
            bench_args,
            note,
        } => {
            // Parse `--kind` to the typed enum. Reject anything not
            // valid for slice 5's auto-trigger surface.
            let trigger_kind = match kind.as_str() {
                "branch_push" => TriggerKind::BranchPush,
                "tag_created" => TriggerKind::TagCreated,
                other => {
                    return Err(anyhow!(
                        "--kind must be `branch_push` or `tag_created` (got `{other}`)"
                    ));
                }
            };
            let row = add_trigger_policy(
                pool,
                install_id,
                repo_id,
                trigger_kind,
                &match_spec,
                bench_args.as_deref(),
                note.as_deref(),
            )
            .await
            .context("add trigger policy")?;
            println!(
                "added: id={} install={} repo={} kind={:?}",
                row.id, row.github_installation_id, row.github_repo_id, row.trigger_kind,
            );
        }
        PolicyTriggerAction::Disable { id } => {
            let row = disable_trigger_policy(pool, id)
                .await
                .context("disable trigger policy")?;
            println!("disabled: id={} is_enabled={}", row.id, row.is_enabled);
        }
        PolicyTriggerAction::List { install_id, repo_id } => {
            let rows = list_trigger_policies(pool, install_id, repo_id)
                .await
                .context("list trigger policies")?;
            if rows.is_empty() {
                println!("(no trigger policies)");
                return Ok(());
            }
            for r in rows {
                println!(
                    "{:>6} {:>12} {:>12}  {:<12}  {}  spec={}",
                    r.id,
                    r.github_installation_id,
                    r.github_repo_id,
                    format!("{:?}", r.trigger_kind),
                    if r.is_enabled { "ENABLED " } else { "disabled" },
                    r.match_spec.0,
                );
            }
        }
    }
    Ok(())
}

async fn run_user(action: UserAction) -> anyhow::Result<()> {
    let database_url =
        std::env::var("DATABASE_URL").context("DATABASE_URL must be set (owner DSN)")?;
    let pool = db::connect(&database_url)
        .await
        .context("connect to postgres")?;
    let api_base =
        std::env::var("SBGH_GH_API_BASE_URL").unwrap_or_else(|_| "https://api.github.com".into());

    match action {
        UserAction::Grant {
            login,
            user_id,
            install,
            repo,
            role,
        } => {
            let outcome = match (login, user_id) {
                (Some(l), None) => grant_role(&pool, &api_base, &l, install, repo, role.into())
                    .await
                    .context("grant role")?,
                (None, Some(id)) => grant_role_by_user_id(&pool, id, install, repo, role.into())
                    .await
                    .context("grant role")?,
                (None, None) => {
                    return Err(anyhow!("exactly one of --login or --user-id is required"));
                }
                (Some(_), Some(_)) => {
                    return Err(anyhow!("--login and --user-id are mutually exclusive"));
                }
            };
            let scope = outcome
                .role
                .github_repo_id
                .map(|r| format!("repo={r}"))
                .unwrap_or_else(|| "install-wide".into());
            // Three message shapes:
            //   - created=true → "granted" (new row)
            //   - created=false, was revoked → "reactivated" (cleared revoked_at)
            //   - created=false, was active → "already granted" (no-op)
            // The post-soft-revoke store always returns revoked_at=NULL
            // on success, so distinguishing reactivated from
            // already-active needs the pre-call state — which we don't
            // have here without a SELECT. Keep the two-state output
            // for now; a future enhancement could return the prior
            // revoked_at from grant_role for the precise message.
            let verb = if outcome.created { "granted" } else { "granted (or reactivated)" };
            println!(
                "{}: id={} user={} install={} {} role={:?}",
                verb,
                outcome.role.id,
                outcome.role.github_user_id,
                outcome
                    .role
                    .github_installation_id,
                scope,
                outcome.role.granted_role,
            );
        }
        UserAction::Revoke {
            login,
            user_id,
            install,
            repo,
            role,
        } => {
            let row = match (login, user_id) {
                (Some(l), None) => revoke_role(&pool, &api_base, &l, install, repo, role.into())
                    .await
                    .context("revoke role")?,
                (None, Some(id)) => revoke_role_by_user_id(&pool, id, install, repo, role.into())
                    .await
                    .context("revoke role")?,
                (None, None) => {
                    return Err(anyhow!("exactly one of --login or --user-id is required"));
                }
                (Some(_), Some(_)) => {
                    return Err(anyhow!("--login and --user-id are mutually exclusive"));
                }
            };
            println!(
                "revoked: id={} user={} install={} repo={:?} role={:?}",
                row.id,
                row.github_user_id,
                row.github_installation_id,
                row.github_repo_id,
                row.granted_role,
            );
        }
        UserAction::List { install, users } => {
            if users {
                let rows = list_users(&pool)
                    .await
                    .context("list users")?;
                if rows.is_empty() {
                    println!("(no users known)");
                    return Ok(());
                }
                for u in rows {
                    println!("{:>12}  {:<24}  type={:?}", u.id, u.login, u.user_type);
                }
            } else {
                let rows = list_roles(&pool, install)
                    .await
                    .context("list roles")?;
                if rows.is_empty() {
                    println!("(no grants matched)");
                    return Ok(());
                }
                for r in rows {
                    let scope = r
                        .github_repo_id
                        .map(|id| format!("repo={id}"))
                        .unwrap_or_else(|| "install-wide".into());
                    let status = if r.revoked_at.is_some() { "REVOKED " } else { "active  " };
                    println!(
                        "id={:<6} {} user={:>12} install={:>12} {:<20} role={:?}",
                        r.id,
                        status,
                        r.github_user_id,
                        r.github_installation_id,
                        scope,
                        r.granted_role,
                    );
                }
            }
        }
    }
    Ok(())
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer())
        .init();
}

fn load_env(explicit: Option<&Path>) -> anyhow::Result<()> {
    match explicit {
        Some(path) => {
            dotenvy::from_path(path)
                .with_context(|| format!("loading env file from {}", path.display()))?;
        }
        None => {
            let _ = dotenvy::dotenv();
        }
    }
    Ok(())
}
