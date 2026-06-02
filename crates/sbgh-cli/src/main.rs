//! Operator CLI — a pure client of the daemon `/api`.
//!
//! `installer` / `repo` / `policy` / `user` admin plus the `installation` /
//! `webhook` / `jobs` / `status` read commands all talk to the daemon. It
//! authenticates with the daemon-written **admin cookie** (the `bitcoind`
//! model): no DB credential, no GitHub access (the daemon resolves
//! logins/repos server-side), no secret in the repo.
//!
//! Commands read the cookie at `--cookie` (default
//! `/etc/sbgh/daemon/.cookie`) and target `--api-url` (default
//! `http://127.0.0.1:8787`).
//!
//! Migrations are no longer the CLI's job — the daemon applies them at
//! startup (roadmap-v3 Phase 6 retired the `migrate` subcommand).

use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{Parser, Subcommand};
use sbgh_api::{
    AddTriggerRequest, AllowInstallerRequest, AllowPolicyRequest, AllowRepoRequest, Client,
    DisableInstallerRequest, DisablePolicyRequest, DisableRepoRequest, RoleRequest, read_cookie,
};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

const DEFAULT_API_URL: &str = "http://127.0.0.1:8787";
const DEFAULT_COOKIE_PATH: &str = "/etc/sbgh/daemon/.cookie";

#[derive(Parser, Debug)]
#[command(version, about = "sbgh operator CLI")]
struct Cli {
    /// Optional env file to source before reading required vars. Useful when
    /// running outside docker. Inside the container, env is set by compose.
    #[arg(long, value_name = "PATH", global = true)]
    env_file: Option<PathBuf>,

    /// Daemon `/api` base URL (admin commands). The daemon binds this
    /// on loopback for the local operator.
    #[arg(long, value_name = "URL", global = true, default_value = DEFAULT_API_URL)]
    api_url: String,

    /// Path to the daemon-written admin cookie (regenerated each boot, mode
    /// 0600). Read by the API commands to authenticate as `admin`.
    #[arg(long, value_name = "PATH", global = true, default_value = DEFAULT_COOKIE_PATH)]
    cookie: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
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

    /// Manage the per-installation policy tables that gate which PR / push /
    /// tag events will trigger a benchmark job. Three nested groups:
    /// `target` (which repos are benchmark targets), `source` (which repos
    /// are trusted as PR sources), `trigger` (which branches / tag patterns
    /// auto-trigger jobs).
    Policy {
        #[command(subcommand)]
        action: PolicyAction,
    },

    /// Manage per-installation role grants. `grant` / `revoke` / `list`
    /// mirror the installer/repo/policy shape. Each grant is `(user,
    /// install, optional repo, role)`; `--repo` narrows to a single repo
    /// within the install, omitting it grants install-wide.
    User {
        #[command(subcommand)]
        action: UserAction,
    },

    /// Read-only: list known GitHub App installations.
    Installation {
        #[command(subcommand)]
        action: InstallationAction,
    },

    /// Read-only: inspect the webhook inbox.
    Webhook {
        #[command(subcommand)]
        action: WebhookAction,
    },

    /// Read-only: list benchmark jobs (run visibility).
    Jobs {
        #[command(subcommand)]
        action: JobsAction,
    },

    /// Probe the daemon `/api`: health + the scope the cookie resolves to.
    Status,
}

#[derive(Subcommand, Debug)]
enum UserAction {
    /// Grant a role to a user on an installation. The daemon resolves
    /// `--login` → id server-side and upserts the user before recording the
    /// grant. `--repo` narrows the grant to one repo within the install;
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
    /// (user, install, repo, role) — `--repo` MUST exactly match the grant
    /// being revoked (NULL grants are NOT matched by a repo-narrowed revoke).
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
    /// List grants, optionally filtered by install id. With `--users`
    /// instead, lists every known user (independent of role grants).
    List {
        #[arg(long, conflicts_with = "users")]
        install: Option<i64>,
        #[arg(long)]
        users: bool,
    },
}

/// Clap-facing role choices. Mapped to the API's snake_case wire names.
#[derive(clap::ValueEnum, Clone, Copy, Debug)]
enum RoleArg {
    Admin,
    TriggerPrBenchmark,
    ViewResults,
}

impl RoleArg {
    fn as_wire(self) -> &'static str {
        match self {
            RoleArg::Admin => "admin",
            RoleArg::TriggerPrBenchmark => "trigger_pr_benchmark",
            RoleArg::ViewResults => "view_results",
        }
    }
}

#[derive(Subcommand, Debug)]
enum InstallerAction {
    /// Add (or re-enable) a GitHub account on the allowlist. The daemon
    /// resolves the login → numeric account id, then upserts the row.
    Allow {
        #[arg(long)]
        login: String,
        #[arg(long)]
        note: Option<String>,
    },
    /// Soft-disable an installer (sets `is_enabled=FALSE`). The row is kept
    /// for audit; re-enable with `allow --login ...`.
    ///
    /// Exactly one of `--login` or `--account-id` must be supplied.
    /// `--login` resolves through GitHub (handles rename/recycling);
    /// `--account-id` skips resolution and is the emergency path.
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
    /// Add (or re-enable) a canonical repo on the supported list. The daemon
    /// resolves owner/name → numeric id, then upserts the identity +
    /// operator rows in one transaction.
    Allow {
        #[arg(long)]
        owner: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        note: Option<String>,
    },
    /// Soft-disable a supported root (sets is_enabled=FALSE; row kept for
    /// audit; forks of this root start being denied).
    ///
    /// Exactly one of `--owner --name` (resolves via the daemon) or
    /// `--repo-id` (emergency path) must be supplied.
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
    /// Per-installation target-repo opt-in: "this install will benchmark PRs
    /// against this repo." Requires a current membership row (FK).
    Target {
        #[command(subcommand)]
        action: PolicyPairAction,
    },
    /// Per-installation source-repo trust: "this install trusts this repo as
    /// a PR source — its code may execute in our bench VM."
    Source {
        #[command(subcommand)]
        action: PolicyPairAction,
    },
    /// Per-installation auto-trigger subscriptions for `push` / `create`
    /// events. Multiple per (install, repo).
    Trigger {
        #[command(subcommand)]
        action: PolicyTriggerAction,
    },
}

/// Shared shape for `target` and `source` policies (same args).
#[derive(Subcommand, Debug)]
enum PolicyPairAction {
    /// Allow (or re-enable) a (install, repo) pair. Operator pulls the ids
    /// from `installer list` + `repo list` first.
    Allow {
        #[arg(long)]
        install_id: i64,
        #[arg(long)]
        repo_id: i64,
        #[arg(long)]
        note: Option<String>,
    },
    /// Soft-disable a policy row.
    Disable {
        #[arg(long)]
        install_id: i64,
        #[arg(long)]
        repo_id: i64,
    },
    /// List policies. Optional `--install-id` filter.
    List {
        #[arg(long)]
        install_id: Option<i64>,
    },
}

#[derive(Subcommand, Debug)]
enum PolicyTriggerAction {
    /// Add a new trigger_policy row. `--kind` is `branch_push` or
    /// `tag_created`; `--match` is the JSON match_spec (e.g.
    /// `'{"kind":"branch_push","branch_name":"develop"}'`), validated
    /// server-side; `--args` is forwarded to the eventual job.
    Add {
        #[arg(long)]
        install_id: i64,
        #[arg(long)]
        repo_id: i64,
        #[arg(long)]
        kind: String,
        #[arg(long = "match")]
        match_spec: String,
        #[arg(long = "args")]
        bench_args: Option<String>,
        #[arg(long)]
        note: Option<String>,
    },
    /// Soft-disable a trigger row by its id.
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

#[derive(Subcommand, Debug)]
enum InstallationAction {
    /// List known installations.
    List,
}

#[derive(Subcommand, Debug)]
enum WebhookAction {
    /// Show the most recent inbox rows, newest first.
    Tail {
        /// Filter by event type (e.g. `issue_comment`, `push`).
        #[arg(long)]
        event_type: Option<String>,
        /// Filter by status (e.g. `received`, `processed`, `failed`).
        #[arg(long)]
        status: Option<String>,
        /// Max rows (server default 50).
        #[arg(long)]
        limit: Option<u32>,
    },
}

#[derive(Subcommand, Debug)]
enum JobsAction {
    /// List jobs, newest first.
    List {
        /// Filter by status.
        #[arg(long)]
        status: Option<String>,
        /// Max rows (server default 50).
        #[arg(long)]
        limit: Option<u32>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    load_env(cli.env_file.as_deref())?;
    init_tracing();

    let api_url = cli.api_url.clone();
    let cookie = cli.cookie.clone();
    // Build the API client (reading the admin cookie) on demand — `migrate`
    // never needs it.
    let mk_client = move || -> anyhow::Result<Client> {
        let token = read_cookie(&cookie).with_context(|| {
            format!("reading admin cookie from {} (is the daemon running?)", cookie.display())
        })?;
        Ok(Client::new(api_url.clone(), Some(token)))
    };

    match cli.command {
        Command::Installer { action } => run_installer(&mk_client()?, action).await,
        Command::Repo { action } => run_repo(&mk_client()?, action).await,
        Command::Policy { action } => run_policy(&mk_client()?, action).await,
        Command::User { action } => run_user(&mk_client()?, action).await,
        Command::Installation { action } => run_installation(&mk_client()?, action).await,
        Command::Webhook { action } => run_webhook(&mk_client()?, action).await,
        Command::Jobs { action } => run_jobs(&mk_client()?, action).await,
        Command::Status => run_status(&mk_client()?).await,
    }
}

async fn run_installer(client: &Client, action: InstallerAction) -> anyhow::Result<()> {
    match action {
        InstallerAction::Allow { login, note } => {
            let row = client
                .allow_installer(&AllowInstallerRequest { login, note })
                .await
                .context("allow installer")?;
            println!(
                "allowed: account_id={} login={} type={} is_enabled={}",
                row.account_id, row.login, row.account_type, row.is_enabled,
            );
        }
        InstallerAction::Disable { login, account_id } => {
            let row = client
                .disable_installer(&DisableInstallerRequest { login, account_id })
                .await
                .context("disable installer")?;
            println!(
                "disabled: account_id={} login={} is_enabled={}",
                row.account_id, row.login, row.is_enabled,
            );
        }
        InstallerAction::List => {
            let rows = client
                .list_installers()
                .await
                .context("list installers")?;
            if rows.is_empty() {
                println!("(no installers in allowlist)");
                return Ok(());
            }
            for r in rows {
                println!(
                    "{:<12} {:>12}  {:<14}  {}  note={}",
                    r.login,
                    r.account_id,
                    r.account_type,
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

async fn run_repo(client: &Client, action: RepoAction) -> anyhow::Result<()> {
    match action {
        RepoAction::Allow { owner, name, note } => {
            let row = client
                .allow_repo(&AllowRepoRequest { owner, name, note })
                .await
                .context("allow repo root")?;
            println!(
                "allowed: repo_id={} {}/{} is_enabled={}",
                row.repo_id, row.owner, row.name, row.is_enabled,
            );
        }
        RepoAction::Disable { owner, name, repo_id } => {
            let row = client
                .disable_repo(&DisableRepoRequest { owner, name, repo_id })
                .await
                .context("disable repo root")?;
            println!(
                "disabled: repo_id={} {}/{} is_enabled={}",
                row.repo_id, row.owner, row.name, row.is_enabled,
            );
        }
        RepoAction::List => {
            let rows = client
                .list_repos()
                .await
                .context("list repo roots")?;
            if rows.is_empty() {
                println!("(no supported repo roots)");
                return Ok(());
            }
            for r in rows {
                println!(
                    "{:>12}  {}/{:<32}  {}",
                    r.repo_id,
                    r.owner,
                    r.name,
                    if r.is_enabled { "ENABLED " } else { "disabled" },
                );
            }
        }
    }
    Ok(())
}

async fn run_policy(client: &Client, action: PolicyAction) -> anyhow::Result<()> {
    match action {
        PolicyAction::Target { action } => {
            run_policy_pair(client, PolicyKind::Target, action).await
        }
        PolicyAction::Source { action } => {
            run_policy_pair(client, PolicyKind::Source, action).await
        }
        PolicyAction::Trigger { action } => run_policy_trigger(client, action).await,
    }
}

#[derive(Clone, Copy)]
enum PolicyKind {
    Target,
    Source,
}

async fn run_policy_pair(
    client: &Client,
    kind: PolicyKind,
    action: PolicyPairAction,
) -> anyhow::Result<()> {
    match action {
        PolicyPairAction::Allow { install_id, repo_id, note } => {
            let req = AllowPolicyRequest { install_id, repo_id, note };
            let row = match kind {
                PolicyKind::Target => {
                    client
                        .allow_target_policy(&req)
                        .await
                }
                PolicyKind::Source => {
                    client
                        .allow_source_policy(&req)
                        .await
                }
            }
            .context("allow policy")?;
            println!(
                "allowed: install={} repo={} is_enabled={}",
                row.install_id, row.repo_id, row.is_enabled,
            );
        }
        PolicyPairAction::Disable { install_id, repo_id } => {
            let req = DisablePolicyRequest { install_id, repo_id };
            let row = match kind {
                PolicyKind::Target => {
                    client
                        .disable_target_policy(&req)
                        .await
                }
                PolicyKind::Source => {
                    client
                        .disable_source_policy(&req)
                        .await
                }
            }
            .context("disable policy")?;
            println!(
                "disabled: install={} repo={} is_enabled={}",
                row.install_id, row.repo_id, row.is_enabled,
            );
        }
        PolicyPairAction::List { install_id } => {
            let rows = match kind {
                PolicyKind::Target => {
                    client
                        .list_target_policies(install_id)
                        .await
                }
                PolicyKind::Source => {
                    client
                        .list_source_policies(install_id)
                        .await
                }
            }
            .context("list policies")?;
            if rows.is_empty() {
                println!("(no policies)");
                return Ok(());
            }
            for r in rows {
                println!(
                    "{:>12} {:>12}  {}",
                    r.install_id,
                    r.repo_id,
                    if r.is_enabled { "ENABLED " } else { "disabled" },
                );
            }
        }
    }
    Ok(())
}

async fn run_policy_trigger(client: &Client, action: PolicyTriggerAction) -> anyhow::Result<()> {
    match action {
        PolicyTriggerAction::Add {
            install_id,
            repo_id,
            kind,
            match_spec,
            bench_args,
            note,
        } => {
            // Parse the JSON match spec client-side for a fast, clear error;
            // the daemon validates its shape against `TriggerMatchSpec`.
            let match_spec: serde_json::Value =
                serde_json::from_str(&match_spec).context("--match must be valid JSON")?;
            let row = client
                .add_trigger(&AddTriggerRequest {
                    install_id,
                    repo_id,
                    kind,
                    match_spec,
                    bench_args,
                    note,
                })
                .await
                .context("add trigger policy")?;
            println!(
                "added: id={} install={} repo={} kind={}",
                row.id, row.install_id, row.repo_id, row.kind,
            );
        }
        PolicyTriggerAction::Disable { id } => {
            let row = client
                .disable_trigger(id)
                .await
                .context("disable trigger policy")?;
            println!("disabled: id={} is_enabled={}", row.id, row.is_enabled);
        }
        PolicyTriggerAction::List { install_id, repo_id } => {
            let rows = client
                .list_triggers(install_id, repo_id)
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
                    r.install_id,
                    r.repo_id,
                    r.kind,
                    if r.is_enabled { "ENABLED " } else { "disabled" },
                    r.match_spec,
                );
            }
        }
    }
    Ok(())
}

async fn run_user(client: &Client, action: UserAction) -> anyhow::Result<()> {
    match action {
        UserAction::Grant {
            login,
            user_id,
            install,
            repo,
            role,
        } => {
            let outcome = client
                .grant_role(&RoleRequest {
                    login,
                    user_id,
                    install,
                    repo,
                    role: role.as_wire().into(),
                })
                .await
                .context("grant role")?;
            let scope = outcome
                .role
                .repo_id
                .map(|r| format!("repo={r}"))
                .unwrap_or_else(|| "install-wide".into());
            let verb = if outcome.created { "granted" } else { "granted (or reactivated)" };
            println!(
                "{}: id={} user={} install={} {} role={}",
                verb,
                outcome.role.id,
                outcome.role.user_id,
                outcome.role.install_id,
                scope,
                outcome.role.role,
            );
        }
        UserAction::Revoke {
            login,
            user_id,
            install,
            repo,
            role,
        } => {
            let row = client
                .revoke_role(&RoleRequest {
                    login,
                    user_id,
                    install,
                    repo,
                    role: role.as_wire().into(),
                })
                .await
                .context("revoke role")?;
            println!(
                "revoked: id={} user={} install={} repo={:?} role={}",
                row.id, row.user_id, row.install_id, row.repo_id, row.role,
            );
        }
        UserAction::List { install, users } => {
            if users {
                let rows = client
                    .list_users()
                    .await
                    .context("list users")?;
                if rows.is_empty() {
                    println!("(no users known)");
                    return Ok(());
                }
                for u in rows {
                    println!("{:>12}  {:<24}  type={}", u.id, u.login, u.user_type);
                }
            } else {
                let rows = client
                    .list_roles(install)
                    .await
                    .context("list roles")?;
                if rows.is_empty() {
                    println!("(no grants matched)");
                    return Ok(());
                }
                for r in rows {
                    let scope = r
                        .repo_id
                        .map(|id| format!("repo={id}"))
                        .unwrap_or_else(|| "install-wide".into());
                    let status = if r.revoked { "REVOKED " } else { "active  " };
                    println!(
                        "id={:<6} {} user={:>12} install={:>12} {:<20} role={}",
                        r.id, status, r.user_id, r.install_id, scope, r.role,
                    );
                }
            }
        }
    }
    Ok(())
}

async fn run_installation(client: &Client, action: InstallationAction) -> anyhow::Result<()> {
    match action {
        InstallationAction::List => {
            let rows = client
                .list_installations()
                .await
                .context("list installations")?;
            if rows.is_empty() {
                println!("(no installations)");
                return Ok(());
            }
            for i in rows {
                let state = if i.deleted {
                    "deleted "
                } else if i.suspended {
                    "suspended"
                } else {
                    "active  "
                };
                println!(
                    "{:>12}  {:<20} {:<14}  {}  account_id={}  {}",
                    i.id, i.account_login, i.account_type, state, i.account_id, i.created_at,
                );
            }
        }
    }
    Ok(())
}

async fn run_webhook(client: &Client, action: WebhookAction) -> anyhow::Result<()> {
    match action {
        WebhookAction::Tail { event_type, status, limit } => {
            let rows = client
                .list_webhooks(event_type.as_deref(), status.as_deref(), limit)
                .await
                .context("list webhooks")?;
            if rows.is_empty() {
                println!("(no webhooks)");
                return Ok(());
            }
            for w in rows {
                println!(
                    "{:>8} {:<24} {:<16} {:<10} {:>3}x  {}  {}",
                    w.id,
                    w.delivery_id,
                    w.event_type,
                    w.status,
                    w.attempts,
                    w.received_at,
                    w.outcome
                        .as_deref()
                        .unwrap_or("-"),
                );
            }
        }
    }
    Ok(())
}

async fn run_jobs(client: &Client, action: JobsAction) -> anyhow::Result<()> {
    match action {
        JobsAction::List { status, limit } => {
            let rows = client
                .list_jobs(status.as_deref(), limit)
                .await
                .context("list jobs")?;
            if rows.is_empty() {
                println!("(no jobs)");
                return Ok(());
            }
            for j in rows {
                println!(
                    "{}  {:<12} install={:>12} repo={:>12}  {} {}  {}",
                    j.id,
                    j.status,
                    j.install_id,
                    j.repo_id,
                    j.git_ref_kind,
                    j.git_ref_display,
                    j.created_at,
                );
            }
        }
    }
    Ok(())
}

async fn run_status(client: &Client) -> anyhow::Result<()> {
    let health = client
        .health()
        .await
        .context("GET /api/health (is the daemon running?)")?;
    let who = client
        .whoami()
        .await
        .context("GET /api/whoami (is the cookie valid?)")?;
    println!("api: {}   scope: {}", health.status, who.scope);
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
