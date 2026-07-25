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
//! Migrations are the daemon's responsibility and are applied at startup; the
//! CLI has no schema-management subcommand.

use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{Parser, Subcommand};
use sbgh_api::{
    AddTriggerRequest, AllowInstallerRequest, AllowPolicyRequest, AllowRepoRequest, Client,
    DisableInstallerRequest, DisablePolicyRequest, DisableRepoRequest, PinTriggerRequest,
    RoleRequest, TriggerView, read_cookie,
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
    /// grant. Target the install with `--on <owner>/<repo>` (repo-scoped) or
    /// raw `--install` (+ optional `--repo`; omit `--repo` for install-wide).
    /// Exactly one of `--login` or `--user-id` (emergency / GH-outage path)
    /// must be supplied.
    Grant {
        #[arg(long, group = "user_grant_target")]
        login: Option<String>,
        #[arg(long, group = "user_grant_target")]
        user_id: Option<i64>,
        /// Repo-scoped grant via `owner/repo` (resolves install + repo).
        #[arg(long, value_name = "OWNER/REPO", conflicts_with_all = ["install", "repo"])]
        on: Option<String>,
        #[arg(long, required_unless_present = "on")]
        install: Option<i64>,
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
    /// Allow (or re-enable) a (install, repo) pair. Give either
    /// `--on <owner>/<repo>` (resolved server-side) or the raw
    /// `--install-id` + `--repo-id`.
    Allow {
        /// Resolve install + repo from an `owner/repo` slug (same account).
        #[arg(long, value_name = "OWNER/REPO", conflicts_with_all = ["install_id", "repo_id"])]
        on: Option<String>,
        #[arg(long, requires = "repo_id")]
        install_id: Option<i64>,
        #[arg(long, requires = "install_id")]
        repo_id: Option<i64>,
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
    /// Add a new trigger_policy row. Target with `--on <owner>/<repo>` or raw
    /// `--install-id`/`--repo-id`. Spec with sugar flags (`--branch-push`,
    /// `--tag-created`) or raw `--kind` + `--match` JSON. `--args` is forwarded
    /// to the eventual job.
    Add {
        /// Resolve install + repo from an `owner/repo` slug (same account).
        #[arg(long, value_name = "OWNER/REPO", conflicts_with_all = ["install_id", "repo_id"])]
        on: Option<String>,
        #[arg(long, requires = "repo_id")]
        install_id: Option<i64>,
        #[arg(long, requires = "install_id")]
        repo_id: Option<i64>,
        /// Sugar: `branch_push` trigger on an exact branch or trailing-* prefix
        /// glob.
        #[arg(
            long,
            value_name = "BRANCH_OR_PREFIX_GLOB",
            conflicts_with_all = ["kind", "match_spec", "tag_created"]
        )]
        branch_push: Option<String>,
        /// Sugar: `tag_created` trigger matching a tag-name regex.
        #[arg(
            long,
            value_name = "REGEX",
            conflicts_with_all = ["kind", "match_spec", "branch_push"]
        )]
        tag_created: Option<String>,
        /// Raw: `branch_push` or `tag_created` (use with `--match`).
        #[arg(long, requires = "match_spec")]
        kind: Option<String>,
        /// Raw: JSON match_spec (use with `--kind`).
        #[arg(long = "match", requires = "kind")]
        match_spec: Option<String>,
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
    /// Pin a trigger's built binary in the host cache (kept past the LRU budget
    /// so a release ref stays warm). Optionally expire the pin with `--until`.
    Pin {
        #[arg(long)]
        id: i64,
        /// RFC3339 expiry, e.g. `2026-07-01T00:00:00Z`. Omit for no expiry.
        #[arg(long, value_name = "RFC3339")]
        until: Option<String>,
    },
    /// Unpin a trigger (clears the pin + any expiry).
    Unpin {
        #[arg(long)]
        id: i64,
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
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cli = Cli::parse();
    load_env(cli.env_file.as_deref())?;
    init_tracing();

    let api_url = cli.api_url.clone();
    let cookie = cli.cookie.clone();
    // Build the API client and read the admin cookie only for the selected
    // command.
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

/// Split an `owner/repo` slug, with a clear error rather than a downstream
/// 404 on a malformed value.
fn split_slug(slug: &str) -> anyhow::Result<(String, String)> {
    let (owner, repo) = slug
        .split_once('/')
        .with_context(|| format!("`{slug}` must be in <owner>/<repo> form"))?;
    if owner.is_empty() || repo.is_empty() || repo.contains('/') {
        anyhow::bail!("`{slug}` must be in <owner>/<repo> form");
    }
    Ok((owner.to_string(), repo.to_string()))
}

/// Resolve a `(install_id, repo_id)` pair from either the `--on owner/repo`
/// sugar (a server-side lookup) or the raw `--install-id`/`--repo-id` flags.
/// clap already enforces the two are mutually exclusive and that the raw pair
/// is supplied together; this adds the "at least one source" check + the
/// resolve call, and echoes what `--on` resolved to.
async fn resolve_pair(
    client: &Client,
    on: Option<String>,
    install_id: Option<i64>,
    repo_id: Option<i64>,
) -> anyhow::Result<(i64, i64)> {
    if let Some(slug) = on {
        let (owner, repo) = split_slug(&slug)?;
        let r = client
            .resolve_repo(&owner, &repo)
            .await
            .with_context(|| format!("resolve --on {owner}/{repo}"))?;
        eprintln!(
            "resolved {}/{} → install={} repo={}",
            r.repo_owner, r.repo_name, r.install_id, r.repo_id
        );
        Ok((r.install_id, r.repo_id))
    } else {
        match (install_id, repo_id) {
            (Some(i), Some(r)) => Ok((i, r)),
            _ => {
                anyhow::bail!(
                    "provide either --on <owner>/<repo> or both --install-id and --repo-id"
                )
            }
        }
    }
}

/// Build the trigger's `(kind, match_spec_json)` from either the
/// `--branch-push`/`--tag-created` sugar or the raw `--kind`/`--match` flags.
/// clap enforces mutual exclusion; this picks the supplied form.
fn build_trigger_spec(
    branch_push: Option<String>,
    tag_created: Option<String>,
    kind: Option<String>,
    match_spec: Option<String>,
) -> anyhow::Result<(String, serde_json::Value)> {
    if let Some(branch) = branch_push {
        Ok(("branch_push".to_string(), branch_push_match_spec(&branch)?))
    } else if let Some(pattern) = tag_created {
        Ok((
            "tag_created".to_string(),
            serde_json::json!({ "kind": "tag_created", "tag_pattern": pattern }),
        ))
    } else if let (Some(kind), Some(raw)) = (kind, match_spec) {
        // Parse client-side for a fast, clear error; the daemon re-validates
        // its shape against `TriggerMatchSpec`.
        let spec: serde_json::Value =
            serde_json::from_str(&raw).context("--match must be valid JSON")?;
        Ok((kind, spec))
    } else {
        anyhow::bail!(
            "provide one of: --branch-push <branch-or-prefix-glob>, --tag-created <regex>, or \
             --kind + --match"
        )
    }
}

fn branch_push_match_spec(pattern: &str) -> anyhow::Result<serde_json::Value> {
    const ERR: &str = "--branch-push supports exact branches or a trailing-* prefix glob, e.g. \
                       `develop` or `sb-integration/*`";
    if pattern.contains('*') {
        let Some(prefix) = pattern.strip_suffix('*') else {
            anyhow::bail!(ERR);
        };
        if prefix.is_empty()
            || prefix.contains('*')
            || prefix.contains('?')
            || prefix.contains('[')
            || prefix.contains(']')
        {
            anyhow::bail!(ERR);
        }
        return Ok(serde_json::json!({ "kind": "branch_prefix", "prefix": prefix }));
    }
    if pattern.contains('?') || pattern.contains('[') || pattern.contains(']') {
        anyhow::bail!(ERR);
    }
    Ok(serde_json::json!({ "kind": "branch_push", "branch_name": pattern }))
}

async fn run_policy_pair(
    client: &Client,
    kind: PolicyKind,
    action: PolicyPairAction,
) -> anyhow::Result<()> {
    match action {
        PolicyPairAction::Allow { on, install_id, repo_id, note } => {
            let (install_id, repo_id) = resolve_pair(client, on, install_id, repo_id).await?;
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
            on,
            install_id,
            repo_id,
            branch_push,
            tag_created,
            kind,
            match_spec,
            bench_args,
            note,
        } => {
            let (install_id, repo_id) = resolve_pair(client, on, install_id, repo_id).await?;
            let (kind, match_spec) =
                build_trigger_spec(branch_push, tag_created, kind, match_spec)?;
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
                    "{:>6} {:>12} {:>12}  {:<12}  {}  {}  spec={}",
                    r.id,
                    r.install_id,
                    r.repo_id,
                    r.kind,
                    if r.is_enabled { "ENABLED " } else { "disabled" },
                    pin_label(&r),
                    r.match_spec,
                );
            }
        }
        PolicyTriggerAction::Pin { id, until } => {
            let row = client
                .pin_trigger(
                    id,
                    &PinTriggerRequest {
                        pinned: true,
                        pinned_until: until,
                    },
                )
                .await
                .context("pin trigger policy")?;
            println!("pinned: id={} {}", row.id, pin_label(&row));
        }
        PolicyTriggerAction::Unpin { id } => {
            let row = client
                .pin_trigger(
                    id,
                    &PinTriggerRequest {
                        pinned: false,
                        pinned_until: None,
                    },
                )
                .await
                .context("unpin trigger policy")?;
            println!("unpinned: id={} {}", row.id, pin_label(&row));
        }
    }
    Ok(())
}

/// Operator-friendly pin state for a trigger row:
/// `pin=none` / `pin=pinned` / `pin=pinned_until:<rfc3339>` /
/// `pin=expired:<rfc3339>` (a pin whose `pinned_until` is already in the past —
/// the resolver treats it as unpinned).
fn pin_label(r: &TriggerView) -> String {
    match (r.pinned, r.pinned_until.as_deref()) {
        (false, _) => "pin=none".to_owned(),
        (true, None) => "pin=pinned".to_owned(),
        (true, Some(until)) => {
            let expired = chrono::DateTime::parse_from_rfc3339(until)
                .map(|t| t.with_timezone(&chrono::Utc) <= chrono::Utc::now())
                .unwrap_or(false);
            let state = if expired { "expired" } else { "pinned_until" };
            format!("pin={state}:{until}")
        }
    }
}

async fn run_user(client: &Client, action: UserAction) -> anyhow::Result<()> {
    match action {
        UserAction::Grant {
            login,
            user_id,
            on,
            install,
            repo,
            role,
        } => {
            // `--on` → a repo-scoped grant (resolves install + repo). Otherwise
            // `--install` (clap-required without `--on`) + optional `--repo`.
            let (install, repo) = match on {
                Some(slug) => {
                    let (owner, name) = split_slug(&slug)?;
                    let r = client
                        .resolve_repo(&owner, &name)
                        .await
                        .with_context(|| format!("resolve --on {owner}/{name}"))?;
                    eprintln!(
                        "resolved {}/{} → install={} repo={}",
                        r.repo_owner, r.repo_name, r.install_id, r.repo_id
                    );
                    (r.install_id, Some(r.repo_id))
                }
                None => (install.expect("clap: --install is required unless --on is given"), repo),
            };
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
                // Compact axes: `source/intent` (task_kind/build_target are
                // near-constant today — surfaced once build jobs land).
                println!(
                    "{}  {:<12} install={:>12} repo={:>12}  {:<24} {} {}  {}",
                    j.id,
                    j.status,
                    j.install_id,
                    j.repo_id,
                    format!("{}/{}", j.source, j.intent),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_command_graph_is_valid() {
        // clap's own consistency check — verifies every conflicts_with /
        // requires / required_unless_present / group reference points at a
        // real arg id (e.g. the `--on` exclusions on the policy/trigger/user
        // commands), and would panic on a typo'd id.
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }

    #[test]
    fn split_slug_parses_owner_repo() {
        assert_eq!(
            split_slug("cylewitruk/stacks-core").unwrap(),
            ("cylewitruk".to_string(), "stacks-core".to_string())
        );
    }

    #[test]
    fn split_slug_rejects_malformed() {
        for bad in ["no-slash", "/leading", "trailing/", "a/b/c", ""] {
            assert!(split_slug(bad).is_err(), "expected `{bad}` to be rejected");
        }
    }

    #[test]
    fn build_trigger_spec_branch_push_sugar() {
        let (kind, spec) = build_trigger_spec(Some("develop".into()), None, None, None).unwrap();
        assert_eq!(kind, "branch_push");
        assert_eq!(spec, serde_json::json!({ "kind": "branch_push", "branch_name": "develop" }));
    }

    #[test]
    fn build_trigger_spec_branch_push_trailing_star_is_prefix() {
        let (kind, spec) =
            build_trigger_spec(Some("sb-integration/*".into()), None, None, None).unwrap();
        assert_eq!(kind, "branch_push");
        assert_eq!(
            spec,
            serde_json::json!({ "kind": "branch_prefix", "prefix": "sb-integration/" })
        );
    }

    #[test]
    fn build_trigger_spec_branch_push_prefix_need_not_end_with_slash() {
        let (kind, spec) = build_trigger_spec(Some("release-*".into()), None, None, None).unwrap();
        assert_eq!(kind, "branch_push");
        assert_eq!(spec, serde_json::json!({ "kind": "branch_prefix", "prefix": "release-" }));
    }

    #[test]
    fn build_trigger_spec_branch_push_rejects_complex_globs() {
        assert!(build_trigger_spec(Some("release/*/hotfix".into()), None, None, None).is_err());
        assert!(build_trigger_spec(Some("*".into()), None, None, None).is_err());
    }

    fn tv(pinned: bool, pinned_until: Option<&str>) -> TriggerView {
        TriggerView {
            id: 1,
            install_id: 1,
            repo_id: 1,
            kind: "branch_push".into(),
            match_spec: serde_json::json!({}),
            bench_args: None,
            is_enabled: true,
            note: None,
            pinned,
            pinned_until: pinned_until.map(str::to_string),
        }
    }

    #[test]
    fn pin_label_renders_each_state() {
        assert_eq!(pin_label(&tv(false, None)), "pin=none");
        // An unpinned row never shows an expiry, even if one lingers.
        assert_eq!(pin_label(&tv(false, Some("2099-01-01T00:00:00Z"))), "pin=none");
        assert_eq!(pin_label(&tv(true, None)), "pin=pinned");
        assert_eq!(
            pin_label(&tv(true, Some("2099-01-01T00:00:00Z"))),
            "pin=pinned_until:2099-01-01T00:00:00Z"
        );
        // A pin whose expiry is already past renders as expired.
        assert_eq!(
            pin_label(&tv(true, Some("2000-01-01T00:00:00Z"))),
            "pin=expired:2000-01-01T00:00:00Z"
        );
    }

    #[test]
    fn build_trigger_spec_tag_created_sugar() {
        let (kind, spec) =
            build_trigger_spec(None, Some(r"^v\d+\.\d+\.\d+$".into()), None, None).unwrap();
        assert_eq!(kind, "tag_created");
        assert_eq!(
            spec,
            serde_json::json!({ "kind": "tag_created", "tag_pattern": r"^v\d+\.\d+\.\d+$" })
        );
    }

    #[test]
    fn build_trigger_spec_raw_kind_match() {
        let raw = r#"{"kind":"branch_push","branch_name":"main"}"#;
        let (kind, spec) =
            build_trigger_spec(None, None, Some("branch_push".into()), Some(raw.into())).unwrap();
        assert_eq!(kind, "branch_push");
        assert_eq!(spec, serde_json::from_str::<serde_json::Value>(raw).unwrap());
    }

    #[test]
    fn build_trigger_spec_requires_a_spec() {
        assert!(build_trigger_spec(None, None, None, None).is_err());
    }

    #[test]
    fn build_trigger_spec_raw_match_must_be_json() {
        assert!(
            build_trigger_spec(None, None, Some("branch_push".into()), Some("not json".into()),)
                .is_err()
        );
    }
}
