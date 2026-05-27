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
    allow_installer, apply_roles, disable_installer, disable_installer_by_account_id,
    list_installers,
};
use sbgh_core::db;
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
