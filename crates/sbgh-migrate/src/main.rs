//! One-shot binary: apply schema migrations + idempotently create the two
//! narrow Postgres roles the runtime services use.
//!
//! Runs as the DB **owner** (the `sbgh` account that owns the database in
//! `docker-compose.yml`). The handler and orchestrator never run with owner
//! credentials — they each connect as their own narrow role:
//!
//! - `sbgh_handler` → INSERT on a small set of jobs columns + SELECT on `id`
//!   and `github_delivery_id` (required for `INSERT ... ON CONFLICT ...
//!   RETURNING` to work). No read access to head_sha / args / result, no way to
//!   fabricate a `status='completed'` row.
//! - `sbgh_orch` → SELECT + UPDATE on the full `jobs` table.
//!
//! Run on `docker compose up` as a `service_completed_successfully` dependency
//! of both handler and orchestrator. Idempotent: re-running just re-applies
//! grants and resets passwords to whatever is in the env. Safe to run on every
//! deploy.
//!
//! Role/grant logic lives in the library half (`src/lib.rs`) so the
//! integration tests in `sbgh-migrate/tests/grants.rs` can call
//! `apply_roles` directly against an ephemeral testcontainers Postgres
//! without shelling out to this binary.
//!
//! Required env vars:
//!   - `DATABASE_URL` — owner DSN, e.g.
//!     `postgres://sbgh:OWNER_PW@postgres/sbgh`
//!   - `SBGH_HANDLER_DB_PASSWORD` — password assigned to role `sbgh_handler`
//!   - `SBGH_ORCH_DB_PASSWORD` — password assigned to role `sbgh_orch`

use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::Parser;
use sbgh_core::db;
use sbgh_migrate::apply_roles;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Parser, Debug)]
#[command(version, about = "sbgh: apply migrations + set up role grants")]
struct Args {
    /// Optional env file to source before reading required vars. Useful when
    /// running outside docker. Inside the container, env is set by compose.
    #[arg(long, value_name = "PATH")]
    env_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    load_env(args.env_file.as_deref())?;
    init_tracing();

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
