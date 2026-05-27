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
//! Required env vars:
//!   - `DATABASE_URL` — owner DSN, e.g.
//!     `postgres://sbgh:OWNER_PW@postgres/sbgh`
//!   - `SBGH_HANDLER_DB_PASSWORD` — password assigned to role `sbgh_handler`
//!   - `SBGH_ORCH_DB_PASSWORD` — password assigned to role `sbgh_orch`

use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::Parser;
use sbgh_core::db;
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

/// Create (if missing) and reset passwords + grants for the two narrow roles.
///
/// Why not a SQL migration? `CREATE ROLE ... PASSWORD <literal>` doesn't
/// accept bind parameters, so the password has to be string-interpolated.
/// Doing that from Rust (with proper SQL string-literal escaping) is safer
/// than templating a migration file and easier than coordinating per-host
/// secrets through a migration tool.
async fn apply_roles(
    pool: &sbgh_core::db::Pool,
    handler_password: &str,
    orch_password: &str,
) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;

    // sbgh_handler: column-level grants.
    //
    // Columns it CAN INSERT into = exactly the fields the handler's
    // `PostgresJobStore::enqueue` writes. By NOT granting INSERT on
    // `status`, `result`, `started_at`, `finished_at`, `error`, etc.,
    // a compromised handler cannot fabricate a `status='completed'`
    // row with a fake result blob — those columns can only be written
    // by sbgh_orch's UPDATE. Server-side DEFAULTs (gen_random_uuid for
    // `id`, 'queued' for `status`, NOW() for `queued_at`) still fire
    // normally for omitted columns; you only need INSERT grant on a
    // column to *specify a value* for it.
    //
    // Columns it CAN SELECT = the two referenced by enqueue's SQL:
    //   - `id`                 read back by the RETURNING clause
    //   - `github_delivery_id` referenced by the ON CONFLICT predicate
    // Granting SELECT on these two specifically — not the whole table —
    // means a compromised handler still can't enumerate other PRs'
    // job rows (head_sha, args, result, …).
    upsert_role(&mut tx, "sbgh_handler", handler_password).await?;
    sqlx::query("REVOKE ALL ON TABLE jobs FROM sbgh_handler")
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "GRANT INSERT (repository, pr_number, head_sha, requested_by, command, args, \
         installation_id, github_delivery_id) ON TABLE jobs TO sbgh_handler",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query("GRANT SELECT (id, github_delivery_id) ON TABLE jobs TO sbgh_handler")
        .execute(&mut *tx)
        .await?;

    // sbgh_orch: SELECT + UPDATE on the full table. No INSERT (handler
    // enqueues), no DELETE (we never delete from the queue; rows
    // transition to completed/failed and are kept for audit).
    upsert_role(&mut tx, "sbgh_orch", orch_password).await?;
    sqlx::query("REVOKE ALL ON TABLE jobs FROM sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT SELECT, UPDATE ON TABLE jobs TO sbgh_orch")
        .execute(&mut *tx)
        .await?;

    // Slice 1: webhook inbox grants.
    //
    // Handler writes only the columns it can know at HTTP-receipt time:
    // delivery_id, event_type, action, payload_installation_id, payload,
    // payload_size_bytes. Server-side DEFAULTs fire for id, status,
    // received_at, next_attempt_at, attempts. SELECT on (id, delivery_id)
    // supports `INSERT ... ON CONFLICT (delivery_id) DO NOTHING RETURNING
    // id` semantics. By NOT granting status/outcome/claimed_at/etc., a
    // compromised handler can't mark webhooks processed or fabricate
    // terminal outcomes — those columns are processor-only.
    sqlx::query("REVOKE ALL ON TABLE github_webhook FROM sbgh_handler")
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "GRANT INSERT (delivery_id, event_type, action, payload_installation_id, payload, \
         payload_size_bytes) ON TABLE github_webhook TO sbgh_handler",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query("GRANT SELECT (id, delivery_id) ON TABLE github_webhook TO sbgh_handler")
        .execute(&mut *tx)
        .await?;
    // Handler needs USAGE on the implicit sequence backing the BIGSERIAL
    // id column so the server-side default can fire on its INSERTs.
    sqlx::query("GRANT USAGE ON SEQUENCE github_webhook_id_seq TO sbgh_handler")
        .execute(&mut *tx)
        .await?;

    // sbgh_orch: full inbox claim/processing access. Same shape as for
    // `jobs` — SELECT + UPDATE, no INSERT, no DELETE.
    sqlx::query("REVOKE ALL ON TABLE github_webhook FROM sbgh_orch")
        .execute(&mut *tx)
        .await?;
    sqlx::query("GRANT SELECT, UPDATE ON TABLE github_webhook TO sbgh_orch")
        .execute(&mut *tx)
        .await?;

    // USAGE on the schema is required even with table-level grants. CONNECT
    // on the database is implicit for any login role; no need to grant
    // explicitly.
    sqlx::query("GRANT USAGE ON SCHEMA public TO sbgh_handler, sbgh_orch")
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(())
}

/// Create a LOGIN role if missing, then unconditionally ALTER its password.
/// Names are constants (never user input) so direct interpolation is safe.
async fn upsert_role(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    role: &'static str,
    password: &str,
) -> anyhow::Result<()> {
    // Existence check happens in Rust, NOT in a `DO $$ ... EXECUTE '...'`
    // wrapper. Reason: the password literal `'pw'` embedded inside an
    // outer EXECUTE string would close the outer single-quoted string
    // prematurely (postgres tokenises the next char as a fresh token —
    // typically a numeric literal that explodes with "trailing junk
    // after numeric literal"). Doing the conditional from Rust means
    // each statement has exactly ONE layer of single-quoting around the
    // password, which `sql_string_literal` already handles correctly.
    let pw_lit = sql_string_literal(password);

    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)")
            .bind(role)
            .fetch_one(&mut **tx)
            .await
            .with_context(|| format!("probing for role {role}"))?;

    if !exists {
        let create = format!("CREATE ROLE {role} LOGIN PASSWORD {pw_lit}");
        sqlx::query(&create)
            .execute(&mut **tx)
            .await
            .with_context(|| format!("creating role {role}"))?;
    }

    let alter = format!("ALTER ROLE {role} WITH LOGIN PASSWORD {pw_lit}");
    sqlx::query(&alter)
        .execute(&mut **tx)
        .await
        .with_context(|| format!("resetting password for role {role}"))?;
    Ok(())
}

/// Escape an arbitrary string into a Postgres single-quoted string literal.
/// The only metacharacter inside `'…'` is `'` itself (doubled to escape).
fn sql_string_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        if ch == '\'' {
            out.push('\'');
        }
        out.push(ch);
    }
    out.push('\'');
    out
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
    use super::sql_string_literal;

    #[test]
    fn plain_password_quoted() {
        assert_eq!(sql_string_literal("hunter2"), "'hunter2'");
    }

    #[test]
    fn single_quote_is_doubled() {
        assert_eq!(sql_string_literal("it's"), "'it''s'");
        assert_eq!(sql_string_literal("''"), "''''''");
    }

    #[test]
    fn empty_string_round_trips() {
        assert_eq!(sql_string_literal(""), "''");
    }
}
