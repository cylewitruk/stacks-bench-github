//! Exercise the predecessor's unchanged production migration entry point.

use std::process::ExitCode;

use anyhow::{Context, ensure};
use sbgh_postgres::{PersistenceError, connect, migrate};
use sqlx::migrate::MigrateError;

/// Refuse non-fixture databases and report the typed migration outcome.
#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<ExitCode> {
    let url = std::env::var("SBGH_SCHEMA_PROBE_DATABASE_URL")
        .context("schema probe requires an isolated test database")?;
    let pool = connect(&url).await?;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    ensure!(
        database.strip_prefix("sbgh_test_").is_some_and(|suffix| {
            !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
        }),
        "schema probe only accepts generated sbgh_test_<number> databases"
    );
    let result = migrate(&pool).await;
    pool.close().await;
    match result {
        Ok(()) => {
            println!("migration_ready");
            Ok(ExitCode::SUCCESS)
        }
        Err(PersistenceError::Migrate(MigrateError::VersionMissing(version))) => {
            println!("migration_version_missing={version}");
            Ok(ExitCode::from(2))
        }
        Err(error) => Err(error.into()),
    }
}
