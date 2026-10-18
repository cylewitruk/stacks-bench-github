//! Schema startup boundary for the exact supported predecessor.

use std::process::{Output, Stdio};
use std::time::Duration;

use sbgh_postgres::{Pool, migrate, setup_pg_db_to};
use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::time;
use uuid::Uuid;

/// Last migration embedded by the deployed predecessor.
const PREDECESSOR_MIGRATION: i64 = 20_260_731_000_002;

/// Resolve only a generated, per-test database; never accept an operator DSN.
async fn database_name(pool: &Pool) -> String {
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await
        .unwrap();
    assert!(
        database
            .strip_prefix("sbgh_test_")
            .is_some_and(|suffix| {
                !suffix.is_empty()
                    && suffix
                        .bytes()
                        .all(|byte| byte.is_ascii_digit())
            })
    );
    database
}

/// Run the unchanged predecessor migrator against this generated fixture only.
async fn probe(pool: &Pool) -> Output {
    let binary = std::env::var_os("SBGH_PREVIOUS_SCHEMA_PROBE")
        .expect("use just qualify-protocol-runtime to build the pinned schema probe");
    let database = database_name(pool).await;
    let options = pool.connect_options();
    let url = format!(
        "postgres://postgres:postgres@{}:{}/{}",
        options.get_host(),
        options.get_port(),
        database
    );
    time::timeout(
        Duration::from_secs(15),
        Command::new(binary)
            .env("SBGH_SCHEMA_PROBE_DATABASE_URL", url)
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("predecessor migration probe timed out")
    .expect("launching predecessor migration probe")
}

/// Use the compose fixture's matching PostgreSQL tools, with bounded IO.
async fn pg_tool(pool: &Pool, tool: &str, args: &[&str], input: Option<&[u8]>) -> Output {
    let database = database_name(pool).await;
    let mut child = Command::new("docker")
        .args(["exec", "-i", "sbgh-test-postgres", tool, "-U", "postgres", "-d"])
        .arg(database)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("launching isolated PostgreSQL tool");
    let mut stdin = child.stdin.take().unwrap();
    time::timeout(Duration::from_secs(30), async {
        let write = async {
            if let Some(bytes) = input {
                stdin.write_all(bytes).await?;
            }
            drop(stdin);
            Ok::<(), std::io::Error>(())
        };
        let (written, result) = tokio::join!(write, child.wait_with_output());
        written.expect("writing test archive");
        result.expect("collecting PostgreSQL tool result")
    })
    .await
    .expect("PostgreSQL tool timed out")
}

/// Snapshot the complete migration ledger, including checksums and timestamps.
async fn ledger(pool: &Pool) -> Value {
    sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(m) ORDER BY version) FROM _sqlx_migrations m")
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "run just qualify-protocol-runtime to build the pinned predecessor schema probe"]
async fn pinned_predecessor_schema_refuses_new_ledger_without_mutating_it() {
    let (_db, pool) = setup_pg_db_to(PREDECESSOR_MIGRATION).await;
    let worker = Uuid::new_v4();
    sqlx::query("INSERT INTO worker_registry (worker_id, display_name, allowed_capabilities, enabled, draining) VALUES ($1, 'rollback-fixture', ARRAY['build_only']::worker_capability[], false, true)")
        .bind(worker).execute(&pool).await.unwrap();
    let before = probe(&pool).await;
    assert!(before.status.success(), "{before:?}");
    assert_eq!(before.stdout, b"migration_ready\n");
    let original_ledger = ledger(&pool).await;
    let original_worker: Value =
        sqlx::query_scalar("SELECT to_jsonb(w) FROM worker_registry w WHERE worker_id=$1")
            .bind(worker)
            .fetch_one(&pool)
            .await
            .unwrap();
    let archive = pg_tool(&pool, "pg_dump", &["--format=custom"], None).await;
    assert!(archive.status.success(), "{archive:?}");

    migrate(&pool).await.unwrap();
    let upgraded = ledger(&pool).await;
    let rejected = probe(&pool).await;
    assert_eq!(rejected.status.code(), Some(2), "{rejected:?}");
    let message = String::from_utf8(rejected.stdout).unwrap();
    let version: i64 = message
        .trim()
        .strip_prefix("migration_version_missing=")
        .expect("typed unknown-migration rejection")
        .parse()
        .unwrap();
    assert!(version > PREDECESSOR_MIGRATION);
    assert_eq!(ledger(&pool).await, upgraded);

    migrate(&pool).await.unwrap();
    assert_eq!(ledger(&pool).await, upgraded);

    // Restore into a separately guarded database, never over the upgraded one.
    let (_restored_db, restored) = setup_pg_db_to(PREDECESSOR_MIGRATION).await;
    let restore = pg_tool(
        &restored,
        "pg_restore",
        &["--clean", "--if-exists", "--exit-on-error", "--no-owner", "--no-privileges"],
        Some(&archive.stdout),
    )
    .await;
    assert!(restore.status.success(), "{restore:?}");
    assert_eq!(ledger(&restored).await, original_ledger);
    let restored_worker: Value =
        sqlx::query_scalar("SELECT to_jsonb(w) FROM worker_registry w WHERE worker_id=$1")
            .bind(worker)
            .fetch_one(&restored)
            .await
            .unwrap();
    assert_eq!(restored_worker, original_worker);
    let reopened = probe(&restored).await;
    assert!(reopened.status.success(), "{reopened:?}");
    assert_eq!(reopened.stdout, b"migration_ready\n");
    assert_eq!(ledger(&pool).await, upgraded);
}
