//! Shared test infrastructure for integration tests that need a real
//! Postgres. Gated behind the `testing` cargo feature.
//!
//! [`setup_pg_db`] hands a test a fresh, migrated database on the *shared*,
//! compose-managed server (started once by the nextest `postgres` setup
//! script — see `.config/nextest.toml`). Schema isolation per test; the
//! returned [`TestDb`] guard drops the database on teardown so they don't
//! accumulate. Every DB-backed suite (sbgh-core / sbgh-cli / sbgh-daemon)
//! uses it.

use std::future::Future;
use std::time::{Duration, Instant};

use sqlx::AssertSqlSafe;
use tokio::time::sleep;

use crate::db::{self, Pool};

/// Upper bound on the "server up but not yet accepting connections" wait
/// (the nextest setup script already `--wait`s for the healthcheck, so this
/// is just headroom for the first connect). The poll interval is small
/// enough that the fast path pays almost nothing for it.
const PORT_READY_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Admin DSN for the shared, compose-managed test Postgres. The nextest
/// `postgres` setup script (see `.config/nextest.toml`) brings it up on
/// `127.0.0.1:5433` before any matching test runs.
const COMPOSE_ADMIN_DSN: &str = "postgres://postgres:postgres@127.0.0.1:5433/postgres";

/// Tuple returned by [`setup_pg_db`]: the per-test database guard plus a
/// [`Pool`] to it. Bind both (`let (_db, pool) = setup_pg_db().await;`) and
/// keep `_db` alive for the test — `pool` is a real [`Pool`], usable exactly
/// as before, and dropping `_db` drops the database.
pub type TestPgDb = (TestDb, Pool);

/// Guard that drops its per-test database when it goes out of scope —
/// including during a panic unwind, since locals still drop. Keeps the
/// shared server's databases from accumulating (they're ~8 MB each; left
/// unchecked they fill the volume within a few runs).
pub struct TestDb {
    db_name: String,
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let db_name = std::mem::take(&mut self.db_name);
        if db_name.is_empty() {
            return;
        }
        // Dropping a database is async and can't run while connections to it
        // are open. Drop is sync and we may be *inside* the test's runtime
        // (can't `block_on` that one), so do the teardown on a throwaway
        // runtime on a separate thread, and `WITH (FORCE)` to evict any
        // connections the test's pool hasn't finished closing. Best-effort:
        // a failure here only leaves one stale database for the next run's
        // server to outlive — it does not fail the test.
        let _ = std::thread::spawn(move || {
            let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            rt.block_on(async move {
                if let Ok(admin) = db::connect(COMPOSE_ADMIN_DSN).await {
                    let _ = sqlx::query(AssertSqlSafe(format!(
                        r#"DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)"#
                    )))
                    .execute(&admin)
                    .await;
                    admin.close().await;
                }
            });
        })
        .join();
    }
}

/// Create a fresh, migrated database on the shared compose Postgres and
/// return a ([`TestDb`] guard, [`Pool`]) pair. Every test gets *schema*
/// isolation (its own database) on one shared server, and the guard drops
/// that database when the test ends so they don't accumulate.
///
/// nextest runs each test in its **own process**, so the database name must
/// be unique across processes, not just threads — a process-local
/// `AtomicU64` would reset to `0` in every test process and collide. We pull
/// the next id from a Postgres `SEQUENCE` (`sbgh_test_db_seq`): a
/// cluster-wide, atomic, inter-process counter, created on first use.
///
/// Panics on failure: the nextest `postgres` setup script guarantees the
/// server is up, so an unreachable server is a misconfiguration that should
/// fail loudly, not a skip.
pub async fn setup_pg_db() -> TestPgDb {
    setup_pg_db_inner(None).await
}

/// Create a fresh database migrated only through `target`.
///
/// This is reserved for upgrade tests that must seed the immediately preceding
/// schema before applying a newer migration.
pub async fn setup_pg_db_to(target: i64) -> TestPgDb {
    setup_pg_db_inner(Some(target)).await
}

async fn setup_pg_db_inner(target: Option<i64>) -> TestPgDb {
    let admin = connect_when_ready(COMPOSE_ADMIN_DSN)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "shared test postgres not reachable on 127.0.0.1:5433 ({e}); is the nextest \
                 `postgres` setup script configured?"
            )
        });

    // First-call-wins, lazy creation of the shared counter. `IF NOT EXISTS`
    // is NOT atomic under concurrent DDL in Postgres — the parallel first
    // tests of a run race here and the loser gets a duplicate-object /
    // unique-violation on the catalog — so we treat those as success and let
    // the `nextval` below be the real existence check.
    if let Err(e) = sqlx::query("CREATE SEQUENCE IF NOT EXISTS sbgh_test_db_seq")
        .execute(&admin)
        .await
    {
        let raced = matches!(
            e.as_database_error()
                .and_then(|d| d.code())
                .as_deref(),
            Some("42P07" | "23505") // duplicate_object | unique_violation
        );
        if !raced {
            panic!("creating sbgh_test_db_seq: {e}");
        }
    }

    // Cluster-wide atomic counter — unique across the per-test processes.
    let id: i64 = sqlx::query_scalar("SELECT nextval('sbgh_test_db_seq')")
        .fetch_one(&admin)
        .await
        .expect("pulling next id from sbgh_test_db_seq");
    let db_name = format!("sbgh_test_{id}");

    // CREATE DATABASE can't run in a transaction and needs a connection to a
    // *different* database — hence the admin pool on `postgres`. The id is a
    // server-generated integer, so interpolating it (DDL takes no bind
    // params) is injection-safe.
    sqlx::query(AssertSqlSafe(format!(r#"CREATE DATABASE "{db_name}""#)))
        .execute(&admin)
        .await
        .unwrap_or_else(|e| panic!("creating test database {db_name}: {e}"));
    admin.close().await;

    let url = format!("postgres://postgres:postgres@127.0.0.1:5433/{db_name}");
    let pool = connect_when_ready(&url)
        .await
        .unwrap_or_else(|e| panic!("connecting to test database {db_name}: {e}"));
    match target {
        Some(target) => crate::migrate::migrate_to(&pool, target)
            .await
            .unwrap_or_else(|e| panic!("running migrations through {target}: {e}")),
        None => db::migrate(&pool)
            .await
            .expect("running migrations against the test database"),
    }
    (TestDb { db_name }, pool)
}

/// Poll `db::connect` until Postgres accepts a connection — the true
/// readiness signal (port bound, listener up, AND startup/auth complete)
/// — or the deadline passes. Returns the last connect error on timeout.
/// This is the readiness gate, not a fixed sleep.
async fn connect_when_ready(url: &str) -> Result<Pool, String> {
    poll_until_ready(|| async {
        db::connect(url)
            .await
            .map_err(|e| e.to_string())
    })
    .await
}

/// Retry `op` every [`POLL_INTERVAL`] until it returns `Ok` or
/// [`PORT_READY_TIMEOUT`] elapses, surfacing the last error on timeout.
async fn poll_until_ready<T, F, Fut>(mut op: F) -> Result<T, String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    let deadline = Instant::now() + PORT_READY_TIMEOUT;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if Instant::now() >= deadline {
                    return Err(e);
                }
                sleep(POLL_INTERVAL).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// End-to-end smoke test of the shared-Postgres path: each call must
    /// land in its own migrated database (the per-test isolation the suites
    /// rely on), and dropping the guard must drop that database. Exercises
    /// the full pipeline — nextest `postgres` setup script → sequence →
    /// CREATE DATABASE → migrate → guarded teardown.
    #[tokio::test]
    async fn setup_pg_db_mints_isolated_migrated_databases_and_tears_them_down() {
        let (_db_a, a) = setup_pg_db().await;
        let (db_b, b) = setup_pg_db().await;

        let name_a: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&a)
            .await
            .unwrap();
        let name_b: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&b)
            .await
            .unwrap();
        assert_ne!(name_a, name_b, "each call must get its own database");

        // Migrations ran against the fresh database.
        let applied: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(&a)
            .await
            .unwrap();
        assert!(applied > 0, "migrations should have been applied");

        // Dropping the guard (after closing its pool) must drop its database.
        b.close().await;
        drop(db_b);
        let admin = connect_when_ready(COMPOSE_ADMIN_DSN)
            .await
            .unwrap();
        let still_there: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
                .bind(&name_b)
                .fetch_one(&admin)
                .await
                .unwrap();
        assert!(!still_there, "guard drop must remove the test database {name_b}");
    }

    /// Pins the `WITH (FORCE)` path: dropping the guard while a connection to
    /// its database is still **open** must still drop it. Without `FORCE`,
    /// `DROP DATABASE` errors with "is being accessed by other users" and the
    /// best-effort teardown would silently leave the database behind. The
    /// teardown test above closes its pool first, so it never exercises this.
    #[tokio::test]
    async fn guard_drop_force_evicts_live_connections() {
        let (db, pool) = setup_pg_db().await;
        let name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .unwrap();

        // Hold a live, checked-out connection across the drop — never closed.
        let _live = pool.acquire().await.unwrap();

        drop(db); // must FORCE-evict `_live` and drop the database

        let admin = connect_when_ready(COMPOSE_ADMIN_DSN)
            .await
            .unwrap();
        let still_there: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
                .bind(&name)
                .fetch_one(&admin)
                .await
                .unwrap();
        assert!(!still_there, "guard drop must FORCE-drop {name} despite a live connection");
    }
}
