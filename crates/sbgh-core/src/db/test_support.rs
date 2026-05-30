//! Shared test infrastructure for integration tests that need a real
//! Postgres. Gated behind the `testing` cargo feature so the
//! testcontainers dependency doesn't ship in release builds.
//!
//! All `crates/*/tests/postgres_*.rs` files start from `setup_pg()`,
//! which boots an ephemeral Postgres container, connects, and runs the
//! workspace migrations. Returns `None` (with a printed notice) on
//! container start failure — that's the "no Docker daemon" case we
//! want to skip rather than fail.

use std::future::Future;
use std::time::{Duration, Instant};

use testcontainers::core::ContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio::time::sleep;

use crate::db::{self, Pool};

/// Upper bound on the "container started but not yet reachable" race.
/// Generous compared to typical (<200ms) to absorb parallel-test load —
/// 50+ containers spinning up at once on the same docker daemon can
/// stretch port-binding and Postgres startup past 20s under sustained
/// load. The polling interval is small enough that the fast path (ready
/// on the first poll) pays almost nothing for the headroom.
const PORT_READY_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Tuple returned by `setup_pg`. Keep the container handle alive for
/// the duration of the test — dropping it stops the container.
pub type TestPg = (ContainerAsync<Postgres>, Pool);

/// Start a fresh Postgres container, connect, run migrations.
///
/// Returns `None` (with a printed notice) only when the container itself
/// fails to start — the "no Docker daemon" case. Once it's up, the
/// readiness gate (port lookup → connect) MUST succeed within the
/// timeout; we panic otherwise so a genuine, persistent problem surfaces
/// as a test failure rather than a silent skip. Both readiness steps
/// surface the LAST underlying error in the panic, so a real failure is
/// diagnosable (daemon-busy vs. container-exited vs. auth-not-ready)
/// instead of a bare "never became available".
///
/// Pins `postgres:18-trixie` to match docker-compose so tests catch
/// behavior that differs from production. Uses `with_mapped_port(0,
/// ...)` so the OS picks a free host port — required for parallel
/// test execution without collisions.
pub async fn setup_pg() -> Option<TestPg> {
    let container = match Postgres::default()
        .with_tag("18-trixie")
        .with_mapped_port(0, ContainerPort::Tcp(5432))
        .start()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: failed to start postgres container ({e}); Docker not reachable?");
            return None;
        }
    };
    let port = wait_for_port_exposed(&container)
        .await
        .unwrap_or_else(|e| panic!("postgres host port never became available: {e}"));
    // The only true readiness signal is an accepted connection. Polling
    // `db::connect` until it succeeds replaces the old TCP-probe + fixed
    // 250ms sleep, which only guessed at "Postgres is ready" and lost the
    // race under parallel-test load (the pool connected before Postgres
    // had finished startup / begun accepting auth).
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = connect_when_ready(&url)
        .await
        .unwrap_or_else(|e| panic!("postgres never accepted a connection on {url}: {e}"));
    db::migrate(&pool)
        .await
        .expect("migrations failed against ephemeral postgres");
    Some((container, pool))
}

/// Poll `get_host_port_ipv4` until Docker reports the host-side binding,
/// or the deadline passes. Closes the race between "container started"
/// (per the testcontainers runtime) and the daemon publishing the port.
/// Returns the last error on timeout so the caller can report WHY (e.g.
/// the container exited) instead of a bare "not available".
async fn wait_for_port_exposed(container: &ContainerAsync<Postgres>) -> Result<u16, String> {
    poll_until_ready(|| async {
        container
            .get_host_port_ipv4(5432)
            .await
            .map_err(|e| e.to_string())
    })
    .await
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
