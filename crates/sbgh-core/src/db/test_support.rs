//! Shared test infrastructure for integration tests that need a real
//! Postgres. Gated behind the `testing` cargo feature so the
//! testcontainers dependency doesn't ship in release builds.
//!
//! All `crates/*/tests/postgres_*.rs` files start from `setup_pg()`,
//! which boots an ephemeral Postgres container, connects, and runs the
//! workspace migrations. Returns `None` (with a printed notice) on
//! container start failure — that's the "no Docker daemon" case we
//! want to skip rather than fail.

use std::time::{Duration, Instant};

use testcontainers::core::ContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio::net::TcpStream;
use tokio::time::sleep;

use crate::db::{self, Pool};

/// Upper bound on the "container started but port not yet bound" race.
/// Generous compared to typical (<200ms) to absorb parallel-test load.
const PORT_READY_TIMEOUT: Duration = Duration::from_secs(3);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Tuple returned by `setup_pg`. Keep the container handle alive for
/// the duration of the test — dropping it stops the container.
pub type TestPg = (ContainerAsync<Postgres>, Pool);

/// Start a fresh Postgres container, connect, run migrations.
///
/// Returns `None` (with a printed notice) only when the container
/// itself fails to start — the "no Docker daemon" case. Once the
/// container is up, port lookup / pool connect / migrations MUST
/// succeed; we panic on those so they show up as test failures (not
/// skips).
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
        .expect("postgres container started but host port never became available");
    wait_for_tcp_accept(port)
        .await
        .expect("postgres host port exposed but never accepted TCP connections");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = db::connect(&url)
        .await
        .expect("connect to ephemeral postgres failed");
    db::migrate(&pool)
        .await
        .expect("migrations failed against ephemeral postgres");
    Some((container, pool))
}

/// Poll `get_host_port_ipv4` until Docker reports the host-side binding.
/// Closes the race between "container started" (per the testcontainers
/// runtime) and the daemon actually publishing the port.
async fn wait_for_port_exposed(container: &ContainerAsync<Postgres>) -> Option<u16> {
    let deadline = Instant::now() + PORT_READY_TIMEOUT;
    loop {
        match container
            .get_host_port_ipv4(5432)
            .await
        {
            Ok(p) => return Some(p),
            Err(_) if Instant::now() < deadline => sleep(POLL_INTERVAL).await,
            Err(_) => return None,
        }
    }
}

/// TCP-probe `(127.0.0.1, port)` until a connect succeeds. Defends
/// against the "port is bound by docker-proxy but Postgres isn't yet
/// listening behind it" gap, which would otherwise surface as a sqlx
/// connect error on the very first test that hits a fresh container.
async fn wait_for_tcp_accept(port: u16) -> Result<(), ()> {
    let deadline = Instant::now() + PORT_READY_TIMEOUT;
    loop {
        match TcpStream::connect(("127.0.0.1", port)).await {
            Ok(_) => return Ok(()),
            Err(_) if Instant::now() < deadline => sleep(POLL_INTERVAL).await,
            Err(_) => return Err(()),
        }
    }
}
