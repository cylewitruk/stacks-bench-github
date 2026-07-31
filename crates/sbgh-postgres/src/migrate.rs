//! Apply embedded migrations to a Postgres pool.
//!
//! Migrations live in `<workspace>/migrations/` and are embedded at compile
//! time so the binaries can self-bootstrap without needing `sqlx-cli` on the
//! host.

use crate::PersistenceResult;
use crate::db::Pool;

// Rebuild this crate whenever the checked-in migration ledger changes; SQLx
// embeds that ledger into every production binary.
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

pub async fn migrate(pool: &Pool) -> PersistenceResult<()> {
    MIGRATOR
        .run(pool)
        .await
        .map_err(Into::into)
}

#[cfg(any(test, feature = "testing"))]
pub(super) async fn migrate_to(pool: &Pool, target: i64) -> PersistenceResult<()> {
    MIGRATOR
        .run_to(target, pool)
        .await
        .map_err(Into::into)
}
