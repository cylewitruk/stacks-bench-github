//! Apply embedded migrations to a Postgres pool.
//!
//! Migrations live in `<workspace>/migrations/` and are embedded at compile
//! time so the binaries can self-bootstrap without needing `sqlx-cli` on the
//! host.

use crate::PersistenceResult;
use crate::db::Pool;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

pub async fn migrate(pool: &Pool) -> PersistenceResult<()> {
    MIGRATOR
        .run(pool)
        .await
        .map_err(Into::into)
}
