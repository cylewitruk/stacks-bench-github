use std::time::Duration;

use sqlx::postgres::{PgPoolOptions, Postgres};

use crate::PersistenceResult;

pub type Pool = sqlx::Pool<Postgres>;

pub async fn connect(database_url: &str) -> PersistenceResult<Pool> {
    PgPoolOptions::new()
        .max_connections(16)
        .acquire_timeout(Duration::from_secs(5))
        .connect(database_url)
        .await
        .map_err(Into::into)
}
