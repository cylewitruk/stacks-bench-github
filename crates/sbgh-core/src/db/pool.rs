use std::time::Duration;

use sqlx::postgres::{PgPoolOptions, Postgres};

use crate::Result;

pub type Pool = sqlx::Pool<Postgres>;

pub async fn connect(database_url: &str) -> Result<Pool> {
    let pool = PgPoolOptions::new()
        .max_connections(16)
        .acquire_timeout(Duration::from_secs(5))
        .connect(database_url)
        .await?;
    Ok(pool)
}
