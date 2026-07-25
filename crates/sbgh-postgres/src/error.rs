use thiserror::Error;

pub type PersistenceResult<T> = std::result::Result<T, PersistenceError>;

/// Failures owned by the concrete PostgreSQL adapter.
#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("database migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
}
