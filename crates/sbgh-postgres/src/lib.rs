pub mod admin;
pub mod application;
mod error;
mod mapping;
pub mod migrate;
pub mod pool;
pub mod postgres_ingest;
pub mod postgres_installation;
pub mod postgres_jobs;
pub mod postgres_policy;
pub mod postgres_pull_request;
pub mod postgres_repo;
pub mod postgres_user;
pub mod postgres_webhook;
#[cfg(any(test, feature = "testing"))]
pub mod test_support;

pub use error::{PersistenceError, PersistenceResult};
#[doc(hidden)]
pub use mapping::Db;
pub use migrate::migrate;
pub use pool::{Pool, connect};
pub use postgres_ingest::PostgresIngestStore;
pub use postgres_installation::PostgresInstallationStore;
pub use postgres_jobs::PostgresJobStore;
pub use postgres_policy::PostgresPolicyStore;
pub use postgres_pull_request::PostgresPullRequestStore;
pub use postgres_repo::PostgresRepoStore;
pub use postgres_user::PostgresUserStore;
pub use postgres_webhook::PostgresWebhookInbox;
#[cfg(any(test, feature = "testing"))]
pub use test_support::{TestDb, TestPgDb, setup_pg_db};

pub use sbgh_core::{Error, Result, models};

pub(crate) trait IntoCoreResult<T> {
    fn core(self) -> sbgh_core::Result<T>;
}

impl<T, E> IntoCoreResult<T> for std::result::Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn core(self) -> sbgh_core::Result<T> {
        self.map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))
    }
}

pub mod db {
    pub use sbgh_core::db::*;

    pub use crate::{
        Pool, PostgresIngestStore, PostgresInstallationStore, PostgresJobStore,
        PostgresPolicyStore, PostgresPullRequestStore, PostgresRepoStore, PostgresUserStore,
        PostgresWebhookInbox, connect, migrate,
    };
    #[cfg(any(test, feature = "testing"))]
    pub use crate::{TestDb, TestPgDb, setup_pg_db};
}
