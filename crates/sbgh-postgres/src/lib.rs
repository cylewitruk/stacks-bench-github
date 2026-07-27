pub mod admin;
pub mod application;
mod error;
mod mapping;
pub mod migrate;
pub mod pool;
pub mod stores;
#[cfg(any(test, feature = "testing"))]
pub mod test_support;

pub use error::{PersistenceError, PersistenceResult};
#[doc(hidden)]
pub use mapping::Db;
pub use migrate::migrate;
pub use pool::{Pool, connect};
pub use stores::fleet::{PostgresFleetStore, PreparedJobProvenance};
pub use stores::ingest::PostgresIngestStore;
pub use stores::installation::PostgresInstallationStore;
pub use stores::jobs::PostgresJobStore;
pub use stores::policy::PostgresPolicyStore;
pub use stores::pull_request::PostgresPullRequestStore;
pub use stores::repo::PostgresRepoStore;
pub use stores::user::PostgresUserStore;
pub use stores::webhook::PostgresWebhookInbox;
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
        Pool, PostgresFleetStore, PostgresIngestStore, PostgresInstallationStore, PostgresJobStore,
        PostgresPolicyStore, PostgresPullRequestStore, PostgresRepoStore, PostgresUserStore,
        PostgresWebhookInbox, connect, migrate,
    };
    #[cfg(any(test, feature = "testing"))]
    pub use crate::{TestDb, TestPgDb, setup_pg_db};
}
