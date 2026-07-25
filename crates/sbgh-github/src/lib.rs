pub mod auth;
pub mod client;
mod error;

pub use auth::{AppCredentials, InstallationTokenCache};
pub use client::OctocrabClient;
pub use error::{GitHubError, GitHubResult};

trait IntoCoreResult<T> {
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
