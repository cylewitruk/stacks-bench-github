use thiserror::Error;

pub type GitHubResult<T> = std::result::Result<T, GitHubError>;

/// Failures exposed by the GitHub contract and its concrete adapter.
#[derive(Debug, Error)]
pub enum GitHubError {
    /// Invalid contract input or provider response. Converts to
    /// [`sbgh_core::Error::Config`] to preserve the application classification.
    #[error("configuration error: {0}")]
    Config(String),

    /// Adapter bootstrap/authentication configuration. Converts to
    /// [`sbgh_core::Error::Other`], matching the pre-extraction boundary.
    #[error("GitHub adapter configuration error: {0}")]
    AdapterConfig(String),

    #[error("invalid command: {0}")]
    InvalidCommand(String),

    #[error("GitHub API error: {0}")]
    Api(String),

    #[error("GitHub signing error: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),

    #[error("GitHub HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("invalid GitHub HTTP header: {0}")]
    Header(#[from] reqwest::header::InvalidHeaderValue),

    #[error("GitHub credential I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<GitHubError> for sbgh_core::Error {
    fn from(error: GitHubError) -> Self {
        match error {
            GitHubError::Config(message) => Self::Config(message),
            GitHubError::InvalidCommand(message) => Self::InvalidCommand(message),
            other => Self::Other(anyhow::Error::new(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::GitHubError;

    #[test]
    fn configuration_errors_preserve_core_classification() {
        let error = sbgh_core::Error::from(GitHubError::Config("bad repository".into()));
        assert!(
            matches!(error, sbgh_core::Error::Config(ref message) if message == "bad repository")
        );
    }

    #[test]
    fn command_errors_preserve_core_classification() {
        let error = sbgh_core::Error::from(GitHubError::InvalidCommand("bad token".into()));
        assert!(
            matches!(error, sbgh_core::Error::InvalidCommand(ref message) if message == "bad token")
        );
    }

    #[test]
    fn provider_errors_remain_generic_core_failures() {
        let error = sbgh_core::Error::from(GitHubError::Api("rate limited".into()));
        assert!(matches!(error, sbgh_core::Error::Other(_)));
        assert_eq!(error.to_string(), "GitHub API error: rate limited");
    }

    #[test]
    fn adapter_configuration_errors_remain_generic_core_failures() {
        let error = sbgh_core::Error::from(GitHubError::AdapterConfig("bad key".into()));
        assert!(matches!(error, sbgh_core::Error::Other(_)));
        assert_eq!(error.to_string(), "GitHub adapter configuration error: bad key");
    }
}
