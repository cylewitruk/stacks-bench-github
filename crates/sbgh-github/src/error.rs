use thiserror::Error;

pub type GitHubResult<T> = std::result::Result<T, GitHubError>;

/// Authentication and transport failures owned by the concrete GitHub adapter.
#[derive(Debug, Error)]
pub enum GitHubError {
    #[error("GitHub adapter configuration error: {0}")]
    Config(String),

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
}
