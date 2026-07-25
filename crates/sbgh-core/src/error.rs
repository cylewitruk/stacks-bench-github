use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("unsupported event: {0}")]
    UnsupportedEvent(String),

    #[error("invalid command: {0}")]
    InvalidCommand(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("{0}")]
    Other(#[from] anyhow::Error),
}
