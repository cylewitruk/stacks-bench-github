use thiserror::Error;

use crate::db::fleet::SessionRegistrationError;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    /// A session cannot be registered or recovered under the current fleet state.
    #[error(transparent)]
    SessionRegistration(#[from] SessionRegistrationError),

    /// Cleanup acknowledgement requires recovery of the authenticated session.
    #[error("worker session is inactive")]
    FleetSessionInactive,

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
