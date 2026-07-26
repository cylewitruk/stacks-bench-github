//! Slack boundary error contract.

/// Failures owned by Slack transport, reconciliation, or injected persistence.
#[derive(Debug, thiserror::Error)]
pub enum SlackError {
    #[error("Slack HTTP transport failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Slack {method} HTTP {status}")]
    HttpStatus { method: String, status: reqwest::StatusCode },

    #[error("Slack {method} failed: {code}")]
    Api { method: String, code: String },

    #[error("Slack response contract violation: {0}")]
    Protocol(String),

    #[error("Slack Socket Mode startup failed: {0}")]
    Socket(String),

    #[error("Slack message reconciliation failed: {0}")]
    Reconciliation(String),

    #[error("Slack message identity persistence failed: {0}")]
    Persistence(String),
}

pub type Result<T> = std::result::Result<T, SlackError>;
