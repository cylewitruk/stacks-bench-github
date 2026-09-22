//! Bounded, ordered HTTP range streaming with disk-backed reassembly.

mod source;

use std::io;
use std::path::PathBuf;
use std::time::Duration;

use tokio::io::AsyncWrite;

const MIN_CHUNK_BYTES: u64 = 64 * 1024;
const MAX_CONNECTIONS: usize = 256;

/// Errors returned while probing, downloading, or emitting a remote object.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A caller-provided option is invalid.
    #[error("invalid option: {0}")]
    InvalidOption(String),
    /// The remote server violated the ordered range-streaming contract.
    #[error("remote protocol error: {0}")]
    Protocol(String),
    /// A range exhausted its configured retry budget.
    #[error("range {start}-{end} exhausted {attempts} attempts: {last_error}")]
    RetryExhausted {
        /// Inclusive first byte of the failed range.
        start: u64,
        /// Inclusive final byte of the failed range.
        end: u64,
        /// Total attempts made for this range.
        attempts: u32,
        /// Last transient failure.
        last_error: String,
    },
    /// An HTTP client operation failed before it could be retried.
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    /// A local spool or output operation failed.
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Result type used by ripcat.
pub type Result<T> = std::result::Result<T, Error>;

/// Settings for a bounded ordered download.
#[derive(Debug, Clone)]
pub struct DownloadOptions {
    /// Maximum number of simultaneously active HTTP range requests.
    pub connections: usize,
    /// Maximum aggregate size of the disk-backed reorder window.
    pub window_bytes: u64,
    /// Number of retries after the initial request for each range.
    pub max_retries: u32,
    /// Maximum time a range may produce no body bytes before reconnecting.
    pub idle_timeout: Duration,
    /// Initial retry delay; subsequent delays use capped exponential backoff.
    pub retry_base_delay: Duration,
    /// Parent directory for the private temporary spool directory.
    pub spool_dir: Option<PathBuf>,
}

impl Default for DownloadOptions {
    fn default() -> Self {
        Self {
            connections: 8,
            window_bytes: 512 * 1024 * 1024,
            max_retries: 20,
            idle_timeout: Duration::from_secs(30),
            retry_base_delay: Duration::from_secs(1),
            spool_dir: None,
        }
    }
}

impl DownloadOptions {
    fn validate(&self) -> Result<()> {
        if self.connections == 0 {
            return Err(Error::InvalidOption("connections must be greater than zero".into()));
        }
        if self.connections > MAX_CONNECTIONS {
            return Err(Error::InvalidOption(format!(
                "connections must not exceed {MAX_CONNECTIONS}"
            )));
        }
        if self.window_bytes / (self.connections as u64) < MIN_CHUNK_BYTES {
            return Err(Error::InvalidOption(format!(
                "window_bytes must provide at least {MIN_CHUNK_BYTES} bytes per connection"
            )));
        }
        if self.idle_timeout.is_zero() {
            return Err(Error::InvalidOption("idle_timeout must be greater than zero".into()));
        }
        Ok(())
    }
}

/// Monotonic progress emitted after an ordered chunk reaches the consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DownloadProgress {
    /// Bytes emitted to the consumer.
    pub emitted_bytes: u64,
    /// Total remote object size.
    pub total_bytes: u64,
    /// Completed ordered chunks.
    pub completed_chunks: usize,
    /// Total ordered chunks.
    pub total_chunks: usize,
    /// Range retries observed so far.
    pub retries: u64,
}

/// Summary of a completed ordered download.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadReport {
    /// Bytes emitted to the consumer.
    pub bytes: u64,
    /// Number of disk-backed chunks emitted.
    pub chunks: usize,
    /// Total individual range retries.
    pub retries: u64,
    /// Strong ETag that bound every range to one remote object.
    pub etag: String,
}

/// Stream a remote object to `writer` in byte order using default progress handling.
pub async fn stream_url<W>(
    url: impl AsRef<str>,
    writer: &mut W,
    options: DownloadOptions,
) -> Result<DownloadReport>
where
    W: AsyncWrite + Unpin,
{
    stream_url_with_progress(url, writer, options, |_| {}).await
}

/// Stream a remote object to `writer` and report ordered-consumer progress.
pub async fn stream_url_with_progress<W, F>(
    url: impl AsRef<str>,
    writer: &mut W,
    options: DownloadOptions,
    progress: F,
) -> Result<DownloadReport>
where
    W: AsyncWrite + Unpin,
    F: Fn(DownloadProgress),
{
    source::stream_url_with_progress(url.as_ref(), writer, options, progress).await
}
