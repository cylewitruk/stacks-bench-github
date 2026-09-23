use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use futures::{StreamExt, stream};
use reqwest::header::{
    ACCEPT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, ETAG, HeaderMap, IF_MATCH, RANGE,
};
use reqwest::{Client, StatusCode};
use tempfile::Builder;
use tokio::fs::{self, OpenOptions};
use tokio::io::{self, AsyncSeekExt, AsyncWrite, AsyncWriteExt, SeekFrom};
use tokio::time::{sleep, timeout};

use crate::{DownloadOptions, DownloadProgress, DownloadReport, Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Range {
    start: u64,
    end: u64,
}

impl Range {
    fn len(self) -> u64 {
        self.end - self.start + 1
    }
}

#[derive(Debug)]
struct DownloadedChunk {
    index: usize,
    path: PathBuf,
    len: u64,
    retries: u32,
}

#[derive(Clone)]
struct HttpSource {
    client: Client,
    url: String,
    len: u64,
    etag: String,
    max_retries: u32,
    idle_timeout: Duration,
    retry_base_delay: Duration,
}

impl HttpSource {
    async fn probe(url: &str, options: &DownloadOptions) -> Result<Self> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .user_agent(format!("ripcat/{}", env!("CARGO_PKG_VERSION")))
            .build()?;
        let response = client
            .get(url)
            .header(ACCEPT_ENCODING, "identity")
            .header(RANGE, "bytes=0-0")
            .send()
            .await?;
        if response.status() != StatusCode::PARTIAL_CONTENT {
            return Err(Error::Protocol(format!(
                "range probe returned HTTP {}, expected 206",
                response.status()
            )));
        }
        let parsed = parse_content_range(response.headers())?;
        if parsed.start != 0 || parsed.end != 0 {
            return Err(Error::Protocol(format!(
                "range probe returned bytes {}-{}/{}, expected 0-0/<total>",
                parsed.start, parsed.end, parsed.total
            )));
        }
        let etag = required_header(response.headers(), ETAG.as_str())?;
        if etag.starts_with("W/") {
            return Err(Error::Protocol(
                "range probe returned a weak ETag; a strong object validator is required".into(),
            ));
        }
        let body = response.bytes().await?;
        if body.len() != 1 {
            return Err(Error::Protocol(format!(
                "range probe returned {} body bytes, expected 1",
                body.len()
            )));
        }
        Ok(Self {
            client,
            url: url.to_owned(),
            len: parsed.total,
            etag,
            max_retries: options.max_retries,
            idle_timeout: options.idle_timeout,
            retry_base_delay: options.retry_base_delay,
        })
    }

    fn len(&self) -> u64 {
        self.len
    }

    fn etag(&self) -> &str {
        &self.etag
    }

    async fn download_to(
        &self,
        index: usize,
        range: Range,
        path: PathBuf,
    ) -> Result<DownloadedChunk> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .await?;
        file.set_len(range.len())
            .await?;

        let mut received = 0u64;
        let mut retries = 0u32;
        loop {
            match self
                .download_attempt(range, received, &mut file)
                .await
            {
                Ok(written) => received += written,
                Err(AttemptError::Permanent(error)) => return Err(error),
                Err(AttemptError::Transient { message, written }) => {
                    received += written;
                    if retries >= self.max_retries {
                        return Err(Error::RetryExhausted {
                            start: range.start,
                            end: range.end,
                            attempts: retries + 1,
                            last_error: message,
                        });
                    }
                    sleep(retry_delay(self.retry_base_delay, retries)).await;
                    retries += 1;
                }
            }
            if received == range.len() {
                file.flush().await?;
                drop(file);
                return Ok(DownloadedChunk {
                    index,
                    path,
                    len: range.len(),
                    retries,
                });
            }
        }
    }

    async fn download_attempt(
        &self,
        range: Range,
        already_received: u64,
        file: &mut fs::File,
    ) -> std::result::Result<u64, AttemptError> {
        let start = range.start + already_received;
        let response = self
            .client
            .get(&self.url)
            .header(ACCEPT_ENCODING, "identity")
            .header(IF_MATCH, &self.etag)
            .header(RANGE, format!("bytes={start}-{}", range.end))
            .send()
            .await
            .map_err(|error| AttemptError::transient(error.to_string(), 0))?;

        let status = response.status();
        if status != StatusCode::PARTIAL_CONTENT {
            let message = format!("HTTP {status} for bytes {start}-{}", range.end);
            return if is_retryable_status(status) {
                Err(AttemptError::transient(message, 0))
            } else {
                Err(AttemptError::Permanent(Error::Protocol(message)))
            };
        }
        let parsed = parse_content_range(response.headers()).map_err(AttemptError::Permanent)?;
        if parsed.start != start || parsed.end != range.end || parsed.total != self.len {
            return Err(AttemptError::Permanent(Error::Protocol(format!(
                "received bytes {}-{}/{}, expected {start}-{}/{}",
                parsed.start, parsed.end, parsed.total, range.end, self.len
            ))));
        }
        let response_etag =
            required_header(response.headers(), ETAG.as_str()).map_err(AttemptError::Permanent)?;
        if response_etag != self.etag {
            return Err(AttemptError::Permanent(Error::Protocol(format!(
                "remote ETag changed from {} to {response_etag}",
                self.etag
            ))));
        }
        let expected = range.end - start + 1;
        if let Some(length) = response
            .headers()
            .get(CONTENT_LENGTH)
        {
            let length = length
                .to_str()
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| {
                    AttemptError::Permanent(Error::Protocol(
                        "invalid Content-Length response header".into(),
                    ))
                })?;
            if length != expected {
                return Err(AttemptError::Permanent(Error::Protocol(format!(
                    "response length {length} does not match requested range length {expected}"
                ))));
            }
        }

        file.seek(SeekFrom::Start(already_received))
            .await
            .map_err(Error::from)
            .map_err(AttemptError::Permanent)?;
        let mut body = response.bytes_stream();
        let mut written = 0u64;
        while written < expected {
            let next = timeout(self.idle_timeout, body.next())
                .await
                .map_err(|_| AttemptError::transient("response body stalled", written))?;
            let bytes = match next {
                Some(Ok(bytes)) => bytes,
                Some(Err(error)) => {
                    return Err(AttemptError::transient(error.to_string(), written));
                }
                None => {
                    return Err(AttemptError::transient(
                        format!("response ended after {written} of {expected} bytes"),
                        written,
                    ));
                }
            };
            if written + bytes.len() as u64 > expected {
                return Err(AttemptError::Permanent(Error::Protocol(
                    "response body exceeded its declared range".into(),
                )));
            }
            file.write_all(&bytes)
                .await
                .map_err(Error::from)
                .map_err(AttemptError::Permanent)?;
            written += bytes.len() as u64;
        }
        Ok(written)
    }
}

/// Run the range-download implementation behind the crate's public API.
pub async fn stream_url_with_progress<W, F>(
    url: &str,
    writer: &mut W,
    options: DownloadOptions,
    progress: F,
) -> Result<DownloadReport>
where
    W: AsyncWrite + Unpin,
    F: Fn(DownloadProgress) + Send + Sync,
{
    options.validate()?;
    let source = HttpSource::probe(url, &options).await?;
    let chunk_bytes = options.window_bytes / options.connections as u64;
    let ranges = plan_ranges(source.len(), chunk_bytes);
    let total_chunks = ranges.len();
    let spool = match &options.spool_dir {
        Some(parent) => Builder::new()
            .prefix("ripcat-")
            .tempdir_in(parent)?,
        None => Builder::new()
            .prefix("ripcat-")
            .tempdir()?,
    };
    let progress_state = Mutex::new(DownloadProgress {
        emitted_bytes: 0,
        total_bytes: source.len(),
        completed_chunks: 0,
        total_chunks,
        active_chunks: 0,
        retries: 0,
    });
    progress(
        *progress_state
            .lock()
            .expect("progress state poisoned"),
    );

    let downloads = stream::iter(
        ranges
            .into_iter()
            .enumerate()
            .map(|(index, range)| {
                let source = source.clone();
                let path = spool
                    .path()
                    .join(format!("chunk-{index:08}"));
                let progress_state = &progress_state;
                let progress = &progress;
                async move {
                    report_progress(progress_state, progress, |state| {
                        state.active_chunks += 1;
                    });
                    let result = source
                        .download_to(index, range, path)
                        .await;
                    report_progress(progress_state, progress, |state| {
                        state.active_chunks -= 1;
                    });
                    result
                }
            }),
    )
    .buffered(options.connections);
    futures::pin_mut!(downloads);

    let mut emitted_bytes = 0u64;
    let mut retries = 0u64;
    let mut completed_chunks = 0usize;
    while let Some(downloaded) = downloads.next().await {
        let downloaded = downloaded?;
        let mut chunk = fs::File::open(&downloaded.path).await?;
        let copied = io::copy(&mut chunk, writer).await?;
        if copied != downloaded.len {
            return Err(Error::Protocol(format!(
                "spooled chunk {} contained {copied} bytes, expected {}",
                downloaded.index, downloaded.len
            )));
        }
        fs::remove_file(&downloaded.path).await?;
        emitted_bytes += copied;
        retries += u64::from(downloaded.retries);
        completed_chunks += 1;
        report_progress(&progress_state, &progress, |state| {
            state.emitted_bytes = emitted_bytes;
            state.completed_chunks = completed_chunks;
            state.retries = retries;
        });
    }
    writer.flush().await?;

    if emitted_bytes != source.len() {
        return Err(Error::Protocol(format!(
            "emitted {emitted_bytes} bytes, expected {}",
            source.len()
        )));
    }
    Ok(DownloadReport {
        bytes: emitted_bytes,
        chunks: completed_chunks,
        retries,
        etag: source.etag().to_owned(),
    })
}

/// Publish a consistent snapshot after one transfer-state change.
fn report_progress<F>(
    state: &Mutex<DownloadProgress>,
    progress: &F,
    update: impl FnOnce(&mut DownloadProgress),
) where
    F: Fn(DownloadProgress),
{
    let current = {
        let mut current = state
            .lock()
            .expect("progress state poisoned");
        update(&mut current);
        *current
    };
    progress(current);
}

fn plan_ranges(total_bytes: u64, chunk_bytes: u64) -> Vec<Range> {
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < total_bytes {
        let end = start
            .saturating_add(chunk_bytes - 1)
            .min(total_bytes - 1);
        ranges.push(Range { start, end });
        start = end + 1;
    }
    ranges
}

#[derive(Debug)]
enum AttemptError {
    Transient { message: String, written: u64 },
    Permanent(Error),
}

impl AttemptError {
    fn transient(message: impl Into<String>, written: u64) -> Self {
        Self::Transient {
            message: message.into(),
            written,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ContentRange {
    start: u64,
    end: u64,
    total: u64,
}

fn parse_content_range(headers: &HeaderMap) -> Result<ContentRange> {
    let value = required_header(headers, CONTENT_RANGE.as_str())?;
    let value = value
        .strip_prefix("bytes ")
        .ok_or_else(|| Error::Protocol(format!("invalid Content-Range: {value}")))?;
    let (range, total) = value
        .split_once('/')
        .ok_or_else(|| Error::Protocol(format!("invalid Content-Range: {value}")))?;
    let (start, end) = range
        .split_once('-')
        .ok_or_else(|| Error::Protocol(format!("invalid Content-Range: {value}")))?;
    let parsed = ContentRange {
        start: parse_header_u64(start, "Content-Range start")?,
        end: parse_header_u64(end, "Content-Range end")?,
        total: parse_header_u64(total, "Content-Range total")?,
    };
    if parsed.start > parsed.end || parsed.end >= parsed.total {
        return Err(Error::Protocol(format!("invalid Content-Range: {value}")));
    }
    Ok(parsed)
}

fn required_header(headers: &HeaderMap, name: &str) -> Result<String> {
    headers
        .get(name)
        .ok_or_else(|| Error::Protocol(format!("missing {name} response header")))?
        .to_str()
        .map(str::to_owned)
        .map_err(|_| Error::Protocol(format!("invalid {name} response header")))
}

fn parse_header_u64(value: &str, field: &str) -> Result<u64> {
    value
        .parse()
        .map_err(|_| Error::Protocol(format!("invalid {field}: {value}")))
}

fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

fn retry_delay(base: Duration, retry: u32) -> Duration {
    base.saturating_mul(
        1u32.checked_shl(retry.min(5))
            .unwrap_or(u32::MAX),
    )
    .min(Duration::from_secs(30))
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    #[test]
    fn parses_content_range() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_RANGE, HeaderValue::from_static("bytes 10-19/100"));
        assert_eq!(
            parse_content_range(&headers).unwrap(),
            ContentRange { start: 10, end: 19, total: 100 }
        );
    }

    #[test]
    fn rejects_impossible_content_range() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_RANGE, HeaderValue::from_static("bytes 20-10/100"));
        assert!(parse_content_range(&headers).is_err());
    }

    #[test]
    fn retry_delay_is_capped() {
        assert_eq!(retry_delay(Duration::from_secs(1), 0), Duration::from_secs(1));
        assert_eq!(retry_delay(Duration::from_secs(1), 20), Duration::from_secs(30));
    }

    #[test]
    fn ranges_are_contiguous_and_bounded() {
        assert_eq!(
            plan_ranges(10, 4),
            vec![
                Range { start: 0, end: 3 },
                Range { start: 4, end: 7 },
                Range { start: 8, end: 9 },
            ]
        );
    }

    #[test]
    fn empty_objects_need_no_ranges() {
        assert!(plan_ranges(0, 4).is_empty());
    }
}
