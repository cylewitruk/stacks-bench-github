use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use clap::Parser;
use ripcat::{DownloadOptions, DownloadProgress};

#[derive(Debug, Parser)]
#[command(version, about = "Parallel HTTP ranges braided into ordered stdout")]
struct Args {
    /// HTTP(S) object to stream.
    url: String,
    /// Concurrent range requests.
    #[arg(long, default_value_t = 8)]
    connections: usize,
    /// Maximum disk-backed reorder window in MiB.
    #[arg(long, default_value_t = 512)]
    window_mib: u64,
    /// Retries after the initial request for each range.
    #[arg(long, default_value_t = 20)]
    retries: u32,
    /// Reconnect a range after this many seconds without body bytes.
    #[arg(long, default_value_t = 30)]
    idle_timeout_secs: u64,
    /// Parent directory for the private temporary spool.
    #[arg(long)]
    spool_dir: Option<PathBuf>,
    /// Suppress progress and completion reports on stderr.
    #[arg(long)]
    quiet: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let args = Args::parse();
    let window_bytes = args
        .window_mib
        .checked_mul(1024 * 1024)
        .ok_or("--window-mib is too large")?;
    let options = DownloadOptions {
        connections: args.connections,
        window_bytes,
        max_retries: args.retries,
        idle_timeout: Duration::from_secs(args.idle_timeout_secs),
        spool_dir: args.spool_dir,
        ..DownloadOptions::default()
    };
    let last_percent = Arc::new(AtomicU64::new(u64::MAX));
    let progress_percent = Arc::clone(&last_percent);
    let quiet = args.quiet;
    let progress = move |progress: DownloadProgress| {
        if quiet || progress.total_bytes == 0 {
            return;
        }
        let percent =
            ((u128::from(progress.emitted_bytes) * 100) / u128::from(progress.total_bytes)) as u64;
        let previous = progress_percent.swap(percent, Ordering::Relaxed);
        if percent != previous {
            eprintln!(
                "ripcat: {percent:3}% ({}/{}) chunks={} retries={}",
                progress.emitted_bytes,
                progress.total_bytes,
                progress.completed_chunks,
                progress.retries
            );
        }
    };

    let mut stdout = tokio::io::stdout();
    let download = ripcat::stream_url_with_progress(&args.url, &mut stdout, options, progress);
    tokio::pin!(download);
    let report = tokio::select! {
        result = &mut download => result?,
        signal = shutdown_signal() => {
            signal?;
            return Err("download interrupted".into());
        }
    };
    if !args.quiet {
        eprintln!(
            "ripcat: complete bytes={} chunks={} retries={} etag={}",
            report.bytes, report.chunks, report.retries, report.etag
        );
    }
    Ok(())
}

#[cfg(unix)]
async fn shutdown_signal() -> std::io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}
