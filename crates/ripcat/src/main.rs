use std::cell::Cell;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::Parser;
use ripcat::{DownloadOptions, DownloadProgress};

const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
const PROGRESS_INTERVAL: Duration = Duration::from_secs(5);
const SPEED_WINDOW_SAMPLES: usize = 60;
const MIB: f64 = 1024.0 * 1024.0;

struct ProgressMeter {
    last_sample_at: Instant,
    last_sample_bytes: u64,
    last_report_at: Instant,
    samples: VecDeque<(u64, Duration)>,
}

impl ProgressMeter {
    fn new(started_at: Instant) -> Self {
        Self {
            last_sample_at: started_at,
            last_sample_bytes: 0,
            last_report_at: started_at,
            samples: VecDeque::with_capacity(SPEED_WINDOW_SAMPLES),
        }
    }

    fn update(&mut self, progress: Option<DownloadProgress>, now: Instant) -> Option<String> {
        let emitted_bytes = progress.map_or(0, |value| value.emitted_bytes);
        let elapsed = now.duration_since(self.last_sample_at);
        let sample_bytes = emitted_bytes.saturating_sub(self.last_sample_bytes);
        self.last_sample_at = now;
        self.last_sample_bytes = emitted_bytes;
        self.samples
            .push_back((sample_bytes, elapsed));
        if self.samples.len() > SPEED_WINDOW_SAMPLES {
            self.samples.pop_front();
        }

        let progress = progress.filter(|value| value.total_bytes > 0)?;
        if now.duration_since(self.last_report_at) < PROGRESS_INTERVAL {
            return None;
        }
        self.last_report_at = now;
        Some(self.format(progress))
    }

    fn format(&self, progress: DownloadProgress) -> String {
        let (sampled_bytes, sampled_seconds) =
            self.samples
                .iter()
                .fold((0u128, 0.0), |(bytes, seconds), (sample_bytes, elapsed)| {
                    (bytes + u128::from(*sample_bytes), seconds + elapsed.as_secs_f64())
                });
        let bytes_per_second =
            if sampled_seconds > 0.0 { sampled_bytes as f64 / sampled_seconds } else { 0.0 };

        let remaining = progress
            .total_bytes
            .saturating_sub(progress.emitted_bytes);
        let eta = if bytes_per_second > 0.0 {
            let seconds = (remaining as f64 / bytes_per_second).ceil() as u64;
            format!("{:02}:{:02}:{:02}", seconds / 3600, seconds / 60 % 60, seconds % 60)
        } else {
            "unknown".to_owned()
        };
        let percent =
            (u128::from(progress.emitted_bytes) * 100 / u128::from(progress.total_bytes)) as u64;
        let transferred = format_transfer(progress.emitted_bytes, progress.total_bytes);
        format!(
            "ripcat: {percent:3}% {transferred} ({}/{} bytes) speed={:.1} MiB/s eta={eta} chunks={} retries={}",
            progress.emitted_bytes,
            progress.total_bytes,
            bytes_per_second / MIB,
            progress.completed_chunks,
            progress.retries
        )
    }
}

fn format_transfer(emitted_bytes: u64, total_bytes: u64) -> String {
    let units = [
        (1u64 << 60, "EiB"),
        (1u64 << 50, "PiB"),
        (1u64 << 40, "TiB"),
        (1u64 << 30, "GiB"),
        (1u64 << 20, "MiB"),
        (1u64 << 10, "KiB"),
    ];
    match units
        .into_iter()
        .find(|(size, _)| total_bytes >= *size)
    {
        Some((size, unit)) => format!(
            "{:.1} {unit} / {:.1} {unit}",
            emitted_bytes as f64 / size as f64,
            total_bytes as f64 / size as f64
        ),
        None => format!("{emitted_bytes} B / {total_bytes} B"),
    }
}

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
    let latest_progress = Cell::new(None::<DownloadProgress>);
    let progress = |value: DownloadProgress| latest_progress.set(Some(value));

    let mut stdout = tokio::io::stdout();
    let mut meter = ProgressMeter::new(Instant::now());
    let mut ticker =
        tokio::time::interval_at(tokio::time::Instant::now() + SAMPLE_INTERVAL, SAMPLE_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let download = ripcat::stream_url_with_progress(&args.url, &mut stdout, options, progress);
    tokio::pin!(download);
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let report = loop {
        tokio::select! {
            result = &mut download => break result?,
            _ = ticker.tick(), if !args.quiet => {
                if let Some(line) = meter.update(latest_progress.get(), Instant::now()) {
                    eprintln!("{line}");
                }
            }
            signal = &mut shutdown => {
                signal?;
                return Err("download interrupted".into());
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_uses_last_sixty_one_second_samples() {
        let started = Instant::now();
        let mut meter = ProgressMeter::new(started);
        let progress = |emitted_bytes| DownloadProgress {
            emitted_bytes,
            total_bytes: 100 * 1024 * 1024,
            completed_chunks: 1,
            total_chunks: 20,
            retries: 0,
        };

        for second in 1..5 {
            assert!(
                meter
                    .update(Some(progress(10 * 1024 * 1024)), started + Duration::from_secs(second))
                    .is_none()
            );
        }
        let first = meter
            .update(Some(progress(10 * 1024 * 1024)), started + PROGRESS_INTERVAL)
            .unwrap();
        assert!(first.contains("10.0 MiB / 100.0 MiB"), "{first}");
        assert!(first.contains("speed=2.0 MiB/s eta=00:00:45"), "{first}");
        for second in 6..10 {
            assert!(
                meter
                    .update(Some(progress(50 * 1024 * 1024)), started + Duration::from_secs(second))
                    .is_none()
            );
        }
        let second = meter
            .update(Some(progress(50 * 1024 * 1024)), started + PROGRESS_INTERVAL * 2)
            .unwrap();
        assert!(second.contains("speed=5.0 MiB/s eta=00:00:10"), "{second}");
        let mut stalled = String::new();
        for second in 11..=70 {
            if let Some(line) = meter
                .update(Some(progress(50 * 1024 * 1024)), started + Duration::from_secs(second))
            {
                stalled = line;
            }
        }
        assert_eq!(meter.samples.len(), SPEED_WINDOW_SAMPLES);
        assert!(stalled.contains("speed=0.0 MiB/s eta=unknown"), "{stalled}");
    }

    #[test]
    fn transferred_amount_uses_one_unit_for_both_values() {
        assert_eq!(format_transfer(12 << 30, 523 << 30), "12.0 GiB / 523.0 GiB");
        assert_eq!(format_transfer(12, 523), "12 B / 523 B");
    }
}
