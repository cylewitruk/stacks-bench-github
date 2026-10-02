//! Presentation policy shared by new commands and a future global output flag.

use std::io::{self, Write};

use clap::Args;
use serde::Serialize;

/// Command-local output selection, ready to be supplied by global CLI options.
#[derive(Debug, Clone, Default, Args)]
pub struct OutputArgs {
    /// Emit structured JSON; watch emits one JSON object per changed snapshot.
    #[arg(long)]
    pub json: bool,
}

/// Render a typed response without mixing presentation into API operations.
pub fn print<T: Serialize>(value: &T, human: &str, output: &OutputArgs) -> anyhow::Result<()> {
    let mut stdout = io::stdout().lock();
    if output.json {
        serde_json::to_writer_pretty(&mut stdout, value)?;
        writeln!(stdout)?;
    } else {
        write!(stdout, "{human}")?;
    }
    stdout.flush()?;
    Ok(())
}

/// Strip controls from external text before displaying it in a terminal.
pub fn text(value: &str) -> String {
    value
        .chars()
        .map(|character| if character.is_control() { ' ' } else { character })
        .collect()
}

/// Format a nonnegative elapsed interval without implying an ETA.
pub fn duration(seconds: i64) -> String {
    let seconds = seconds.max(0);
    if seconds >= 3600 {
        format!("{}h {}m {}s", seconds / 3600, seconds % 3600 / 60, seconds % 60)
    } else if seconds >= 60 {
        format!("{}m {}s", seconds / 60, seconds % 60)
    } else {
        format!("{seconds}s")
    }
}

/// Format logical bytes in binary units.
pub fn bytes(value: i64) -> String {
    if value >= 1 << 30 {
        format!("{:.1} GiB", value as f64 / (1_u64 << 30) as f64)
    } else if value >= 1 << 20 {
        format!("{:.1} MiB", value as f64 / (1_u64 << 20) as f64)
    } else {
        format!("{value} B")
    }
}
