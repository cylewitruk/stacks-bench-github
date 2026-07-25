use std::time::Duration;

/// Format a reporting duration as `HH:MM:SS`.
pub(crate) fn format_elapsed(duration: Duration) -> String {
    let seconds = duration.as_secs();
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let seconds = seconds % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn elapsed_time_pads_components() {
        assert_eq!(format_elapsed(Duration::from_secs(0)), "00:00:00");
        assert_eq!(format_elapsed(Duration::from_secs(5)), "00:00:05");
        assert_eq!(format_elapsed(Duration::from_secs(65)), "00:01:05");
        assert_eq!(format_elapsed(Duration::from_secs(3 * 3600 + 7 * 60 + 9)), "03:07:09");
    }
}
