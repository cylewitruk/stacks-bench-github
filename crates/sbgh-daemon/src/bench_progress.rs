//! Parser for `stacks-bench --json` stderr progress events.
//!
//! The upstream contract is newline-delimited JSON on stderr. Progress is
//! best-effort UI data: malformed lines, older builds, and unknown event
//! versions are ignored rather than failing the run.

use serde::Deserialize;
use serde_json::Number;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchProgressEvent {
    pub phase: String,
    pub progress: u64,
    pub total: Option<u64>,
    pub message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Envelope {
    schema_version: u32,
    event_type: String,
    event_version: u32,
    progress: Option<ProgressPayload>,
}

#[derive(Debug, Deserialize)]
struct ProgressPayload {
    phase: String,
    /// Schema-v1 upstream currently emits `current`; early drafts used
    /// `progress`. Accept both so deployed integrations survive either side of
    /// the rename.
    current: Option<Number>,
    progress: Option<Number>,
    total: Option<Number>,
    message: Option<String>,
}

pub fn parse_progress_line(line: &str) -> Option<BenchProgressEvent> {
    let envelope: Envelope = serde_json::from_str(line).ok()?;
    if envelope.schema_version != 1
        || envelope.event_type != "progress"
        || envelope.event_version != 1
    {
        return None;
    }
    let progress = envelope.progress?;
    let current = progress
        .current
        .as_ref()
        .or(progress.progress.as_ref())
        .and_then(number_to_u64)?;
    Some(BenchProgressEvent {
        phase: progress.phase,
        progress: current,
        total: progress
            .total
            .as_ref()
            .and_then(number_to_u64),
        message: progress.message,
    })
}

fn number_to_u64(n: &Number) -> Option<u64> {
    if let Some(n) = n.as_u64() {
        return Some(n);
    }
    let n = n.as_f64()?;
    if !n.is_finite() || n < 0.0 || n.fract() != 0.0 || n > u64::MAX as f64 {
        return None;
    }
    Some(n as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_progress_event_and_ignores_additive_fields() {
        let event = parse_progress_line(
            r#"{
                "schema_version": 1,
                "event_type": "progress",
                "event_version": 1,
                "ignored": true,
                "progress": {
                    "phase": "replay",
                    "progress": 42,
                    "total": 100,
                    "message": "Replaying measured entries",
                    "extra": "ok"
                }
            }"#,
        )
        .expect("valid progress event");

        assert_eq!(
            event,
            BenchProgressEvent {
                phase: "replay".into(),
                progress: 42,
                total: Some(100),
                message: Some("Replaying measured entries".into()),
            }
        );
    }

    #[test]
    fn parses_current_field_and_integer_valued_floats() {
        let event = parse_progress_line(
            r#"{"schema_version":1,"event_type":"progress","event_version":1,
                "progress":{"phase":"txid_scan","current":38400.0,
                    "total":40000.0,"message":"Scanned 38400 blocks"}}"#,
        )
        .expect("valid current-style progress event");

        assert_eq!(event.phase, "txid_scan");
        assert_eq!(event.progress, 38_400);
        assert_eq!(event.total, Some(40_000));
        assert_eq!(event.message.as_deref(), Some("Scanned 38400 blocks"));
    }

    #[test]
    fn parses_status_only_progress_event() {
        let event = parse_progress_line(
            r#"{"schema_version":1,"event_type":"progress","event_version":1,
                "progress":{"phase":"planning","progress":0}}"#,
        )
        .expect("valid status-only progress event");

        assert_eq!(event.phase, "planning");
        assert_eq!(event.progress, 0);
        assert_eq!(event.total, None);
        assert_eq!(event.message, None);
    }

    #[test]
    fn ignores_fractional_or_missing_counters() {
        assert!(
            parse_progress_line(
                r#"{"schema_version":1,"event_type":"progress","event_version":1,
                    "progress":{"phase":"replay","current":1.5}}"#,
            )
            .is_none()
        );
        assert!(
            parse_progress_line(
                r#"{"schema_version":1,"event_type":"progress","event_version":1,
                    "progress":{"phase":"replay"}}"#,
            )
            .is_none()
        );
    }

    #[test]
    fn ignores_unknown_or_malformed_lines() {
        assert!(parse_progress_line("not json").is_none());
        assert!(
            parse_progress_line(
                r#"{"schema_version":2,"event_type":"progress","event_version":1,
                    "progress":{"phase":"replay","progress":1}}"#,
            )
            .is_none()
        );
        assert!(
            parse_progress_line(
                r#"{"schema_version":1,"event_type":"other","event_version":1,
                    "progress":{"phase":"replay","progress":1}}"#,
            )
            .is_none()
        );
        assert!(
            parse_progress_line(
                r#"{"schema_version":1,"event_type":"progress","event_version":2,
                    "progress":{"phase":"replay","progress":1}}"#,
            )
            .is_none()
        );
        assert!(
            parse_progress_line(
                r#"{"schema_version":1,"event_type":"progress","event_version":1}"#
            )
            .is_none()
        );
    }
}
