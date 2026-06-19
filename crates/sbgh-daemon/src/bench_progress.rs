//! Parser for `stacks-bench --json` stderr progress events.
//!
//! The upstream contract is newline-delimited JSON on stderr. Progress is
//! best-effort UI data: malformed lines, older builds, and unknown event
//! versions are ignored rather than failing the run.

use serde::Deserialize;

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
    progress: u64,
    total: Option<u64>,
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
    Some(BenchProgressEvent {
        phase: progress.phase,
        progress: progress.progress,
        total: progress.total,
        message: progress.message,
    })
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
