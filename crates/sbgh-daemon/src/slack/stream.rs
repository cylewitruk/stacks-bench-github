//! Typed request chunks for Slack `chat.*Stream` methods (v12 / item `0033`).
//!
//! The Rust Slack crates in use do not expose the new stream/task-update API,
//! so we keep a tiny local shape over the existing reqwest Web API client.

use serde::Serialize;

use crate::bench_summary::PlanTaskStatus;
use crate::slack::card::{Card, CardLink, CardRow, TASK_IDS};

/// Slack stream `task_update` / `plan_update` text fields currently cap at 256
/// characters. Keep the stream path valid even for long error strings; the
/// block fallback still carries the full text if Slack ever rejects a chunk.
const STREAM_TEXT_LIMIT: usize = 256;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamChunk {
    TaskUpdate(TaskUpdate),
    PlanUpdate { title: String },
}

/// Convert a typed card into Slack stream chunks. The Block Kit fallback and
/// the stream path share this one card model, so row text cannot drift.
pub fn chunks_for_card(card: &Card) -> Vec<StreamChunk> {
    let mut chunks = Vec::with_capacity(card.rows.len() + 1);
    chunks.push(StreamChunk::PlanUpdate {
        title: stream_text(&card.title),
    });
    for (id, row) in TASK_IDS
        .iter()
        .zip(&card.rows)
    {
        chunks.push(StreamChunk::TaskUpdate(TaskUpdate::from_row(*id, row)));
    }
    chunks
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskUpdate {
    pub id: String,
    pub title: String,
    pub status: StreamTaskStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub sources: Vec<StreamSource>,
}

impl TaskUpdate {
    pub fn from_row(id: impl Into<String>, row: &CardRow) -> Self {
        let terminal = matches!(row.status, PlanTaskStatus::Complete | PlanTaskStatus::Error);
        Self {
            id: id.into(),
            title: stream_text(&row.title),
            status: row.status.into(),
            details: (!terminal)
                .then(|| row.details.clone())
                .flatten()
                .map(|s| stream_text(&s)),
            output: terminal
                .then(|| row.output.clone())
                .flatten()
                .map(|s| stream_text(&s)),
            sources: row
                .source
                .as_ref()
                .map(StreamSource::from)
                .into_iter()
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamTaskStatus {
    Pending,
    InProgress,
    Complete,
    Error,
}

impl From<PlanTaskStatus> for StreamTaskStatus {
    fn from(value: PlanTaskStatus) -> Self {
        match value {
            PlanTaskStatus::Pending => Self::Pending,
            PlanTaskStatus::InProgress => Self::InProgress,
            PlanTaskStatus::Complete => Self::Complete,
            PlanTaskStatus::Error => Self::Error,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamSource {
    Url { text: String, url: String },
}

impl From<&CardLink> for StreamSource {
    fn from(value: &CardLink) -> Self {
        Self::Url {
            text: stream_text(&value.text),
            url: value.url.clone(),
        }
    }
}

fn stream_text(s: &str) -> String {
    if s.chars().count() <= STREAM_TEXT_LIMIT {
        return s.to_string();
    }
    let mut out: String = s
        .chars()
        .take(STREAM_TEXT_LIMIT - 1)
        .collect();
    out.push('…');
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamFailure {
    NotStreaming,
    Other,
}

pub fn classify_stream_error(error: &str) -> StreamFailure {
    if [
        "message_not_in_streaming_state",
        "stopped_by_user",
        "message_not_owned_by_app",
        "message_not_found",
    ]
    .iter()
    .any(|code| error.contains(code))
    {
        StreamFailure::NotStreaming
    } else {
        StreamFailure::Other
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::bench_summary::PlanTaskStatus;

    #[test]
    fn task_update_serializes_the_slack_shape() {
        let row = CardRow {
            title: "Building benchmark binaries".into(),
            status: PlanTaskStatus::InProgress,
            details: Some("Building for 1m 02s".into()),
            output: Some("stale output".into()),
            source: Some(CardLink {
                text: "View commit".into(),
                url: "https://github.com/o/r/commit/abc".into(),
            }),
        };
        let v = serde_json::to_value(StreamChunk::TaskUpdate(TaskUpdate::from_row("build", &row)))
            .expect("serializes");
        assert_eq!(
            v,
            json!({
                "type": "task_update",
                "id": "build",
                "title": "Building benchmark binaries",
                "status": "in_progress",
                "details": "Building for 1m 02s",
                "sources": [{
                    "type": "url",
                    "text": "View commit",
                    "url": "https://github.com/o/r/commit/abc",
                }],
            })
        );
    }

    #[test]
    fn terminal_task_update_shows_output_not_details() {
        let row = CardRow {
            title: "Built benchmark binaries".into(),
            status: PlanTaskStatus::Complete,
            details: Some("stale detail".into()),
            output: Some("Built in 6m 04s".into()),
            source: None,
        };
        let v = serde_json::to_value(TaskUpdate::from_row("build", &row)).expect("serializes");
        assert_eq!(
            v,
            json!({
                "id": "build",
                "title": "Built benchmark binaries",
                "status": "complete",
                "output": "Built in 6m 04s",
            })
        );
    }

    #[test]
    fn stream_errors_identify_fallback_cases() {
        assert_eq!(
            classify_stream_error("slack chat.appendStream failed: message_not_in_streaming_state"),
            StreamFailure::NotStreaming
        );
        assert_eq!(
            classify_stream_error("slack chat.appendStream failed: message_not_found"),
            StreamFailure::NotStreaming
        );
        assert_eq!(classify_stream_error("invalid_chunks"), StreamFailure::Other);
    }

    #[test]
    fn task_update_truncates_stream_limited_fields() {
        let long = "x".repeat(300);
        let row = CardRow {
            title: long.clone(),
            status: PlanTaskStatus::Error,
            details: None,
            output: Some(long.clone()),
            source: Some(CardLink {
                text: long,
                url: "https://example.test".into(),
            }),
        };
        let task = TaskUpdate::from_row("run", &row);
        assert_eq!(task.title.chars().count(), STREAM_TEXT_LIMIT);
        assert!(task.title.ends_with('…'));
        assert_eq!(
            task.output
                .unwrap()
                .chars()
                .count(),
            STREAM_TEXT_LIMIT
        );
        let StreamSource::Url { text, url } = &task.sources[0];
        assert_eq!(text.chars().count(), STREAM_TEXT_LIMIT);
        assert_eq!(url, "https://example.test");
    }
}
