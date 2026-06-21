//! Slack-specific coalescing for best-effort benchmark progress.
//!
//! Slack task details are append-shaped on streamed cards, so this layer emits
//! only newly reached milestones while keeping a compact snapshot for
//! `chat.update` fallback renders.

use crate::bench_summary::thousands;
use crate::events::{ProgressUpdate, WorkflowStep};

const PERCENT_MILESTONE: u64 = 10;
const TOTALLESS_MILESTONE: u64 = 10;

#[derive(Debug, Default, Clone)]
pub struct SlackProgressTranscript {
    calibrate: StepTranscript,
    run: StepTranscript,
}

impl SlackProgressTranscript {
    pub fn push(&mut self, update: &ProgressUpdate) -> Option<ProgressDelta> {
        let step = match update.workflow_step {
            WorkflowStep::Calibrate => &mut self.calibrate,
            WorkflowStep::Run => &mut self.run,
        };
        step.push(update)
    }

    pub fn snapshot(&self) -> Option<String> {
        let mut sections = Vec::new();
        if let Some(section) = self.calibrate.snapshot() {
            sections.push(section);
        }
        if let Some(section) = self.run.snapshot() {
            sections.push(section);
        }
        (!sections.is_empty()).then(|| sections.join("\n\n"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressDelta {
    pub details: String,
}

#[derive(Debug, Default, Clone)]
struct StepTranscript {
    current_phase: Option<String>,
    lines: Vec<String>,
    last_percent_milestone: Option<u64>,
}

impl StepTranscript {
    fn push(&mut self, update: &ProgressUpdate) -> Option<ProgressDelta> {
        let mut additions = Vec::new();
        if self.current_phase.as_deref() != Some(update.phase.as_str()) {
            self.current_phase = Some(update.phase.clone());
            self.last_percent_milestone = None;
            additions.push(phase_heading(update));
        }

        let milestone = milestone(update)?;
        if Some(milestone) == self.last_percent_milestone {
            return (!additions.is_empty()).then(|| self.commit(additions));
        }

        self.last_percent_milestone = Some(milestone);
        additions.push(milestone_line(update, milestone));
        Some(self.commit(additions))
    }

    fn commit(&mut self, additions: Vec<String>) -> ProgressDelta {
        self.lines
            .extend(additions.clone());
        ProgressDelta { details: additions.join("\n") }
    }

    fn snapshot(&self) -> Option<String> {
        (!self.lines.is_empty()).then(|| self.lines.join("\n"))
    }
}

fn phase_heading(update: &ProgressUpdate) -> String {
    let label = phase_label(&update.phase);
    match &update.message {
        Some(message) if !message.trim().is_empty() => format!("{label}: {}", message.trim()),
        _ => label.to_string(),
    }
}

fn milestone(update: &ProgressUpdate) -> Option<u64> {
    match update.total {
        Some(total) if total > 0 => {
            let percent = update
                .progress
                .saturating_mul(100)
                .checked_div(total)
                .unwrap_or(0)
                .min(100);
            Some(percent / PERCENT_MILESTONE * PERCENT_MILESTONE)
        }
        _ => Some(update.progress / TOTALLESS_MILESTONE * TOTALLESS_MILESTONE),
    }
}

fn milestone_line(update: &ProgressUpdate, milestone: u64) -> String {
    match update.total {
        Some(total) if total > 0 => {
            format!("{} / {} ({}%)", thousands(update.progress), thousands(total), milestone)
        }
        _ => thousands(update.progress),
    }
}

fn phase_label(phase: &str) -> &'static str {
    match phase {
        "baseline" => "Calibrating baselines",
        "warmup" => "Warming up",
        "replay" => "Replaying measured entries",
        "indexing" => "Indexing chainstate",
        "index_merge" => "Merging index",
        "index_checkpoint" => "Checkpointing index",
        "index_vacuum" => "Compacting index",
        "txid_scan" => "Scanning transactions",
        "setup" => "Setting up benchmark",
        "planning" => "Planning benchmark",
        "metrics" => "Collecting metrics",
        "cleanup" => "Cleaning up",
        _ => "Working",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn update(phase: &str, progress: u64, total: Option<u64>) -> ProgressUpdate {
        ProgressUpdate {
            workflow_step: WorkflowStep::Run,
            run_index: 0,
            requested_run_count: 1,
            phase: phase.into(),
            progress,
            total,
            message: None,
        }
    }

    #[test]
    fn emits_phase_heading_and_new_percent_milestones_only() {
        let mut transcript = SlackProgressTranscript::default();

        let first = transcript
            .push(&update("replay", 1, Some(100)))
            .expect("new phase emits");
        assert!(
            first
                .details
                .contains("Replaying measured entries")
        );
        assert!(
            first
                .details
                .contains("1 / 100 (0%)")
        );

        assert!(
            transcript
                .push(&update("replay", 5, Some(100)))
                .is_none(),
            "same 10% bucket is quiet"
        );

        let next = transcript
            .push(&update("replay", 12, Some(100)))
            .expect("new 10% bucket emits");
        assert_eq!(next.details, "12 / 100 (10%)");
        assert!(
            transcript
                .snapshot()
                .unwrap()
                .contains("12 / 100 (10%)")
        );
    }

    #[test]
    fn keeps_calibration_and_run_transcripts_separate() {
        let mut transcript = SlackProgressTranscript::default();
        let mut calibrate = update("baseline", 50, Some(100));
        calibrate.workflow_step = WorkflowStep::Calibrate;
        transcript.push(&calibrate);
        transcript.push(&update("replay", 50, Some(100)));

        let snapshot = transcript.snapshot().unwrap();
        assert!(snapshot.contains("Calibrating baselines"));
        assert!(snapshot.contains("Replaying measured entries"));
        assert!(snapshot.contains("\n\n"));
    }

    #[test]
    fn total_less_progress_is_bucketed() {
        let mut transcript = SlackProgressTranscript::default();

        let first = transcript
            .push(&update("planning", 1, None))
            .expect("new phase emits");
        assert!(
            first
                .details
                .contains("Planning benchmark")
        );
        assert!(first.details.contains("1"));

        assert!(
            transcript
                .push(&update("planning", 5, None))
                .is_none(),
            "same raw-count bucket is quiet"
        );

        let next = transcript
            .push(&update("planning", 12, None))
            .expect("new raw-count bucket emits");
        assert_eq!(next.details, "12");
    }
}
