//! Slack-specific coalescing for best-effort benchmark progress.
//!
//! Slack task details are append-shaped on streamed cards, so this layer emits
//! only newly reached milestones while keeping a compact snapshot for
//! `chat.update` fallback renders.

use crate::bench_summary::thousands;
use crate::events::{ProgressUpdate, WorkflowStep};

const PERCENT_MILESTONE: u64 = 10;
// Total-less phases are generally status/counter streams. `txid_scan` can
// reach hundreds of thousands, so keep this coarse enough for Slack streams.
const TOTALLESS_MILESTONE: u64 = 10_000;

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
        let phase = phase_view(update)?;
        let mut additions = Vec::new();
        if self.current_phase.as_deref() != Some(update.phase.as_str()) {
            self.current_phase = Some(update.phase.clone());
            self.last_percent_milestone = None;
            additions.push(phase.heading.to_string());
        }

        let milestone = milestone(update)?;
        if Some(milestone) == self.last_percent_milestone {
            return (!additions.is_empty()).then(|| self.commit(additions));
        }

        self.last_percent_milestone = Some(milestone);
        if let Some(line) = milestone_line(update, milestone, phase) {
            additions.push(line);
        }
        if additions.is_empty() {
            return None;
        }
        Some(self.commit(additions))
    }

    fn commit(&mut self, additions: Vec<String>) -> ProgressDelta {
        self.lines
            .extend(additions.clone());
        ProgressDelta {
            details: format!("\n{}", additions.join("\n")),
        }
    }

    fn snapshot(&self) -> Option<String> {
        (!self.lines.is_empty()).then(|| self.lines.join("\n"))
    }
}

#[derive(Debug, Clone, Copy)]
struct PhaseView {
    heading: &'static str,
    unit: Option<&'static str>,
}

fn phase_view(update: &ProgressUpdate) -> Option<PhaseView> {
    let view = match update.phase.as_str() {
        "baseline" => PhaseView {
            heading: "Baseline calibration",
            unit: Some("blocks"),
        },
        "warmup" => PhaseView {
            heading: "Warming up",
            unit: Some("entries"),
        },
        "replay" => PhaseView {
            heading: "Measuring",
            unit: Some("entries"),
        },
        "indexing" | "index_merge" | "index_checkpoint" | "index_vacuum" | "txid_scan" => {
            PhaseView {
                heading: "Indexing blocks and transactions",
                unit: Some("blocks"),
            }
        }
        "metrics" => PhaseView {
            heading: "Collecting metrics",
            unit: Some("rows"),
        },
        "cleanup" => PhaseView {
            heading: "Cleaning up",
            unit: None,
        },
        // These upstream messages are operational/debug context. The Slack
        // card already shows the requested workload and daemon-owned flags.
        "setup" | "planning" => return None,
        _ => PhaseView { heading: "Working", unit: None },
    };
    Some(view)
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

fn milestone_line(update: &ProgressUpdate, milestone: u64, phase: PhaseView) -> Option<String> {
    if update.progress == 0 && update.total.is_none() {
        return None;
    }
    Some(match update.total {
        Some(total) if total > 0 => {
            let unit = phase.unit.unwrap_or("items");
            format!("{} / {} {unit} ({}%)", thousands(update.progress), thousands(total), milestone)
        }
        _ => match phase.unit {
            Some(unit) => format!("{} {unit}", thousands(update.progress)),
            None => thousands(update.progress),
        },
    })
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
                .contains("Measuring")
        );
        assert!(
            first
                .details
                .contains("1 / 100 entries (0%)")
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
        assert_eq!(next.details, "\n12 / 100 entries (10%)");
        assert!(
            transcript
                .snapshot()
                .unwrap()
                .contains("12 / 100 entries (10%)")
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
        assert!(snapshot.contains("Baseline calibration"));
        assert!(snapshot.contains("Measuring"));
        assert!(snapshot.contains("\n\n"));
    }

    #[test]
    fn total_less_progress_is_bucketed() {
        let mut transcript = SlackProgressTranscript::default();

        let first = transcript
            .push(&update("txid_scan", 38_400, None))
            .expect("new phase emits");
        assert!(
            first
                .details
                .contains("Indexing blocks and transactions")
        );
        assert!(
            first
                .details
                .contains("38,400 blocks")
        );

        assert!(
            transcript
                .push(&update("txid_scan", 39_000, None))
                .is_none(),
            "same 10,000-count bucket is quiet"
        );

        let next = transcript
            .push(&update("txid_scan", 40_000, None))
            .expect("new raw-count bucket emits");
        assert_eq!(next.details, "\n40,000 blocks");
    }

    #[test]
    fn rendering_ignores_upstream_messages() {
        let mut transcript = SlackProgressTranscript::default();
        let mut done = update("baseline", 1_900, Some(1_900));
        done.workflow_step = WorkflowStep::Calibrate;
        done.message = Some("Baseline converged after 38 segments".into());

        let first = transcript
            .push(&done)
            .expect("new phase emits");

        assert!(
            first
                .details
                .contains("Baseline calibration")
        );
        assert!(
            first
                .details
                .contains("1,900 / 1,900 blocks (100%)")
        );
        assert!(
            !first
                .details
                .contains("converged after")
        );
    }

    #[test]
    fn skips_noisy_setup_and_planning_messages() {
        let mut transcript = SlackProgressTranscript::default();
        let mut setup = update("setup", 0, None);
        setup.message = Some("DESTRUCTIVE: --dangerous-no-chainstate-copy enabled".into());
        assert!(
            transcript
                .push(&setup)
                .is_none()
        );

        let mut planning = update("planning", 0, None);
        planning.message = Some("Benchmark plan: mode=txid targets=1".into());
        assert!(
            transcript
                .push(&planning)
                .is_none()
        );
        assert!(
            transcript
                .snapshot()
                .is_none()
        );
    }
}
