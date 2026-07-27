use std::fmt;

/// A task-neutral phase projected from the durable fleet event ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhaseLabel {
    name: String,
    terminal: bool,
}

impl PhaseLabel {
    pub fn new(name: impl Into<String>, terminal: bool) -> Self {
        Self { name: name.into(), terminal }
    }

    pub fn is_terminal(&self) -> bool {
        self.terminal
    }
}

impl fmt::Display for PhaseLabel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.name)
    }
}

/// The workflow step associated with benchmark progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowStep {
    Calibrate,
    Run,
}

impl fmt::Display for WorkflowStep {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Calibrate => "calibrate",
            Self::Run => "run",
        })
    }
}

/// A presentation-neutral progress snapshot reconstructed from a worker event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressUpdate {
    pub workflow_step: WorkflowStep,
    pub run_index: i32,
    pub requested_run_count: i32,
    pub phase: String,
    pub progress: u64,
    pub total: Option<u64>,
    pub message: Option<String>,
}

#[cfg(test)]
impl From<sbgh_driver::PhaseLabel> for PhaseLabel {
    fn from(label: sbgh_driver::PhaseLabel) -> Self {
        Self::new(label.to_string(), label.is_terminal())
    }
}

#[cfg(test)]
impl From<sbgh_driver::ProgressUpdate> for ProgressUpdate {
    fn from(progress: sbgh_driver::ProgressUpdate) -> Self {
        Self {
            workflow_step: match progress.workflow_step {
                sbgh_driver::WorkflowStep::Calibrate => WorkflowStep::Calibrate,
                sbgh_driver::WorkflowStep::Run => WorkflowStep::Run,
            },
            run_index: progress.run_index,
            requested_run_count: progress.requested_run_count,
            phase: progress.phase,
            progress: progress.progress,
            total: progress.total,
            message: progress.message,
        }
    }
}
