//! Read the `.phase` marker file written by the in-VM `sbgh-run.sh` script.
//!
//! Phase values are single tokens, optionally with extra detail after a colon.
//! Unknown tokens are accepted (forward-compat).

use std::fmt;
use std::path::Path;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Phase {
    Starting,
    Building,
    Running,
    Collecting,
    Done,
    Error,
    Other(String),
}

impl Phase {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Phase::Done | Phase::Error)
    }

    pub fn label(&self) -> &str {
        match self {
            Phase::Starting => "starting",
            Phase::Building => "building",
            Phase::Running => "running",
            Phase::Collecting => "collecting",
            Phase::Done => "done",
            Phase::Error => "error",
            Phase::Other(s) => s,
        }
    }
}

impl FromStr for Phase {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.trim() {
            "starting" => Phase::Starting,
            "building" => Phase::Building,
            "running" => Phase::Running,
            "collecting" => Phase::Collecting,
            "done" => Phase::Done,
            "error" => Phase::Error,
            other => Phase::Other(other.to_string()),
        })
    }
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

pub fn read(phase_file: &Path) -> Option<Phase> {
    match std::fs::read_to_string(phase_file) {
        Ok(s) => Some(Phase::from_str(&s).unwrap()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => {
            tracing::warn!(error = %e, path = %phase_file.display(), "failed to read phase file");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn parses_known_phases() {
        assert_eq!(Phase::from_str("building").unwrap(), Phase::Building);
        assert_eq!(Phase::from_str("done\n").unwrap(), Phase::Done);
        assert!(
            Phase::from_str("done")
                .unwrap()
                .is_terminal()
        );
        assert!(
            Phase::from_str("error")
                .unwrap()
                .is_terminal()
        );
        assert!(
            !Phase::from_str("running")
                .unwrap()
                .is_terminal()
        );
    }

    #[test]
    fn unknown_phase_is_preserved() {
        let p = Phase::from_str("future-phase").unwrap();
        assert_eq!(p, Phase::Other("future-phase".into()));
        assert_eq!(p.label(), "future-phase");
    }

    #[test]
    fn read_missing_file_returns_none() {
        let dir = TempDir::new().unwrap();
        assert!(read(&dir.path().join(".phase")).is_none());
    }

    #[test]
    fn read_existing_file_returns_phase() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join(".phase");
        std::fs::write(&p, "running\n").unwrap();
        assert_eq!(read(&p), Some(Phase::Running));
    }
}
