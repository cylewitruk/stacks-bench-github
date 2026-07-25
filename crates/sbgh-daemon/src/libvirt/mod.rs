//! libvirt-driven benchmark runner. Top of this module is intentionally light;
//! see `driver.rs` for the end-to-end flow.

pub mod boot;
pub mod cloudinit;
pub mod domain;
pub mod driver;
pub mod forensics;
pub mod git_mirror;
pub mod lvm;
pub mod phase;
pub mod progress;
pub mod shell;
pub mod source;
pub mod tmpfs;
pub mod virsh;

#[allow(unused_imports)]
pub use driver::{
    BenchmarkOutcome, LibvirtConfig, LibvirtDriver, NoopPhaseListener, OutcomeStatus, PhaseListener,
};
pub use phase::format_elapsed;
pub use shell::{Shell, SystemShell};
