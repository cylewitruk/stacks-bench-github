//! libvirt-driven benchmark runner. Top of this module is intentionally light;
//! see `driver.rs` for the end-to-end flow.

pub mod block_validation;
pub mod boot;
pub mod cloudinit;
pub mod domain;
pub mod driver;
pub mod forensics;
pub mod git_mirror;
mod guest_file;
pub mod lvm;
pub mod phase;
pub mod progress;
pub mod shell;
pub mod source;
pub mod tmpfs;
pub mod virsh;
