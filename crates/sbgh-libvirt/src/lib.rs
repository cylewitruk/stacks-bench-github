//! Concrete libvirt execution adapter.

mod bench_progress;
mod config;
mod fingerprint;
mod libvirt;

pub use config::{
    BlockDatasetConfig, BlockValidationProfile, LibvirtConfig, LvmConfig, PathsConfig, VmConfig,
};
pub use libvirt::driver::{LibvirtDriver, current_cache_environment};
#[cfg(any(test, feature = "testing"))]
pub use libvirt::forensics::SQLITE_RELATIVE;
#[cfg(any(test, feature = "testing"))]
pub use libvirt::shell::test_support as shell_test_support;
pub use libvirt::shell::{CommandSpec, Shell, SystemShell, spec as command_spec};
