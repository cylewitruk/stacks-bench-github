use std::collections::BTreeSet;

use anyhow::Context;
use sbgh_fleet::ResourceFacts;

#[cfg(target_os = "linux")]
const LINUX_MEMINFO: &str = "/proc/meminfo";
#[cfg(target_os = "linux")]
const LINUX_ONLINE_CPUS: &str = "/sys/devices/system/cpu/online";
#[cfg(any(target_os = "linux", test))]
const BYTES_PER_KIB: u64 = 1024;
const MAX_CPU_ID: u32 = 65_535;

/// Host-wide execution capacity plus the exact online CPU identifiers used to
/// validate worker-owned VM placement.
///
/// The worker adapter may itself be confined to housekeeping CPUs. Its process
/// affinity therefore cannot describe the capacity available to libvirt VMs
/// pinned onto isolated CPUs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostResources {
    facts: ResourceFacts,
    online_cpus: BTreeSet<u32>,
}

impl HostResources {
    pub fn new(online_cpus: BTreeSet<u32>, memory_bytes: u64) -> anyhow::Result<Self> {
        anyhow::ensure!(!online_cpus.is_empty(), "online CPU set must not be empty");
        anyhow::ensure!(memory_bytes > 0, "host memory must be non-zero");
        let logical_cpus = online_cpus
            .len()
            .try_into()
            .context("online CPU count does not fit the fleet protocol")?;
        Ok(Self {
            facts: ResourceFacts { logical_cpus, memory_bytes },
            online_cpus,
        })
    }

    pub fn facts(&self) -> &ResourceFacts {
        &self.facts
    }

    pub fn online_cpus(&self) -> &BTreeSet<u32> {
        &self.online_cpus
    }
}

/// Discover host-wide online VM capacity.
///
/// Fleet workers are Linux/libvirt hosts. Keeping this probe here makes host
/// discovery a worker concern while the wire contract remains a plain value.
pub fn discover_host_resources() -> anyhow::Result<HostResources> {
    HostResources::new(discover_online_cpus()?, discover_memory_bytes()?)
}

#[cfg(target_os = "linux")]
fn discover_online_cpus() -> anyhow::Result<BTreeSet<u32>> {
    let online =
        std::fs::read_to_string(LINUX_ONLINE_CPUS).context("reading Linux online CPU set")?;
    parse_cpu_list(&online, "Linux online CPU set")
}

#[cfg(not(target_os = "linux"))]
fn discover_online_cpus() -> anyhow::Result<BTreeSet<u32>> {
    anyhow::bail!("fleet worker host discovery requires Linux")
}

pub(crate) fn parse_cpu_list(value: &str, field: &str) -> anyhow::Result<BTreeSet<u32>> {
    let mut cpus = BTreeSet::new();
    let value = value.trim();
    anyhow::ensure!(!value.is_empty(), "{field} must not be empty");
    for component in value.split(',') {
        let component = component.trim();
        anyhow::ensure!(!component.is_empty(), "{field} contains an empty component");
        let (start, end) = match component.split_once('-') {
            Some((start, end)) => {
                anyhow::ensure!(!end.contains('-'), "{field} contains an invalid range");
                (parse_cpu_id(start, field)?, parse_cpu_id(end, field)?)
            }
            None => {
                let cpu = parse_cpu_id(component, field)?;
                (cpu, cpu)
            }
        };
        anyhow::ensure!(start <= end, "{field} contains a descending range");
        for cpu in start..=end {
            anyhow::ensure!(cpus.insert(cpu), "{field} contains CPU {cpu} more than once");
        }
    }
    Ok(cpus)
}

fn parse_cpu_id(value: &str, field: &str) -> anyhow::Result<u32> {
    let cpu: u32 = value
        .trim()
        .parse()
        .with_context(|| format!("{field} contains a non-integer CPU id"))?;
    anyhow::ensure!(cpu <= MAX_CPU_ID, "{field} contains an unsupported CPU id");
    Ok(cpu)
}

#[cfg(target_os = "linux")]
fn discover_memory_bytes() -> anyhow::Result<u64> {
    let meminfo = std::fs::read_to_string(LINUX_MEMINFO).context("reading /proc/meminfo")?;
    parse_mem_total(&meminfo)
}

#[cfg(not(target_os = "linux"))]
fn discover_memory_bytes() -> anyhow::Result<u64> {
    anyhow::bail!("fleet worker host discovery requires Linux")
}

#[cfg(any(target_os = "linux", test))]
fn parse_mem_total(meminfo: &str) -> anyhow::Result<u64> {
    let mut matches = meminfo
        .lines()
        .filter_map(|line| line.strip_prefix("MemTotal:"));
    let value = matches
        .next()
        .context("MemTotal is missing from /proc/meminfo")?;
    anyhow::ensure!(matches.next().is_none(), "MemTotal occurs more than once in /proc/meminfo");

    let mut fields = value.split_whitespace();
    let kib: u64 = fields
        .next()
        .context("MemTotal has no value")?
        .parse()
        .context("MemTotal is not an integer")?;
    anyhow::ensure!(
        fields.next() == Some("kB") && fields.next().is_none(),
        "MemTotal must use the Linux kB representation"
    );
    anyhow::ensure!(kib > 0, "MemTotal must be non-zero");
    kib.checked_mul(BYTES_PER_KIB)
        .context("MemTotal overflows bytes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_linux_mem_total_as_bytes() {
        assert_eq!(
            parse_mem_total("MemFree: 1 kB\nMemTotal:       33554432 kB\n").unwrap(),
            32 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn parses_linux_cpu_lists_without_counting_process_affinity() {
        let online = parse_cpu_list("0-3,6,8-9\n", "online CPUs").unwrap();
        assert_eq!(online, BTreeSet::from([0, 1, 2, 3, 6, 8, 9]));
        assert_eq!(
            HostResources::new(online, 64 * 1024 * 1024 * 1024)
                .unwrap()
                .facts()
                .logical_cpus,
            7
        );
    }

    #[test]
    fn rejects_ambiguous_or_invalid_cpu_lists() {
        for invalid in ["", "0,,1", "3-1", "0-1-2", "0,0", "65536"] {
            assert!(parse_cpu_list(invalid, "CPU set").is_err(), "{invalid:?} unexpectedly passed");
        }
    }

    #[test]
    fn rejects_missing_duplicate_malformed_and_overflowing_memory() {
        for invalid in [
            "MemFree: 1 kB\n",
            "MemTotal: 1 kB\nMemTotal: 2 kB\n",
            "MemTotal: unknown kB\n",
            "MemTotal: 1 MB\n",
            "MemTotal: 0 kB\n",
            "MemTotal: 18014398509481984 kB\n",
        ] {
            assert!(parse_mem_total(invalid).is_err(), "{invalid:?} unexpectedly passed");
        }
    }
}
