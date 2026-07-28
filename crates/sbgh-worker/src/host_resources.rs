use anyhow::Context;
use sbgh_proto::ResourceFacts;

#[cfg(target_os = "linux")]
const LINUX_MEMINFO: &str = "/proc/meminfo";
#[cfg(any(target_os = "linux", test))]
const BYTES_PER_KIB: u64 = 1024;

/// Discover the capacity visible to this worker process.
///
/// Fleet workers are Linux/libvirt hosts. Keeping this probe here makes host
/// discovery a worker concern while the wire contract remains a plain value.
pub fn discover_host_resources() -> anyhow::Result<ResourceFacts> {
    let logical_cpus = std::thread::available_parallelism()
        .context("discovering available logical CPUs")?
        .get()
        .try_into()
        .context("logical CPU count does not fit the fleet protocol")?;

    Ok(ResourceFacts {
        logical_cpus,
        memory_bytes: discover_memory_bytes()?,
    })
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
