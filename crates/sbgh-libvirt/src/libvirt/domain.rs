//! Build a libvirt domain XML programmatically with `quick-xml`.
//!
//! Output shape (abridged):
//!
//! ```text
//!   <domain type='kvm'>
//!     <name/>, <memory/>, <vcpu/>, <os/>, <features/>, <cpu/>, ...
//!     <memoryBacking><source type='memfd'/><access
//! mode='shared'/></memoryBacking>     <devices>
//!       <disk vda boot.qcow2 qcow2>
//!       <disk vdb /dev/.../chainstate raw block>
//!       <disk vdc source.raw raw>
//!       <disk sda cidata.iso cdrom readonly>
//!       <filesystem virtiofs results_share_dir -> tag>
//!       <interface type='network' source=network/>
//!       <serial/console type='file' path=console.log>
//!       <channel virtio org.qemu.guest_agent.0>
//!     </devices>
//!   </domain>
//! ```

use std::io::Cursor;
use std::path::Path;

use quick_xml::Writer;
use quick_xml::events::*;

pub struct DomainSpec<'a> {
    pub name: &'a str,
    /// Stable per-job UUID. MUST be identical across both phases of a
    /// job's two-VM lifecycle — `virsh define` only treats the second
    /// call as an update-in-place when the XML carries the same UUID
    /// as the existing inactive domain definition. Without this,
    /// libvirt auto-generates a fresh UUID on each render and the
    /// second define fails with "domain '<name>' already exists with
    /// uuid <previous>". The job UUID itself is a natural fit since
    /// it's already a uniquely-allocated UUID per job.
    pub uuid: &'a str,
    /// vCPUs allocated to the VM. Different per phase — build typically
    /// gets more for parallel cargo, bench gets fewer to match
    /// production deployment shape.
    pub vcpus: u32,
    /// Memory allocated to the VM, in bytes. Set per render call from
    /// `benchmark.build_memory_bytes` or `benchmark.bench_memory_bytes`.
    /// We emit `<memory unit='KiB'>` because libvirt's smallest
    /// guaranteed-supported unit is KiB; bytes would work on modern
    /// libvirt but KiB is the safe portable choice.
    pub memory_bytes: u64,
    pub boot_disk_path: &'a Path,
    /// The benchmark's behavior-preserving single virtio chainstate disk.
    pub chainstate_dev_path: Option<&'a Path>,
    /// Additional raw devices. Block validation uses one virtio-scsi disk per
    /// shard with an explicit stable serial.
    pub block_devices: &'a [BlockDeviceSpec<'a>],
    pub source_disk_path: &'a Path,
    pub cidata_iso_path: &'a Path,
    pub results_share_dir: &'a Path,
    pub results_share_tag: &'a str,
    pub console_log_path: &'a Path,
    pub network: &'a str,
    /// Optional CPU pinning (roadmap-v5 Phase 5): the libvirt cpuset this VM's
    /// vCPUs are pinned to (e.g. `"0-1"`), from `[runner].cpu_sets[slot]`.
    /// `None` → vCPUs float (no `<vcpu cpuset=…>` / `<cputune>`).
    pub vcpu_cpuset: Option<&'a str>,
    /// Optional host cpuset for the qemu emulator/I-O threads (`[runner].
    /// host_cpus`), pinned off the bench cores. Only emitted when `vcpu_cpuset`
    /// is set.
    pub emulator_cpuset: Option<&'a str>,
}

pub struct BlockDeviceSpec<'a> {
    pub path: &'a Path,
    pub target_dev: &'a str,
    pub serial: &'a str,
}

pub fn render(spec: &DomainSpec<'_>) -> anyhow::Result<String> {
    let mut w = Writer::new_with_indent(Cursor::new(Vec::new()), b' ', 2);

    w.create_element("domain")
        .with_attribute(("type", "kvm"))
        .write_inner_content(|w| {
            text(w, "name", spec.name)?;
            text(w, "uuid", spec.uuid)?;
            // libvirt rounds memory down to its smallest unit; KiB is
            // universally supported. Divide-by-1024 is safe (memory is
            // always allocated in page-multiples upstream).
            let memory_kib = spec.memory_bytes / 1024;
            w.create_element("memory")
                .with_attribute(("unit", "KiB"))
                .write_text_content(BytesText::new(&memory_kib.to_string()))?;

            // `<vcpu>` — pinned (`placement='static' cpuset='…'`) when a slot
            // cpuset is configured, else floating. `<cputune><emulatorpin>`
            // keeps the qemu emulator/I-O threads off the bench cores.
            let vcpus = spec.vcpus.to_string();
            match spec.vcpu_cpuset {
                Some(cpuset) => {
                    w.create_element("vcpu")
                        .with_attribute(("placement", "static"))
                        .with_attribute(("cpuset", cpuset))
                        .write_text_content(BytesText::new(&vcpus))?;
                }
                None => text(w, "vcpu", &vcpus)?,
            }
            if let (Some(_), Some(emulator)) = (spec.vcpu_cpuset, spec.emulator_cpuset) {
                w.create_element("cputune")
                    .write_inner_content(|w| {
                        empty(w, "emulatorpin", &[("cpuset", emulator)])?;
                        Ok(())
                    })?;
            }

            w.create_element("os")
                .write_inner_content(|w| {
                    w.create_element("type")
                        .with_attribute(("arch", "x86_64"))
                        .with_attribute(("machine", "q35"))
                        .write_text_content(BytesText::new("hvm"))?;
                    empty(w, "boot", &[("dev", "hd")])?;
                    Ok(())
                })?;

            w.create_element("features")
                .write_inner_content(|w| {
                    empty(w, "acpi", &[])?;
                    empty(w, "apic", &[])?;
                    Ok(())
                })?;

            empty(w, "cpu", &[("mode", "host-passthrough"), ("check", "none")])?;
            empty(w, "clock", &[("offset", "utc")])?;

            text(w, "on_poweroff", "destroy")?;
            text(w, "on_reboot", "destroy")?;
            text(w, "on_crash", "destroy")?;

            // memoryBacking is required for virtio-fs (shared memory access).
            w.create_element("memoryBacking")
                .write_inner_content(|w| {
                    empty(w, "source", &[("type", "memfd")])?;
                    empty(w, "access", &[("mode", "shared")])?;
                    Ok(())
                })?;

            w.create_element("devices")
                .write_inner_content(|w| {
                    emulator(w, "/usr/bin/qemu-system-x86_64")?;

                    file_disk(w, spec.boot_disk_path, "qcow2", "vda", false)?;
                    if let Some(chainstate) = spec.chainstate_dev_path {
                        block_disk(w, chainstate, "vdb")?;
                    }
                    if !spec.block_devices.is_empty() {
                        empty(
                            w,
                            "controller",
                            &[("type", "scsi"), ("model", "virtio-scsi"), ("index", "0")],
                        )?;
                        for device in spec.block_devices {
                            scsi_block_disk(w, device)?;
                        }
                    }
                    file_disk(w, spec.source_disk_path, "raw", "vdc", false)?;
                    file_disk(w, spec.cidata_iso_path, "raw", "sda", true)?;

                    virtiofs_filesystem(w, spec.results_share_dir, spec.results_share_tag)?;
                    interface_network(w, spec.network)?;
                    serial_console_file(w, spec.console_log_path)?;
                    guest_agent_channel(w)?;
                    Ok(())
                })?;

            Ok(())
        })?;

    let bytes = w.into_inner().into_inner();
    Ok(String::from_utf8(bytes)?)
}

// ─────────────────────────── element helpers ───────────────────────────

type W = Writer<Cursor<Vec<u8>>>;

fn text(w: &mut W, name: &str, content: &str) -> std::io::Result<()> {
    w.create_element(name)
        .write_text_content(BytesText::new(content))?;
    Ok(())
}

fn empty(w: &mut W, name: &str, attrs: &[(&str, &str)]) -> std::io::Result<()> {
    let mut el = w.create_element(name);
    for (k, v) in attrs {
        el = el.with_attribute((*k, *v));
    }
    el.write_empty()?;
    Ok(())
}

fn emulator(w: &mut W, path: &str) -> std::io::Result<()> {
    w.create_element("emulator")
        .write_text_content(BytesText::new(path))?;
    Ok(())
}

fn file_disk(
    w: &mut W,
    path: &Path,
    driver_type: &str,
    target_dev: &str,
    cdrom: bool,
) -> std::io::Result<()> {
    let device = if cdrom { "cdrom" } else { "disk" };
    let bus = if cdrom { "sata" } else { "virtio" };
    let mut disk = w.create_element("disk");
    disk = disk
        .with_attribute(("type", "file"))
        .with_attribute(("device", device));
    disk.write_inner_content(|w| {
        empty(w, "driver", &[("name", "qemu"), ("type", driver_type)])?;
        empty(w, "source", &[("file", &path.display().to_string())])?;
        empty(w, "target", &[("dev", target_dev), ("bus", bus)])?;
        if cdrom {
            empty(w, "readonly", &[])?;
        }
        Ok(())
    })?;
    Ok(())
}

fn block_disk(w: &mut W, dev: &Path, target_dev: &str) -> std::io::Result<()> {
    w.create_element("disk")
        .with_attribute(("type", "block"))
        .with_attribute(("device", "disk"))
        .write_inner_content(|w| {
            empty(
                w,
                "driver",
                &[("name", "qemu"), ("type", "raw"), ("cache", "none"), ("io", "native")],
            )?;
            empty(w, "source", &[("dev", &dev.display().to_string())])?;
            empty(w, "target", &[("dev", target_dev), ("bus", "virtio")])?;
            Ok(())
        })?;
    Ok(())
}

fn scsi_block_disk(w: &mut W, spec: &BlockDeviceSpec<'_>) -> std::io::Result<()> {
    w.create_element("disk")
        .with_attribute(("type", "block"))
        .with_attribute(("device", "disk"))
        .write_inner_content(|w| {
            empty(
                w,
                "driver",
                &[("name", "qemu"), ("type", "raw"), ("cache", "none"), ("io", "native")],
            )?;
            empty(
                w,
                "source",
                &[(
                    "dev",
                    &spec
                        .path
                        .display()
                        .to_string(),
                )],
            )?;
            empty(w, "target", &[("dev", spec.target_dev), ("bus", "scsi")])?;
            text(w, "serial", spec.serial)?;
            Ok(())
        })?;
    Ok(())
}

fn virtiofs_filesystem(w: &mut W, dir: &Path, tag: &str) -> std::io::Result<()> {
    w.create_element("filesystem")
        .with_attribute(("type", "mount"))
        .with_attribute(("accessmode", "passthrough"))
        .write_inner_content(|w| {
            empty(w, "driver", &[("type", "virtiofs")])?;
            empty(w, "source", &[("dir", &dir.display().to_string())])?;
            empty(w, "target", &[("dir", tag)])?;
            Ok(())
        })?;
    Ok(())
}

fn interface_network(w: &mut W, network: &str) -> std::io::Result<()> {
    w.create_element("interface")
        .with_attribute(("type", "network"))
        .write_inner_content(|w| {
            empty(w, "source", &[("network", network)])?;
            empty(w, "model", &[("type", "virtio")])?;
            empty(w, "port", &[("isolated", "yes")])?;
            Ok(())
        })?;
    Ok(())
}

fn serial_console_file(w: &mut W, log: &Path) -> std::io::Result<()> {
    let log_s = log.display().to_string();
    w.create_element("serial")
        .with_attribute(("type", "file"))
        .write_inner_content(|w| {
            empty(w, "source", &[("path", &log_s), ("append", "off")])?;
            empty(w, "target", &[("type", "isa-serial"), ("port", "0")])?;
            Ok(())
        })?;
    w.create_element("console")
        .with_attribute(("type", "file"))
        .write_inner_content(|w| {
            empty(w, "source", &[("path", &log_s), ("append", "off")])?;
            empty(w, "target", &[("type", "serial"), ("port", "0")])?;
            Ok(())
        })?;
    Ok(())
}

fn guest_agent_channel(w: &mut W) -> std::io::Result<()> {
    w.create_element("channel")
        .with_attribute(("type", "unix"))
        .write_inner_content(|w| {
            empty(w, "target", &[("type", "virtio"), ("name", "org.qemu.guest_agent.0")])?;
            Ok(())
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn sample<'a>() -> DomainSpec<'a> {
        DomainSpec {
            name: "sbgh-job1",
            uuid: "11111111-2222-3333-4444-555555555555",
            vcpus: 4,
            memory_bytes: 8u64 * 1024 * 1024 * 1024, // 8 GiB
            boot_disk_path: Path::new("/var/lib/sbgh/jobs/job1/boot.qcow2"),
            chainstate_dev_path: Some(Path::new("/dev/sbgh-vg/sbgh-job1-chainstate")),
            block_devices: &[],
            source_disk_path: Path::new("/var/lib/sbgh/jobs/job1/source.raw"),
            cidata_iso_path: Path::new("/var/lib/sbgh/jobs/job1/cidata.iso"),
            results_share_dir: Path::new("/run/sbgh/jobs/job1"),
            results_share_tag: "results",
            console_log_path: Path::new("/var/lib/sbgh/jobs/job1/console.log"),
            network: "sandbox-egress",
            vcpu_cpuset: None,
            emulator_cpuset: None,
        }
    }

    #[test]
    fn renders_all_required_sections() {
        let xml = render(&sample()).unwrap();
        // UUID is pinned so two-phase redefine works (`virsh define`
        // treats same-UUID as an update-in-place rather than a name
        // collision).
        assert!(xml.contains("<uuid>11111111-2222-3333-4444-555555555555</uuid>"));
        // sanity: well-formed structure
        assert!(xml.starts_with("<domain type=\"kvm\">"));
        assert!(xml.contains("<name>sbgh-job1</name>"));
        // Memory is rendered in KiB (libvirt's smallest universally
        // supported unit). 8 GiB == 8 * 1024 * 1024 KiB.
        assert!(xml.contains("<memory unit=\"KiB\">8388608</memory>"));
        assert!(xml.contains("<vcpu>4</vcpu>"));
        // disks
        assert!(xml.contains("/var/lib/sbgh/jobs/job1/boot.qcow2"));
        assert!(xml.contains("/dev/sbgh-vg/sbgh-job1-chainstate"));
        assert!(xml.contains("/var/lib/sbgh/jobs/job1/source.raw"));
        assert!(xml.contains("/var/lib/sbgh/jobs/job1/cidata.iso"));
        // virtio-fs
        assert!(xml.contains("type=\"virtiofs\""));
        assert!(xml.contains("dir=\"results\""));
        assert!(xml.contains("/run/sbgh/jobs/job1"));
        assert_eq!(
            xml.matches("<filesystem")
                .count(),
            1
        );
        assert!(!xml.contains("sccache"));
        // memoryBacking shared (virtio-fs requirement)
        assert!(xml.contains("<memoryBacking>"));
        assert!(xml.contains("mode=\"shared\""));
        // network + console + guest agent
        assert!(xml.contains("network=\"sandbox-egress\""));
        assert!(xml.contains("<port isolated=\"yes\"/>"));
        assert!(xml.contains("/var/lib/sbgh/jobs/job1/console.log"));
        assert!(xml.contains("org.qemu.guest_agent.0"));
    }

    #[test]
    fn block_validation_renders_only_explicit_stable_scsi_snapshots() {
        let devices = [
            BlockDeviceSpec {
                path: Path::new("/dev/vg0/attempt-shard-0"),
                target_dev: "sdd",
                serial: "sbgh-block-0000",
            },
            BlockDeviceSpec {
                path: Path::new("/dev/vg0/attempt-shard-1"),
                target_dev: "sde",
                serial: "sbgh-block-0001",
            },
        ];
        let spec = DomainSpec {
            chainstate_dev_path: None,
            block_devices: &devices,
            vcpus: 48,
            memory_bytes: 192 * 1024 * 1024 * 1024,
            network: "sandbox-egress",
            vcpu_cpuset: Some("0-47"),
            emulator_cpuset: Some("48-63"),
            ..sample()
        };
        let xml = render(&spec).unwrap();
        assert!(xml.contains("model=\"virtio-scsi\""));
        assert!(xml.contains("dev=\"sdd\" bus=\"scsi\""));
        assert!(xml.contains("<serial>sbgh-block-0000</serial>"));
        assert!(xml.contains("/dev/vg0/attempt-shard-1"));
        assert!(xml.contains("network=\"sandbox-egress\""));
        assert!(xml.contains("cpuset=\"0-47\""));
        assert!(!xml.contains("sbgh-job1-chainstate"));
        assert_eq!(
            xml.matches("type=\"block\" device=\"disk\"")
                .count(),
            2
        );
    }

    /// No pinning by default: plain `<vcpu>`, no `<cputune>` (Phase 5).
    #[test]
    fn no_cpu_pinning_by_default() {
        let xml = render(&sample()).unwrap();
        assert!(xml.contains("<vcpu>4</vcpu>"), "unpinned vcpu has no cpuset");
        assert!(!xml.contains("cputune"), "no cputune block when unpinned");
        assert!(!xml.contains("cpuset"), "no cpuset attribute when unpinned");
    }

    /// With a slot cpuset + host cpus, vCPUs pin (`placement='static'
    /// cpuset='…'`) and the emulator threads pin off the bench cores. Modelled
    /// on the recommended shape: 2 vCPUs on a 2-core slot (1 vCPU per core).
    #[test]
    fn renders_cpu_pinning_when_configured() {
        let spec = DomainSpec {
            vcpus: 2,
            vcpu_cpuset: Some("0-1"),
            emulator_cpuset: Some("4-5"),
            ..sample()
        };
        let xml = render(&spec).unwrap();
        assert!(
            xml.contains("<vcpu placement=\"static\" cpuset=\"0-1\">2</vcpu>"),
            "vcpus pinned to the slot cpuset, got: {xml}",
        );
        assert!(
            xml.contains("<emulatorpin cpuset=\"4-5\"/>"),
            "emulator threads pinned to the host cores, got: {xml}",
        );
    }

    /// `emulator_cpuset` without `vcpu_cpuset` is a no-op (emulatorpin only
    /// makes sense alongside vCPU pinning).
    #[test]
    fn emulator_cpuset_alone_does_nothing() {
        let spec = DomainSpec {
            emulator_cpuset: Some("4-5"),
            ..sample()
        };
        let xml = render(&spec).unwrap();
        assert!(!xml.contains("cputune"), "no emulatorpin without vcpu pinning");
        assert!(xml.contains("<vcpu>4</vcpu>"));
    }

    #[test]
    fn xml_parses_back_with_quick_xml() {
        // Cheap sanity check that what we wrote is well-formed.
        use quick_xml::reader::Reader;
        let xml = render(&sample()).unwrap();
        let mut reader = Reader::from_str(&xml);
        loop {
            match reader.read_event() {
                Ok(quick_xml::events::Event::Eof) => break,
                Ok(_) => {}
                Err(e) => panic!("malformed XML: {e}"),
            }
        }
    }

    #[test]
    fn paths_with_special_chars_are_escaped() {
        let mut spec = sample();
        let weird: PathBuf = PathBuf::from("/var/lib/sbgh/jobs/job&id/boot.qcow2");
        spec.boot_disk_path = weird.as_path();
        let xml = render(&spec).unwrap();
        // attribute values should be XML-escaped
        assert!(xml.contains("job&amp;id"));
        assert!(!xml.contains("job&id\""));
    }
}
