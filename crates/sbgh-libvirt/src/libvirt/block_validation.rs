//! Typed block-validation guest plan and fail-closed result reducer.

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, ensure};
use sbgh_driver::{
    BlockValidationOutput, BlockValidationTaskSpec, DatasetIdentity, InclusiveRange, InvalidBlock,
    ValidationEpoch,
};
use serde::{Deserialize, Serialize};

use crate::libvirt::guest_file;
use crate::libvirt::lvm::ChainstateSnapshotSet;

pub const RESULT_SCHEMA_VERSION: u32 = 1;
const MAX_RESULT_MANIFEST_BYTES: u64 = 1024 * 1024;
const MAX_SHARD_DIAGNOSTIC_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BlockGuestPlan {
    pub schema_version: u32,
    pub job_id: String,
    pub attempt_id: String,
    pub fencing_generation: u64,
    pub commit: String,
    pub dataset: PlanDataset,
    pub epoch: &'static str,
    pub range: PlanRange,
    pub requested_shards: u32,
    pub max_concurrency: u32,
    pub timeout_secs: u64,
    pub mount_options: Vec<String>,
    pub devices: Vec<PlanDevice>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDataset {
    pub generation: String,
    pub network: String,
    pub format_version: String,
    pub covered_start: u64,
    pub covered_end: u64,
    pub manifest_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlanDevice {
    pub shard: u32,
    pub serial: String,
    pub mountpoint: String,
}

impl BlockGuestPlan {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        job_id: &str,
        attempt_id: &str,
        fencing_generation: u64,
        commit: &str,
        spec: &BlockValidationTaskSpec,
        snapshots: &ChainstateSnapshotSet,
        mount_options: Vec<String>,
    ) -> anyhow::Result<Self> {
        ensure!(
            snapshots.snapshots.len() == spec.requested_shards as usize,
            "snapshot count does not match requested shard count"
        );
        Ok(Self {
            schema_version: RESULT_SCHEMA_VERSION,
            job_id: job_id.into(),
            attempt_id: attempt_id.into(),
            fencing_generation,
            commit: commit.into(),
            dataset: PlanDataset::from(&spec.dataset),
            epoch: match spec.epoch {
                ValidationEpoch::PreNakamoto => "pre_nakamoto",
                ValidationEpoch::Nakamoto => "nakamoto",
            },
            range: PlanRange::from(spec.range),
            requested_shards: spec.requested_shards,
            max_concurrency: spec.max_concurrency,
            timeout_secs: spec.timeout_secs,
            mount_options,
            devices: snapshots
                .snapshots
                .iter()
                .map(|snapshot| PlanDevice {
                    shard: snapshot.shard,
                    serial: snapshot.serial.clone(),
                    mountpoint: format!("/var/lib/sbgh-chainstate/shard-{:04}", snapshot.shard),
                })
                .collect(),
        })
    }
}

impl From<&DatasetIdentity> for PlanDataset {
    fn from(value: &DatasetIdentity) -> Self {
        Self {
            generation: value.generation.clone(),
            network: value.network.clone(),
            format_version: value.format_version.clone(),
            covered_start: value.covered_start,
            covered_end: value.covered_end,
            manifest_sha256: value.manifest_sha256.clone(),
        }
    }
}

impl From<InclusiveRange> for PlanRange {
    fn from(value: InclusiveRange) -> Self {
        Self {
            start: value.start,
            end: value.end,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GuestResult {
    schema_version: u32,
    job_id: String,
    attempt_id: String,
    fencing_generation: u64,
    dataset: PlanDataset,
    epoch: String,
    requested_range: PlanRange,
    shards: Vec<GuestShard>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct GuestShard {
    index: u32,
    start: u64,
    end: u64,
    exit_code: i32,
    stdout_file: String,
    stderr_file: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReducedBlockResult {
    pub output: BlockValidationOutput,
    pub artifacts: Vec<PathBuf>,
}

pub fn reduce_result(
    result_path: &Path,
    results_dir: &Path,
    plan: &BlockGuestPlan,
) -> anyhow::Result<ReducedBlockResult> {
    guest_file::verify_beneath(results_dir, result_path)
        .context("guest result manifest is not a safe regular file")?;
    let bytes = guest_file::read_bounded(result_path, MAX_RESULT_MANIFEST_BYTES)
        .with_context(|| format!("reading guest result {}", result_path.display()))?;
    let result: GuestResult =
        serde_json::from_slice(&bytes).context("parsing block-validation guest result")?;
    ensure!(result.schema_version == RESULT_SCHEMA_VERSION, "unsupported guest result schema");
    ensure!(
        result.job_id == plan.job_id
            && result.attempt_id == plan.attempt_id
            && result.fencing_generation == plan.fencing_generation,
        "guest result attempt identity mismatch"
    );
    ensure!(
        result.dataset == plan.dataset
            && result.epoch == plan.epoch
            && result.requested_range == plan.range,
        "guest result task identity mismatch"
    );
    // Coverage is anchored to the host-authored plan. The guest may translate
    // global Nakamoto indices into epoch-local CLI arguments, but terminal
    // accounting always remains in the trusted global coordinate system.
    let expected = partition(
        InclusiveRange {
            start: plan.range.start,
            end: plan.range.end,
        },
        plan.requested_shards,
    )?;
    ensure!(result.shards.len() == expected.len(), "guest result is partial");
    let mut shards = result.shards;
    shards.sort_by_key(|shard| shard.index);
    let mut invalid_blocks = Vec::new();
    let mut artifacts = Vec::with_capacity(shards.len() * 2 + 2);
    let mut checked_blocks = 0_u64;
    let mut negative = false;
    for (shard, expected) in shards.iter().zip(&expected) {
        ensure!(
            shard.index == expected.index
                && shard.start == expected.start
                && shard.end == expected.end,
            "guest shard ranges are not an exact, gap-free partition"
        );
        let stdout = safe_result_file(results_dir, &shard.stdout_file)?;
        let stderr = safe_result_file(results_dir, &shard.stderr_file)?;
        let stdout_text = guest_file::read_to_string_bounded(&stdout, MAX_SHARD_DIAGNOSTIC_BYTES)
            .with_context(|| format!("reading shard stdout {}", stdout.display()))?;
        let stderr_text = guest_file::read_to_string_bounded(&stderr, MAX_SHARD_DIAGNOSTIC_BYTES)
            .with_context(|| format!("reading shard stderr {}", stderr.display()))?;
        match shard.exit_code {
            0 => {}
            1 if explicit_validation_failure(&stdout_text, &stderr_text) => {
                let parsed = parse_invalid_blocks(shard.index, &stdout_text, &stderr_text);
                ensure!(
                    !parsed.is_empty(),
                    "negative shard {} reported no typed invalid-block details",
                    shard.index
                );
                negative = true;
                invalid_blocks.extend(parsed);
            }
            code => anyhow::bail!(
                "stacks-inspect shard {} infrastructure failure (exit={code}): stdout={} stderr={}",
                shard.index,
                stdout_text.trim(),
                stderr_text.trim()
            ),
        }
        checked_blocks = checked_blocks
            .checked_add(shard.end - shard.start + 1)
            .context("checked-block count overflow")?;
        artifacts.push(stdout);
        artifacts.push(stderr);
    }
    ensure!(
        !negative || !invalid_blocks.is_empty(),
        "negative result has no typed invalid-block diagnostics"
    );
    artifacts.push(result_path.to_path_buf());
    let invalid_path = results_dir.join("invalid-blocks.json");
    // The VM is stopped before reduction. Remove a guest-created terminal
    // file or symlink by directory entry (never follow it), then create the
    // authoritative host-owned artifact exclusively.
    match std::fs::symlink_metadata(&invalid_path) {
        Ok(metadata)
            if metadata.is_file()
                || metadata
                    .file_type()
                    .is_symlink() =>
        {
            std::fs::remove_file(&invalid_path)
                .context("removing guest-created invalid-block diagnostics entry")?;
        }
        Ok(_) => anyhow::bail!("guest-created invalid-block diagnostics entry is not a file"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).context("inspecting invalid-block diagnostics destination");
        }
    }
    let mut invalid_file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&invalid_path)
        .context("creating host-owned invalid-block diagnostics")?;
    invalid_file.write_all(&serde_json::to_vec_pretty(&invalid_blocks)?)?;
    artifacts.push(invalid_path);
    Ok(ReducedBlockResult {
        output: BlockValidationOutput {
            valid: !negative,
            checked_blocks,
            invalid_blocks,
            dataset: DatasetIdentity {
                generation: plan
                    .dataset
                    .generation
                    .clone(),
                network: plan.dataset.network.clone(),
                format_version: plan
                    .dataset
                    .format_version
                    .clone(),
                covered_start: plan.dataset.covered_start,
                covered_end: plan.dataset.covered_end,
                manifest_sha256: plan
                    .dataset
                    .manifest_sha256
                    .clone(),
            },
        },
        artifacts,
    })
}

fn safe_result_file(root: &Path, name: &str) -> anyhow::Result<PathBuf> {
    ensure!(
        !name.is_empty()
            && !name.contains('/')
            && !name.contains('\\')
            && name != "."
            && name != "..",
        "guest result contains an unsafe artifact filename"
    );
    let path = root.join(name);
    guest_file::verify_beneath(root, &path).with_context(|| {
        format!("guest artifact is not a safe regular file: {}", path.display())
    })?;
    Ok(path)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRange {
    pub index: u32,
    pub start: u64,
    pub end: u64,
}

pub fn partition(range: InclusiveRange, requested_shards: u32) -> anyhow::Result<Vec<ShardRange>> {
    ensure!(range.start <= range.end, "validation range is reversed");
    ensure!(requested_shards > 0, "requested_shards must be non-zero");
    let count = u128::from(range.end) - u128::from(range.start) + 1;
    let shard_count = u128::from(requested_shards).min(count);
    let base = count / shard_count;
    let remainder = count % shard_count;
    let mut cursor = u128::from(range.start);
    let mut ranges = Vec::with_capacity(shard_count as usize);
    for index in 0..shard_count {
        let length = base + u128::from(index < remainder);
        let end = cursor + length - 1;
        ranges.push(ShardRange {
            index: index as u32,
            start: cursor as u64,
            end: end as u64,
        });
        cursor = end + 1;
    }
    ensure!(cursor == u128::from(range.end) + 1, "partition did not exactly cover range");
    Ok(ranges)
}

fn explicit_validation_failure(stdout: &str, stderr: &str) -> bool {
    stdout
        .lines()
        .chain(stderr.lines())
        .any(|line| line.contains("Failed processing block"))
}

fn parse_invalid_blocks(shard: u32, stdout: &str, stderr: &str) -> Vec<InvalidBlock> {
    stdout
        .lines()
        .chain(stderr.lines())
        .filter_map(|line| parse_invalid_block_line(shard, line))
        .collect()
}

fn parse_invalid_block_line(shard: u32, line: &str) -> Option<InvalidBlock> {
    let (_, rest) = line
        .trim()
        .split_once("Failed processing block")?;
    let rest = rest.trim_start_matches([':', ' ', '#']);
    if rest.is_empty() {
        return None;
    }
    let (block, reason) = if let Some(canonical) = rest.strip_prefix("! block = ") {
        if let Some((block, error)) = canonical.split_once(", error = ") {
            (block, error.trim().to_owned())
        } else if let Some((block, detail)) = canonical.split_once(". Unexpected cost.") {
            (block, format!("Unexpected cost. {}", detail.trim()))
        } else {
            (canonical, "block validation failed".into())
        }
    } else {
        rest.split_once(':')
            .map_or((rest, "block validation failed".into()), |(block, reason)| {
                (block, reason.trim().to_owned())
            })
    };
    let block = block
        .trim()
        .trim_end_matches([',', '.']);
    (!block.is_empty()).then(|| InvalidBlock {
        shard,
        block: block.into(),
        reason,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::libvirt::lvm::ShardSnapshot;
    use tempfile::TempDir;

    fn plan() -> BlockGuestPlan {
        let spec = BlockValidationTaskSpec {
            dataset: DatasetIdentity {
                generation: "g1".into(),
                network: "mainnet".into(),
                format_version: "v3".into(),
                covered_start: 0,
                covered_end: 100,
                manifest_sha256: "a".repeat(64),
            },
            epoch: ValidationEpoch::PreNakamoto,
            range: InclusiveRange { start: 10, end: 12 },
            requested_shards: 2,
            max_concurrency: 2,
            timeout_secs: 60,
        };
        let snapshots = ChainstateSnapshotSet {
            origin: "vg/origin".into(),
            snapshots: (0..2)
                .map(|shard| ShardSnapshot {
                    shard,
                    vg: "vg".into(),
                    name: format!("snapshot-{shard}"),
                    device: PathBuf::from(format!("/dev/vg/snapshot-{shard}")),
                    serial: format!("sbgh-block-{shard:04}"),
                })
                .collect(),
        };
        BlockGuestPlan::new(
            "job",
            "attempt",
            4,
            "deadbeef",
            &spec,
            &snapshots,
            vec!["nouuid".into()],
        )
        .unwrap()
    }

    fn write_result(directory: &TempDir, shards: serde_json::Value) -> PathBuf {
        let result = directory
            .path()
            .join("block-validation-result.json");
        std::fs::write(
            &result,
            serde_json::to_vec(&serde_json::json!({
                "schema_version": 1,
                "job_id": "job",
                "attempt_id": "attempt",
                "fencing_generation": 4,
                "dataset": {
                    "generation": "g1",
                    "network": "mainnet",
                    "format_version": "v3",
                    "covered_start": 0,
                    "covered_end": 100,
                    "manifest_sha256": "a".repeat(64),
                },
                "epoch": "pre_nakamoto",
                "requested_range": {"start": 10, "end": 12},
                "shards": shards,
            }))
            .unwrap(),
        )
        .unwrap();
        result
    }

    fn shard(index: u32, start: u64, end: u64, exit_code: i32) -> serde_json::Value {
        serde_json::json!({
            "index": index,
            "start": start,
            "end": end,
            "exit_code": exit_code,
            "stdout_file": format!("shard-{index}.stdout.log"),
            "stderr_file": format!("shard-{index}.stderr.log"),
        })
    }

    #[test]
    fn partition_is_ordered_nonempty_and_exact() {
        assert_eq!(
            partition(InclusiveRange { start: 10, end: 20 }, 4).unwrap(),
            vec![
                ShardRange { index: 0, start: 10, end: 12 },
                ShardRange { index: 1, start: 13, end: 15 },
                ShardRange { index: 2, start: 16, end: 18 },
                ShardRange { index: 3, start: 19, end: 20 },
            ]
        );
    }

    #[test]
    fn partition_handles_u64_boundary_without_overflow() {
        let ranges = partition(
            InclusiveRange {
                start: u64::MAX - 1,
                end: u64::MAX,
            },
            8,
        )
        .unwrap();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[1].end, u64::MAX);
    }

    #[test]
    fn partition_properties_hold_across_small_ranges_and_shard_counts() {
        for start in 0..32_u64 {
            for end in start..64 {
                for requested_shards in 1..80 {
                    let ranges =
                        partition(InclusiveRange { start, end }, requested_shards).unwrap();
                    assert_eq!(ranges.first().unwrap().start, start);
                    assert_eq!(ranges.last().unwrap().end, end);
                    assert_eq!(
                        ranges.len(),
                        usize::try_from((end - start + 1).min(u64::from(requested_shards)))
                            .unwrap()
                    );
                    for (index, range) in ranges.iter().enumerate() {
                        assert_eq!(range.index, index as u32);
                        assert!(range.start <= range.end);
                        if let Some(next) = ranges.get(index + 1) {
                            assert_eq!(range.end.checked_add(1), Some(next.start));
                        }
                    }
                    let covered: u128 = ranges
                        .iter()
                        .map(|range| u128::from(range.end - range.start + 1))
                        .sum();
                    assert_eq!(covered, u128::from(end - start + 1));
                }
            }
        }
    }

    #[test]
    fn canonical_invalid_block_diagnostic_is_typed() {
        let invalid = parse_invalid_blocks(
            7,
            "Block outer: Failed processing block! block = ccdd, error = Invalid parent",
            "",
        );
        assert_eq!(
            invalid,
            vec![InvalidBlock {
                shard: 7,
                block: "ccdd".into(),
                reason: "Invalid parent".into(),
            }]
        );
    }

    #[test]
    fn exit_one_requires_the_canonical_failure_marker() {
        assert!(explicit_validation_failure(
            "Block outer: Failed processing block! block = ccdd, error = Invalid parent",
            ""
        ));
        for assumed_or_infrastructure_message in [
            "database could not be opened",
            "Validation completed with 2 error(s)",
            "block validation failed: invalid parent",
        ] {
            assert!(
                !explicit_validation_failure(assumed_or_infrastructure_message, ""),
                "non-canonical output must remain infrastructure failure: \
                 {assumed_or_infrastructure_message}"
            );
        }
    }

    #[test]
    fn reducer_accepts_exact_positive_coverage() {
        let directory = TempDir::new().unwrap();
        std::fs::write(
            directory
                .path()
                .join("invalid-blocks.json"),
            "guest-controlled",
        )
        .unwrap();
        for index in 0..2 {
            std::fs::write(
                directory
                    .path()
                    .join(format!("shard-{index}.stdout.log")),
                "validation complete\n",
            )
            .unwrap();
            std::fs::write(
                directory
                    .path()
                    .join(format!("shard-{index}.stderr.log")),
                "",
            )
            .unwrap();
        }
        let result =
            write_result(&directory, serde_json::json!([shard(0, 10, 11, 0), shard(1, 12, 12, 0)]));
        let reduced = reduce_result(&result, directory.path(), &plan()).unwrap();
        assert!(reduced.output.valid);
        assert_eq!(reduced.output.checked_blocks, 3);
        assert!(
            reduced
                .output
                .invalid_blocks
                .is_empty()
        );
        assert_eq!(
            std::fs::read_to_string(
                directory
                    .path()
                    .join("invalid-blocks.json")
            )
            .unwrap(),
            "[]"
        );
    }

    #[test]
    fn reducer_accepts_only_an_explicit_typed_negative() {
        let directory = TempDir::new().unwrap();
        std::fs::write(
            directory
                .path()
                .join("shard-0.stdout.log"),
            "ok\n",
        )
        .unwrap();
        std::fs::write(
            directory
                .path()
                .join("shard-0.stderr.log"),
            "",
        )
        .unwrap();
        std::fs::write(
            directory
                .path()
                .join("shard-1.stdout.log"),
            "Block outer: Failed processing block! block = aabb, error = bad parent\n",
        )
        .unwrap();
        std::fs::write(
            directory
                .path()
                .join("shard-1.stderr.log"),
            "",
        )
        .unwrap();
        let result =
            write_result(&directory, serde_json::json!([shard(0, 10, 11, 0), shard(1, 12, 12, 1)]));
        let reduced = reduce_result(&result, directory.path(), &plan()).unwrap();
        assert!(!reduced.output.valid);
        assert_eq!(
            reduced
                .output
                .invalid_blocks
                .len(),
            1
        );
        assert_eq!(reduced.output.invalid_blocks[0].block, "aabb");
    }

    #[test]
    fn reducer_rejects_partial_or_infrastructure_results() {
        let directory = TempDir::new().unwrap();
        std::fs::write(
            directory
                .path()
                .join("shard-0.stdout.log"),
            "ok\n",
        )
        .unwrap();
        std::fs::write(
            directory
                .path()
                .join("shard-0.stderr.log"),
            "",
        )
        .unwrap();
        let partial = write_result(&directory, serde_json::json!([shard(0, 10, 11, 0)]));
        assert!(
            reduce_result(&partial, directory.path(), &plan())
                .unwrap_err()
                .to_string()
                .contains("partial")
        );

        std::fs::write(
            directory
                .path()
                .join("shard-1.stdout.log"),
            "",
        )
        .unwrap();
        std::fs::write(
            directory
                .path()
                .join("shard-1.stderr.log"),
            "database could not be opened\n",
        )
        .unwrap();
        let infrastructure =
            write_result(&directory, serde_json::json!([shard(0, 10, 11, 0), shard(1, 12, 12, 1)]));
        assert!(
            format!("{:#}", reduce_result(&infrastructure, directory.path(), &plan()).unwrap_err())
                .contains("infrastructure failure")
        );
    }

    #[test]
    fn reducer_rejects_guest_reported_undercoverage() {
        let directory = TempDir::new().unwrap();
        std::fs::write(
            directory
                .path()
                .join("shard-0.stdout.log"),
            "ok\n",
        )
        .unwrap();
        std::fs::write(
            directory
                .path()
                .join("shard-0.stderr.log"),
            "",
        )
        .unwrap();
        let result = write_result(&directory, serde_json::json!([shard(0, 10, 10, 0)]));

        let error = reduce_result(&result, directory.path(), &plan()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("partial")
                || error
                    .to_string()
                    .contains("exact, gap-free partition"),
            "{error:#}"
        );
    }

    #[test]
    fn reducer_rejects_stale_attempt_identity_before_reading_artifacts() {
        let directory = TempDir::new().unwrap();
        let result =
            write_result(&directory, serde_json::json!([shard(0, 10, 11, 0), shard(1, 12, 12, 0)]));
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&result).unwrap()).unwrap();
        value["attempt_id"] = serde_json::json!("stale-attempt");
        std::fs::write(&result, serde_json::to_vec(&value).unwrap()).unwrap();

        assert!(
            reduce_result(&result, directory.path(), &plan())
                .unwrap_err()
                .to_string()
                .contains("attempt identity mismatch")
        );
    }
}
