use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, ensure};
use sbgh_proto::{
    BlockValidationPayload, BlockValidationResult, InclusiveRange, InvalidBlock, ValidationEpoch,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::config::BlockValidationConfig;

const DATASET_MANIFEST: &str = ".sbgh-dataset-manifest.json";
const DATASET_FILE_DIGESTS: &str = ".sbgh-dataset-files.sha256";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DatasetManifest {
    generation: String,
    network: String,
    format_version: String,
    covered_start: u64,
    covered_end: u64,
    files_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRange {
    pub index: u32,
    pub start: u64,
    pub end: u64,
}

#[derive(Debug)]
struct ShardOutcome {
    index: u32,
    checked_blocks: u64,
    invalid_blocks: Vec<InvalidBlock>,
    normal_negative: bool,
    stdout: String,
    stderr: String,
}

pub struct BlockExecution {
    pub result: BlockValidationResult,
    pub artifacts: Vec<PathBuf>,
}

#[derive(Clone, Copy)]
pub struct BlockExecutionRequest<'a> {
    pub repository: &'a str,
    pub commit: &'a str,
    pub repository_token: Option<&'a str>,
    pub payload: &'a BlockValidationPayload,
    pub job_id: uuid::Uuid,
}

#[derive(Debug, Clone, Copy)]
pub struct BlockProgress {
    pub completed_shards: u64,
    pub total_shards: u64,
    pub checked_blocks: u64,
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

pub async fn execute(
    config: &BlockValidationConfig,
    request: &BlockExecutionRequest<'_>,
    cancel: &CancellationToken,
    progress: tokio::sync::mpsc::UnboundedSender<BlockProgress>,
) -> anyhow::Result<BlockExecution> {
    let BlockExecutionRequest {
        repository,
        commit,
        repository_token,
        payload,
        job_id,
    } = *request;
    let deadline = Instant::now() + Duration::from_secs(payload.timeout_secs);
    verify_dataset(config, payload).await?;
    let binary =
        ensure_binary(config, repository, commit, repository_token, cancel, deadline).await?;
    let probed_range = probe_range(
        &binary,
        &config.chain_config,
        &config.canonical_dataset,
        payload.epoch,
        &payload.range,
        cancel,
        deadline,
    )
    .await?;
    let ranges = partition(probed_range, payload.requested_shards)?;
    let attempt_root = config
        .workspace_root
        .join(job_id.to_string());
    tokio::fs::create_dir_all(&attempt_root).await?;
    let concurrency = Arc::new(Semaphore::new(
        payload
            .max_concurrency
            .min(ranges.len() as u32) as usize,
    ));
    let total_shards = ranges.len() as u64;
    let shard_cancel = cancel.child_token();
    let mut shards = JoinSet::new();
    for range in ranges {
        let permit = concurrency
            .clone()
            .acquire_owned()
            .await?;
        let canonical = config
            .canonical_dataset
            .clone();
        let workspace = attempt_root.join(format!("shard-{}", range.index));
        let binary = binary.clone();
        let chain_config = config.chain_config.clone();
        let cancel = shard_cancel.clone();
        let epoch = payload.epoch;
        let diagnostics = attempt_root.clone();
        shards.spawn(async move {
            let _permit = permit;
            clone_dataset(&canonical, &workspace, &cancel, deadline).await?;
            let mut result =
                run_shard(&binary, &chain_config, &workspace, epoch, &range, deadline, &cancel)
                    .await;
            if let Err(log_error) =
                persist_shard_diagnostics(&diagnostics, range.index, &result).await
            {
                result = Err(match result {
                    Ok(_) => log_error.context("persisting successful shard diagnostics"),
                    Err(error) => error.context(format!(
                        "also failed to persist shard diagnostics: {log_error:#}"
                    )),
                });
            }
            let cleanup = tokio::fs::remove_dir_all(&workspace).await;
            if let Err(error) = cleanup {
                tracing::warn!(
                    workspace = %workspace.display(),
                    %error,
                    "failed to clean block-validation shard workspace"
                );
            }
            result
        });
    }

    let mut outcomes = Vec::new();
    let mut failure = None;
    while let Some(outcome) = shards.join_next().await {
        match outcome {
            Ok(Ok(outcome)) if failure.is_none() => {
                outcomes.push(outcome);
                let _ = progress.send(BlockProgress {
                    completed_shards: outcomes.len() as u64,
                    total_shards,
                    checked_blocks: outcomes
                        .iter()
                        .map(|outcome| outcome.checked_blocks)
                        .sum(),
                });
            }
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                if failure.is_none() {
                    failure = Some(error);
                    shard_cancel.cancel();
                }
            }
            Err(error) => {
                if failure.is_none() {
                    failure =
                        Some(anyhow::Error::new(error).context("validation shard task failed"));
                    shard_cancel.cancel();
                }
            }
        }
    }
    if let Some(error) = failure {
        return Err(error);
    }
    outcomes.sort_by_key(|outcome| outcome.index);
    let checked_blocks = outcomes
        .iter()
        .try_fold(0_u64, |sum, outcome| sum.checked_add(outcome.checked_blocks))
        .context("checked-block count overflow")?;
    let invalid_blocks = outcomes
        .iter()
        .flat_map(|outcome| outcome.invalid_blocks.clone())
        .collect::<Vec<_>>();
    let valid = outcomes
        .iter()
        .all(|outcome| !outcome.normal_negative);
    let mut artifacts = Vec::with_capacity(outcomes.len() * 2 + 1);
    for outcome in &outcomes {
        for stream in ["stdout", "stderr"] {
            let path = attempt_root.join(format!("shard-{}.{}.log", outcome.index, stream));
            artifacts.push(path);
        }
    }
    let failures = attempt_root.join("invalid-blocks.json");
    tokio::fs::write(&failures, serde_json::to_vec_pretty(&invalid_blocks)?).await?;
    artifacts.push(failures);
    Ok(BlockExecution {
        result: BlockValidationResult {
            valid,
            checked_blocks,
            invalid_blocks,
            dataset: payload.dataset.clone(),
        },
        artifacts,
    })
}

async fn persist_shard_diagnostics(
    root: &Path,
    shard: u32,
    result: &anyhow::Result<ShardOutcome>,
) -> anyhow::Result<()> {
    match result {
        Ok(outcome) => {
            tokio::fs::write(root.join(format!("shard-{shard}.stdout.log")), &outcome.stdout)
                .await?;
            tokio::fs::write(root.join(format!("shard-{shard}.stderr.log")), &outcome.stderr)
                .await?;
        }
        Err(error) => {
            tokio::fs::write(root.join(format!("shard-{shard}.error.log")), format!("{error:#}\n"))
                .await?;
        }
    }
    Ok(())
}

/// Ask the provisioned dataset for its epoch boundaries and translate the
/// operator-facing inclusive global range into the epoch-local half-open
/// indices consumed by `stacks-inspect`.
async fn probe_range(
    binary: &Path,
    chain_config: &Path,
    dataset: &Path,
    epoch: ValidationEpoch,
    requested: &InclusiveRange,
    cancel: &CancellationToken,
    deadline: Instant,
) -> anyhow::Result<InclusiveRange> {
    let pre_nakamoto =
        probe_total(binary, chain_config, dataset, "index-range", cancel, deadline).await?;
    ensure!(pre_nakamoto > 0, "dataset probe returned no pre-Nakamoto blocks");
    match epoch {
        ValidationEpoch::PreNakamoto => {
            ensure!(
                requested.end < pre_nakamoto,
                "requested pre-Nakamoto range {}-{} exceeds probed global range 0-{}",
                requested.start,
                requested.end,
                pre_nakamoto - 1
            );
            Ok(requested.clone())
        }
        ValidationEpoch::Nakamoto => {
            let nakamoto =
                probe_total(binary, chain_config, dataset, "naka-index-range", cancel, deadline)
                    .await?;
            ensure!(nakamoto > 0, "dataset probe returned no Nakamoto blocks");
            let global_end = pre_nakamoto
                .checked_add(nakamoto)
                .and_then(|value| value.checked_sub(1))
                .context("probed global block range overflow")?;
            ensure!(
                requested.start >= pre_nakamoto && requested.end <= global_end,
                "requested Nakamoto range {}-{} is outside probed global range {}-{}",
                requested.start,
                requested.end,
                pre_nakamoto,
                global_end
            );
            Ok(InclusiveRange {
                start: requested.start - pre_nakamoto,
                end: requested.end - pre_nakamoto,
            })
        }
    }
}

async fn probe_total(
    binary: &Path,
    chain_config: &Path,
    dataset: &Path,
    command: &str,
    cancel: &CancellationToken,
    deadline: Instant,
) -> anyhow::Result<u64> {
    let mut process = Command::new(binary);
    process
        .arg("--config")
        .arg(chain_config)
        .arg("validate-block")
        .arg(dataset)
        .arg(command)
        .stderr(Stdio::piped());
    let output = run_captured(
        &mut process,
        &format!("probing block-validation dataset with {command}"),
        cancel,
        deadline,
    )
    .await
    .with_context(|| format!("probing block-validation dataset with {command}"))?;
    ensure!(
        output.status.success(),
        "dataset {command} probe failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    parse_probe_total(&String::from_utf8_lossy(&output.stdout))
        .with_context(|| format!("parsing dataset {command} probe"))
}

fn parse_probe_total(output: &str) -> anyhow::Result<u64> {
    output
        .split_whitespace()
        .next_back()
        .context("probe output was empty")?
        .parse()
        .context("probe output did not end in an integer block count")
}

/// Fail startup before advertising block-validation capability unless the
/// configured immutable dataset and exact reflink mechanism are usable.
pub async fn verify_host(
    config: &BlockValidationConfig,
    dataset: &sbgh_proto::DatasetIdentity,
) -> anyhow::Result<()> {
    let payload = BlockValidationPayload {
        dataset: dataset.clone(),
        epoch: ValidationEpoch::Nakamoto,
        range: InclusiveRange {
            start: dataset.covered_start,
            end: dataset.covered_start,
        },
        requested_shards: 1,
        max_concurrency: 1,
        timeout_secs: 1,
    };
    verify_dataset(config, &payload).await?;
    let symlink = Command::new("/usr/bin/find")
        .arg(&config.canonical_dataset)
        .args(["-type", "l", "-print", "-quit"])
        .output()
        .await
        .context("scanning canonical dataset for symlinks")?;
    ensure!(symlink.status.success(), "canonical dataset symlink scan failed");
    ensure!(
        symlink.stdout.is_empty(),
        "canonical dataset contains a symlink; shared-write indirection is forbidden"
    );
    prove_cow_isolation(config, uuid::Uuid::new_v4()).await
}

pub async fn cleanup(config: &BlockValidationConfig, job_id: uuid::Uuid) -> bool {
    let path = config
        .workspace_root
        .join(job_id.to_string());
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(error) => {
            tracing::warn!(%job_id, %error, "block-validation cleanup failed");
            false
        }
    }
}

async fn verify_dataset(
    config: &BlockValidationConfig,
    payload: &BlockValidationPayload,
) -> anyhow::Result<()> {
    let metadata = tokio::fs::symlink_metadata(&config.canonical_dataset)
        .await
        .context("stat canonical block-validation dataset")?;
    ensure!(
        metadata.is_dir()
            && !metadata
                .file_type()
                .is_symlink(),
        "canonical dataset must be a real directory"
    );
    let manifest = config
        .canonical_dataset
        .join(DATASET_MANIFEST);
    let manifest_bytes = tokio::fs::read(&manifest)
        .await
        .with_context(|| format!("reading canonical dataset manifest {}", manifest.display()))?;
    let manifest_fields: DatasetManifest =
        serde_json::from_slice(&manifest_bytes).context("parsing canonical dataset manifest")?;
    let digest = sha256_file(&manifest)
        .await
        .with_context(|| format!("hashing canonical dataset manifest {}", manifest.display()))?;
    ensure!(
        digest.eq_ignore_ascii_case(
            &payload
                .dataset
                .manifest_sha256
        ),
        "canonical dataset manifest does not match assignment generation"
    );
    ensure!(
        manifest_fields.generation == payload.dataset.generation
            && manifest_fields.network == payload.dataset.network
            && manifest_fields.format_version == payload.dataset.format_version
            && manifest_fields.covered_start == payload.dataset.covered_start
            && manifest_fields.covered_end == payload.dataset.covered_end,
        "canonical dataset manifest fields do not match assignment identity"
    );
    ensure!(
        manifest_fields
            .files_sha256
            .len()
            == 64
            && manifest_fields
                .files_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit()),
        "canonical dataset file-list digest is invalid"
    );
    let file_list = config
        .canonical_dataset
        .join(DATASET_FILE_DIGESTS);
    ensure!(
        sha256_file(&file_list)
            .await
            .with_context(|| format!("hashing dataset file list {}", file_list.display()))?
            .eq_ignore_ascii_case(&manifest_fields.files_sha256),
        "canonical dataset file list does not match its manifest"
    );
    ensure!(
        payload.range.start >= payload.dataset.covered_start
            && payload.range.end <= payload.dataset.covered_end,
        "assignment range is outside the pinned dataset generation"
    );
    Ok(())
}

async fn prove_cow_isolation(
    config: &BlockValidationConfig,
    job_id: uuid::Uuid,
) -> anyhow::Result<()> {
    let proof = config
        .workspace_root
        .join(format!(".cow-proof-{job_id}"));
    clone_dataset(
        &config.canonical_dataset,
        &proof,
        &CancellationToken::new(),
        Instant::now() + Duration::from_secs(300),
    )
    .await?;
    let canonical_manifest = config
        .canonical_dataset
        .join(DATASET_MANIFEST);
    let canonical_before = sha256_file(&canonical_manifest).await?;
    let clone_manifest = proof.join(DATASET_MANIFEST);
    let mut clone_bytes = tokio::fs::read(&clone_manifest).await?;
    clone_bytes.extend_from_slice(b"\n");
    tokio::fs::write(&clone_manifest, clone_bytes).await?;
    ensure!(
        sha256_file(&canonical_manifest).await? == canonical_before,
        "mutating a reflink clone changed the canonical dataset"
    );
    tokio::fs::remove_dir_all(&proof).await?;
    Ok(())
}

async fn clone_dataset(
    source: &Path,
    destination: &Path,
    cancel: &CancellationToken,
    deadline: Instant,
) -> anyhow::Result<()> {
    if tokio::fs::try_exists(destination).await? {
        tokio::fs::remove_dir_all(destination).await?;
    }
    tokio::fs::create_dir_all(destination).await?;
    let source_contents = source.join(".");
    let mut copy = Command::new("/bin/cp");
    copy.args(["--reflink=always", "-a"])
        .arg(source_contents)
        .arg(destination);
    let output = run_captured(&mut copy, "spawning reflink clone", cancel, deadline)
        .await
        .context("spawning reflink clone")?;
    ensure!(
        output.status.success(),
        "CoW clone failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    let mut chmod = Command::new("/bin/chmod");
    chmod
        .args(["-R", "u+w"])
        .arg(destination);
    let writable =
        run_captured(&mut chmod, "making private CoW workspace writable", cancel, deadline)
            .await
            .context("making private CoW workspace writable")?;
    ensure!(
        writable.status.success(),
        "making CoW workspace writable failed: {}",
        String::from_utf8_lossy(&writable.stderr).trim()
    );
    Ok(())
}

async fn ensure_binary(
    config: &BlockValidationConfig,
    repository: &str,
    commit: &str,
    repository_token: Option<&str>,
    cancel: &CancellationToken,
    deadline: Instant,
) -> anyhow::Result<PathBuf> {
    ensure!(
        (commit.len() == 40 || commit.len() == 64)
            && commit
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit()),
        "resolved commit must be a hexadecimal object ID"
    );
    let entry = config
        .binary_cache
        .join(commit);
    let binary = entry.join("stacks-inspect");
    if binary.is_file() {
        return Ok(binary);
    }
    tokio::fs::create_dir_all(&config.source_cache).await?;
    tokio::fs::create_dir_all(&config.binary_cache).await?;
    let source = config
        .source_cache
        .join(commit);
    if !source.is_dir() {
        let staging = config
            .source_cache
            .join(format!(".staging-{commit}-{}", uuid::Uuid::new_v4()));
        let mut clone = Command::new(&config.git_binary);
        clone
            .args(["clone", "--no-checkout", "--filter=blob:none"])
            .arg(format!("https://github.com/{repository}.git"))
            .arg(&staging);
        add_git_auth(&mut clone, repository_token);
        run_checked(&mut clone, "cloning block-validation source", cancel, deadline).await?;
        let mut fetch = Command::new(&config.git_binary);
        fetch
            .current_dir(&staging)
            .args(["fetch", "--depth=1", "origin", commit]);
        add_git_auth(&mut fetch, repository_token);
        run_checked(&mut fetch, "fetching block-validation commit", cancel, deadline).await?;
        let mut checkout = Command::new(&config.git_binary);
        checkout
            .current_dir(&staging)
            .args(["checkout", "--detach", commit]);
        run_checked(&mut checkout, "checking out block-validation commit", cancel, deadline)
            .await?;
        tokio::fs::rename(staging, &source).await?;
    }
    let build_root = config
        .binary_cache
        .join(format!(".staging-{commit}-{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&build_root).await?;
    let mut cargo = Command::new(&config.cargo_binary);
    cargo
        .current_dir(&source)
        .env("CARGO_TARGET_DIR", &build_root)
        .args(["build", "--locked", "--release", "--package", "stacks-inspect"]);
    run_checked(&mut cargo, "building stacks-inspect", cancel, deadline).await?;
    tokio::fs::create_dir_all(&entry).await?;
    tokio::fs::rename(build_root.join("release/stacks-inspect"), &binary).await?;
    let _ = tokio::fs::remove_dir_all(build_root).await;
    Ok(binary)
}

fn add_git_auth(command: &mut Command, token: Option<&str>) {
    if let Some(token) = token {
        command
            .env("GIT_CONFIG_COUNT", "1")
            .env("GIT_CONFIG_KEY_0", "http.https://github.com/.extraheader")
            .env("GIT_CONFIG_VALUE_0", format!("Authorization: Bearer {token}"));
    }
}

async fn run_checked(
    command: &mut Command,
    context: &str,
    cancel: &CancellationToken,
    deadline: Instant,
) -> anyhow::Result<()> {
    let output = run_captured(command, context, cancel, deadline).await?;
    ensure!(
        output.status.success(),
        "{context} failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout).trim(),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(())
}

struct CapturedOutput {
    status: std::process::ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

async fn run_captured(
    command: &mut Command,
    context: &str,
    cancel: &CancellationToken,
    deadline: Instant,
) -> anyhow::Result<CapturedOutput> {
    ensure!(Instant::now() < deadline, "{context} timed out before it started");
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command
        .spawn()
        .with_context(|| format!("spawning {context}"))?;
    let pid = child
        .id()
        .context("spawned process has no PID")?;
    let mut stdout = child
        .stdout
        .take()
        .context("spawned process has no stdout")?;
    let mut stderr = child
        .stderr
        .take()
        .context("spawned process has no stderr")?;
    let stdout_task = tokio::spawn(async move {
        let mut bytes = Vec::new();
        stdout
            .read_to_end(&mut bytes)
            .await
            .map(|_| bytes)
    });
    let stderr_task = tokio::spawn(async move {
        let mut bytes = Vec::new();
        stderr
            .read_to_end(&mut bytes)
            .await
            .map(|_| bytes)
    });
    let status = tokio::select! {
        status = child.wait() => status.with_context(|| format!("waiting for {context}"))?,
        () = cancel.cancelled() => {
            terminate_process_group(pid).await;
            let _ = child.start_kill();
            let _ = child.wait().await;
            let _ = stdout_task.await;
            let _ = stderr_task.await;
            anyhow::bail!("{context} cancelled");
        }
        () = tokio::time::sleep_until(deadline) => {
            terminate_process_group(pid).await;
            let _ = child.start_kill();
            let _ = child.wait().await;
            let _ = stdout_task.await;
            let _ = stderr_task.await;
            anyhow::bail!("{context} timed out");
        }
    };
    Ok(CapturedOutput {
        status,
        stdout: stdout_task.await??,
        stderr: stderr_task.await??,
    })
}

async fn run_shard(
    binary: &Path,
    chain_config: &Path,
    workspace: &Path,
    epoch: ValidationEpoch,
    range: &ShardRange,
    deadline: Instant,
    cancel: &CancellationToken,
) -> anyhow::Result<ShardOutcome> {
    let end_exclusive = range
        .end
        .checked_add(1)
        .context("stacks-inspect half-open range end overflow")?;
    let range_kind = match epoch {
        ValidationEpoch::PreNakamoto => "index-range",
        ValidationEpoch::Nakamoto => "naka-index-range",
    };
    let mut command = Command::new(binary);
    command
        .arg("--config")
        .arg(chain_config)
        .arg("validate-block")
        .arg(workspace)
        .arg(range_kind)
        .arg(range.start.to_string())
        .arg(end_exclusive.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let output = run_captured(
        &mut command,
        &format!("block-validation shard {}", range.index),
        cancel,
        deadline,
    )
    .await?;
    let status = output.status;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    // The assignment range is authoritative. `stacks-inspect` does not expose
    // a stable machine-readable checked-count field.
    let checked_blocks = range.end - range.start + 1;
    match status.code() {
        Some(0) => Ok(ShardOutcome {
            index: range.index,
            checked_blocks,
            invalid_blocks: Vec::new(),
            normal_negative: false,
            stdout,
            stderr,
        }),
        Some(1) if explicit_validation_failure(&stdout, &stderr) => {
            let invalid_blocks = parse_invalid_blocks(range.index, &stdout, &stderr);
            ensure!(
                !invalid_blocks.is_empty(),
                "negative shard {} reported no typed invalid-block details",
                range.index
            );
            Ok(ShardOutcome {
                index: range.index,
                checked_blocks,
                invalid_blocks,
                normal_negative: true,
                stdout,
                stderr,
            })
        }
        code => anyhow::bail!(
            "stacks-inspect shard {} infrastructure failure (exit={code:?}): stdout={} stderr={}",
            range.index,
            stdout.trim(),
            stderr.trim()
        ),
    }
}

#[cfg(unix)]
async fn terminate_process_group(pid: u32) {
    let group = format!("-{pid}");
    let _ = Command::new("/bin/kill")
        .args(["-TERM", "--", &group])
        .status()
        .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let _ = Command::new("/bin/kill")
        .args(["-KILL", "--", &group])
        .status()
        .await;
}

#[cfg(not(unix))]
async fn terminate_process_group(_pid: u32) {}

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

async fn sha256_file(path: &Path) -> std::io::Result<String> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    Ok(hex::encode(hash.finalize()))
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[test]
    fn partition_is_ordered_nonempty_and_exact() {
        let ranges = partition(InclusiveRange { start: 10, end: 20 }, 4).unwrap();
        assert_eq!(
            ranges,
            vec![
                ShardRange { index: 0, start: 10, end: 12 },
                ShardRange { index: 1, start: 13, end: 15 },
                ShardRange { index: 2, start: 16, end: 18 },
                ShardRange { index: 3, start: 19, end: 20 },
            ]
        );
    }

    #[test]
    fn shard_count_is_bounded_by_item_count() {
        let ranges = partition(InclusiveRange { start: 5, end: 6 }, 64).unwrap();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].start, ranges[0].end);
        assert_eq!(ranges[1].start, ranges[1].end);
    }

    #[test]
    fn exit_one_requires_explicit_validation_signature() {
        assert!(explicit_validation_failure(
            "ERRO Failed processing block 0x42: invalid parent",
            ""
        ));
        assert!(!explicit_validation_failure("", "database could not be opened"));
        assert!(!explicit_validation_failure("Validation completed with 2 error(s)", ""));
    }

    #[test]
    fn invalid_block_details_are_typed() {
        let invalid = parse_invalid_blocks(
            3,
            "\rValidating: 100% (1/1)\n\
             \n\
             Validation completed with 1 error(s) found in 12s:\n\
               Block outer: Failed processing block! block = aabb, error = Invalid parent\n",
            "",
        );
        assert_eq!(invalid.len(), 1);
        assert_eq!(invalid[0].shard, 3);
        assert_eq!(invalid[0].block, "aabb");
        assert_eq!(invalid[0].reason, "Invalid parent");
    }

    #[test]
    fn canonical_stacks_inspect_cost_failure_is_typed() {
        let invalid = parse_invalid_blocks(
            7,
            "  Block outer: Failed processing block! block = ccdd. Unexpected cost. expected = 10, evaluated = 11",
            "",
        );
        assert_eq!(invalid.len(), 1);
        assert_eq!(invalid[0].shard, 7);
        assert_eq!(invalid[0].block, "ccdd");
        assert_eq!(invalid[0].reason, "Unexpected cost. expected = 10, evaluated = 11");
    }

    #[tokio::test]
    async fn infrastructure_failure_keeps_attempt_scoped_shard_diagnostics() {
        let directory = tempfile::tempdir().unwrap();
        let result: anyhow::Result<ShardOutcome> =
            Err(anyhow::anyhow!("database could not be opened"));
        persist_shard_diagnostics(directory.path(), 4, &result)
            .await
            .unwrap();
        let log = tokio::fs::read_to_string(
            directory
                .path()
                .join("shard-4.error.log"),
        )
        .await
        .unwrap();
        assert!(log.contains("database could not be opened"));
    }

    #[test]
    fn probe_total_requires_an_unambiguous_trailing_count() {
        assert_eq!(parse_probe_total("Total blocks: 185630\n").unwrap(), 185_630);
        assert!(parse_probe_total("Total blocks: unknown").is_err());
        assert!(parse_probe_total("").is_err());
    }

    #[cfg(unix)]
    fn executable_script(contents: &str) -> (tempfile::TempDir, PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory
            .path()
            .join("stacks-inspect");
        std::fs::write(&path, format!("#!/bin/sh\nset -eu\n{contents}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        (directory, path)
    }

    #[cfg(unix)]
    fn test_range() -> ShardRange {
        ShardRange { index: 2, start: 10, end: 12 }
    }

    #[cfg(unix)]
    fn process_fixture_deadline() -> Instant {
        // Process startup can be delayed when the workspace test suite is
        // saturated; this deadline is not the behavior under test.
        Instant::now() + Duration::from_secs(10)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shard_exit_contract_distinguishes_valid_negative_and_infrastructure() {
        let (_success_dir, success) = executable_script("printf 'validation complete\\n'");
        let workspace = tempfile::tempdir().unwrap();
        let cancel = CancellationToken::new();
        let outcome = run_shard(
            &success,
            Path::new("unused.toml"),
            workspace.path(),
            ValidationEpoch::Nakamoto,
            &test_range(),
            process_fixture_deadline(),
            &cancel,
        )
        .await
        .unwrap();
        assert!(!outcome.normal_negative);
        assert_eq!(outcome.checked_blocks, 3);

        let (_negative_dir, negative) = executable_script(
            "printf '  Block outer: Failed processing block! block = aabb, error = wrong parent\\n'; exit 1",
        );
        let outcome = run_shard(
            &negative,
            Path::new("unused.toml"),
            workspace.path(),
            ValidationEpoch::Nakamoto,
            &test_range(),
            process_fixture_deadline(),
            &cancel,
        )
        .await
        .unwrap();
        assert!(outcome.normal_negative);
        assert_eq!(outcome.checked_blocks, 3);
        assert_eq!(outcome.invalid_blocks[0].block, "aabb");
        assert_eq!(outcome.invalid_blocks[0].reason, "wrong parent");

        let (_infra_dir, infra) =
            executable_script("echo 'database could not be opened' >&2; exit 1");
        let error = run_shard(
            &infra,
            Path::new("unused.toml"),
            workspace.path(),
            ValidationEpoch::Nakamoto,
            &test_range(),
            process_fixture_deadline(),
            &cancel,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("infrastructure failure")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shard_cancellation_terminates_the_whole_process_group() {
        let directory = tempfile::tempdir().unwrap();
        let child_pid = directory
            .path()
            .join("child.pid");
        let script = format!("sleep 30 &\necho $! > '{}'\nwait", child_pid.display());
        let (_script_dir, binary) = executable_script(&script);
        let cancel = CancellationToken::new();
        let cancel_task = cancel.clone();
        let pid_file = child_pid.clone();
        tokio::spawn(async move {
            for _ in 0..100 {
                if pid_file.exists() {
                    cancel_task.cancel();
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            cancel_task.cancel();
        });
        let error = run_shard(
            &binary,
            Path::new("unused.toml"),
            directory.path(),
            ValidationEpoch::Nakamoto,
            &test_range(),
            Instant::now() + Duration::from_secs(5),
            &cancel,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cancelled")
        );
        let pid = std::fs::read_to_string(child_pid).unwrap();
        let status = Command::new("/bin/kill")
            .args(["-0", pid.trim()])
            .status()
            .await
            .unwrap();
        assert!(!status.success(), "descendant process survived cancellation");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shard_timeout_is_an_infrastructure_error() {
        let (_directory, binary) = executable_script("sleep 30");
        let workspace = tempfile::tempdir().unwrap();
        let error = run_shard(
            &binary,
            Path::new("unused.toml"),
            workspace.path(),
            ValidationEpoch::PreNakamoto,
            &test_range(),
            Instant::now() + Duration::from_millis(10),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("timed out")
        );
    }
}
