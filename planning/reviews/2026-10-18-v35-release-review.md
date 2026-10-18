# v35 Release Candidate — Opus Review Handoff

## Review identity

- Repository: `/Users/cylwit/Code/github.com/cylewitruk/stacks-bench-github`.
- Base: `0f18950ad086799015fd1af0d66a7e672a3e5e27`.
- Candidate: 101 modified/new files, currently uncommitted and unstaged.
- File inventory: [SHA-256 manifest](2026-10-18-v35-release.sha256).
- Manifest SHA-256: `aaf2ef4223d2d8e3cd33712671e383685d56e7e76a98511a3f2f5ce7aad4a07e`.
- This handoff and its manifest are review metadata, outside the hashed candidate.

The manifest detects edits; it is not an immutable checkout or permission to
discard changes. Check identity before and after review. Stop and report drift
instead of reviewing a moving target. Include new files: `git diff HEAD` alone
omits them. Flag any additional candidate files absent from the manifest.

```bash
git rev-parse HEAD
git status --short
shasum -a 256 planning/reviews/2026-10-18-v35-release.sha256
shasum -a 256 -c planning/reviews/2026-10-18-v35-release.sha256
git diff --stat HEAD
git diff --binary HEAD
git ls-files --others --exclude-standard
```

## Assignment and boundaries

Perform one consolidated, read-only correctness/operability review. Read
`AGENTS.md` and the working agreement it references. Do not edit, stage, commit,
push, provision or contact production. Local fixture tests are allowed; report
what you actually ran separately from the author's evidence below. Do not
start additional reviewers unless the operator requests it.

Decide whether this candidate is safe to commit and proceed to a controlled
single-host qualification rollout. This is **not** a request to declare v35
complete or to design general macOS/ARM execution. Identify concrete regression,
race, authorization, data-loss and unsafe-maintenance risks before style nits.
Trace contracts across layers; passing isolated tests are not sufficient proof.

Accepted decisions:

- Decision 0006 was operator-approved; process signals must not become fleet
  abort or persistent drain.
- The supported previous worker is exactly `0f18950`, not incompatible pre-v33
  binaries that reused revision 1. Only one Hetzner worker is deployed.
- On 2026-10-17 the operator approved **backup/restore-only daemon rollback**
  and preferred roll-forward fixes. The old daemon cannot start on the new
  migration ledger. No bridge release is required; do not bypass migration
  validation. Matching DB/binaries/configuration and reconciliation of later
  work/provider effects are required for a restore.
- Host qualification, second-worker commissioning and full v35 closure remain
  open. Standalone build-only support comes after this release is qualified.

Authoritative context:

- [Decision 0006](../decisions/0006-fleet-lifecycle-state-separation.md).
- [v35 acceptance and carried gates](../iterations/v35-seamless-fleet-lifecycle.md).
- [Failure matrix and local evidence](../design/0082-fleet-lifecycle-failure-matrix.md).
- [Recorded host evidence](../qualification/2026-10-14-first-fleet.md).
- [Operational procedures](../../docs/worker-fleet-operations.md).
- [Pinned predecessor contract](../../fixtures/fleet/revision-1/README.md).

## Review map

| Area | Main files | Required checks |
| ---- | ---- | ---- |
| Scheduler and cancellation | `sbgh-postgres/src/stores/fleet/{scheduler,abort,cancellation}.rs`, `fleet.rs`, six new migrations | Lock ordering and races across prepare/offer/accept, pause/resume, terminal and cleanup. Abort scope selection and immutable retry receipts. Expired cleanup cannot requeue a cancelled job. Zero-byte artifact/projection debt counts. |
| Resident maintenance | `sbgh-postgres/src/stores/fleet/lifecycle.rs`, `sbgh-worker/src/fleet.rs`, lifecycle tests | Cordon preserves active work; shutdown targets one session; lost ACK/replay cannot stop its successor. Cleanup before new work. Generation/session-pinned waits, offline versus quiescent versus deregistered distinctions. |
| Compatibility and floor | `sbgh-fleet/src/{negotiation,model,validate}.rs`, proto/convert, store `protocol.rs`, daemon gRPC compatibility, fixtures | Immutable selected contracts; task authorization is distinct from feature support. Previous Drain retains exit behavior. Floor changes serialize with registration/offer/accept and require paused, fully quiescent, converged state. |
| Operator APIs and CLI | New API/CLI/core scheduler, lifecycle, protocol and abort modules; daemon API endpoints | Read/admin enforcement, server-derived audit actor, replay conflict handling, bounded waits, truthful JSON/human output and no implicit policy changes. Provenance is worker-reported, not attestation. |
| Shutdown and recovery | Daemon `shutdown.rs`, coordinator tests, `tests/daemon_lifecycle.rs`, artifact tests | Real signal paths remain process-local. Startup grace does not extend leases. Provider identity/terminal state survive replay; object-copy/DB-ACK gaps converge. Tests must not hide child leaks or replace actual guarantees with mocks. |
| Worker update script | `scripts/update-worker-host.sh` and its Python tests | Targeted quiescence, legacy/current ordering, error paths, changed sessions/generations and readiness checks. A failed update must not uncordon or stop unrelated workers. |
| Validation telemetry | Libvirt guest template/helper, driver/cloudinit tests, Python progress tests | Initialization is not processed work; ETA is conservative and reset/suppressed appropriately. Best-effort telemetry never changes terminal authority. Preserve trusted range counts and bounded parsing/emission. |
| Release and recovery | CI, nextest configuration, pinned build script, rollback tests and operations docs | Tests exercise the claimed contract. No accidental live credentials/resources. Roll-forward/restore instructions match the accepted scope, preserve evidence and keep unsafe traffic stopped. |

All crate paths in this table are under `crates/`. Read referenced unchanged
callers where needed, but keep findings attributable to this candidate.

## Author verification — 2026-10-18

- `just build --no-sccache`: passed.
- `just lint --no-sccache`: passed, including 17 Python validation-progress tests.
- Full nextest run `dd663b8d-072c-449a-aec1-4bb429ac5455`: **1,107 passed,
  26 skipped**. No leak warning on this run; historical intermittent child-leak
  warning remains unresolved, not silently declared fixed.
- Buf 1.69.0 `breaking proto --against fixtures/fleet/revision-1/proto`: passed
  in a read-only, network-disabled container.
- `just qualify-protocol-runtime`: **2 passed**, run
  `73830c00-fd11-4b68-b422-1758b67df0a5` (previous/current worker and schema-restore
  probes). These explicitly run two tests skipped by the ordinary suite.
- Documentation links, diff whitespace and all 101 candidate hashes: passed.

Useful verification commands (Docker needed for integration fixtures):

```bash
just build --no-sccache
just lint --no-sccache
just test --no-sccache --failures
just qualify-protocol-runtime
just check-proto-compat
git diff --check
```

Pinned runtime probes use production TLS/RPC/stores but injected resource facts
and mock execution commands, not actual KVM/LVM tasks. Full-daemon signal tests
seed attempts rather than running worker VMs. Local MinIO/provider-adapter tests
are not live GitHub/Slack evidence. The schema probe proves isolated restoration,
not a production restore or preservation of post-backup external effects.

## Required response

1. Findings first, ordered by severity. For each: exact file/line, triggering
   sequence, violated invariant, user-visible impact and a focused fix/test.
   Distinguish demonstrated defects from hypotheses needing reproduction.
2. Explicit commit-readiness verdict: ready, ready with non-blocking follow-ups,
   or blocked. Separate code defects from acknowledged deployment gates.
3. Tests actually run, results, coverage gaps and any unreviewed scope.
4. Remaining host qualification checks; do not mark them complete from mocks.
5. Confirm the manifest still matches. If no actionable issues were found, say
   so directly, with residual risks rather than an unconditional safety claim.

After fixes, use a new dated/round-specific manifest and review only the fix
delta plus affected contracts, while confirming every original finding is
resolved. Preserve this initial handoff rather than silently changing its hashes.
