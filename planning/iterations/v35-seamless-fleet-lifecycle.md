# v35: Seamless Fleet Lifecycle and Rolling Maintenance

Successor to
[v34](../archive/superseded/v34-first-fleet-deployment-readiness.md).
Separate daemon shutdown, scheduler admission, worker cordon and process
shutdown while preserving leases, fencing, cleanup and replay.

> **Status:** in_progress — implementation reviewed and deployed on the single
> Hetzner host on 2026-10-18. Two maintenance scenarios and evidence closeout
> remain. This is not qualification of a distributed fleet.
>
> **Scope approved 2026-10-19:** close v35 on lifecycle correctness and
> single-host operational acceptance. Broader feature and multi-worker checks
> have explicit owners below; they are deferred, not passed.

## Qualification Policy

The [failure matrix](../design/0082-fleet-lifecycle-failure-matrix.md) remains
the correctness contract. Lease enforcement, stale-result fencing,
cleanup-before-requeue, authorization, audit and idempotency are hard gates.

- Use production-Postgres tests for persistence and concurrency, controlled
  clocks/transport tests for timing boundaries, and daemon/MinIO composition
  tests for signal and artifact recovery. Do not repeat every race manually.
- Use host checks for installation, real VM teardown, deployed connectivity,
  representative lifecycle transitions and artifact retrieval.
- Retain dated evidence with its exact release and limitations. Repeat prior
  checks when relevant code, configuration or infrastructure changes invalidate
  them, not automatically for every release.
- The reviewed `1616c49` suite is the current automated baseline; October 15–16
  host restart/lease-loss checks are predecessor evidence, not reruns of that
  release. Source-pinned compatibility and recovery tests supplement them.
- Record observed teardown times, not an unmeasured universal timing guarantee.
  Artifact listings do not establish content verification; registration does
  not establish workload execution; a local VM does not establish independent
  physical-host availability.

## Qualification Handoff — 2026-10-19

The [first-fleet record](../qualification/2026-10-14-first-fleet.md) preserves
historical results. The [October 18 rollout](../qualification/2026-10-18-v35-rollout.md)
records deployment, backup restore, legacy/current worker behavior and successful
validation/benchmark canaries. Historical handoffs do not add closure gates
beyond this iteration's Final Validation.

## Items

| Item | Role | Status |
| ---- | ---- | ---- |
| `0082-seamless-fleet-lifecycle-maintenance` | primary: lifecycle separation and single-host maintenance | in_progress |
| `0075-rolling-worker-protocol-compatibility` | co-primary: bounded server-first compatibility | in_progress |

The item acceptance below states the behavior contract. The qualification
policy and Final Validation specify its evidence level: automated multi-session
coverage is retained; physical mixed-cohort operation is a separate `0080` gate.

## 0082 — Seamless Fleet Lifecycle and Maintenance

- **id:** `0082-seamless-fleet-lifecycle-maintenance`
- **status:** `in_progress`
- **priority:** `high`
- **depends_on:** `0004-worker-fleet`, `0074-protobuf-fleet-protocol`,
  `0076-database-backed-worker-registry`
- **relates_to:** `0075-rolling-worker-protocol-compatibility`,
  `0080-first-fleet-deployment-qualification`
- **unblocks:** `0083-multi-orchestrator-high-availability`
- **source:** v34 live daemon-restart and worker-drain qualification (2026-08)

**Problem:** The first fleet maps daemon shutdown to a persistent global worker
drain and maps systemd `SIGTERM` to cancellation of all active attempts. Idle
workers then deregister and exit successfully, so an ordinary daemon update
requires manual worker undrain and systemd restart. This couples centralized
control-plane maintenance to every worker and is unsuitable for independently
operated or multi-tenant capacity.

**Scope:** Implement
[Decision 0006](../decisions/0006-fleet-lifecycle-state-separation.md).
Make normal daemon shutdown process-local and non-destructive; add an explicit,
audited scheduler pause; separate worker cordon from process shutdown; provide
targeted daemon, worker-update, and host-maintenance workflows; preserve active
attempts through bounded control-plane restarts; and expose enough state and
wait primitives for safe rolling operations.

**Acceptance:**

- A normal daemon `SIGTERM` neither changes any worker's persisted scheduling
  policy nor requests cancellation of active fleet attempts.
- An idle worker remains process-resident across daemon restart and resumes
  polling without an operator undrain or systemd start, reusing its session
  inside the session TTL or safely reactivating the same session identity after
  expiry when no live successor exists.
- An active attempt survives a daemon outage shorter than its confirmed lease,
  retains the same job/attempt/fencing identities, replays buffered events,
  promotes artifacts once, and converges the same provider surfaces.
- An idle worker whose session expires safely reactivates the same session
  identity in process and reconciles cleanup before polling; it does not depend
  on systemd restart for recovery or fence a legitimate successor.
- An outage beyond the confirmed lease remains fail closed: local execution is
  cancelled, cleanup is verified, pending cleanup reaches the resident worker,
  requeue waits for its acknowledgement, and a stale terminal cannot win.
- Daemon startup gives overdue attempts one bounded heartbeat reconciliation
  opportunity without renewing their leases or allowing an authoritative
  overlap after the grace.
- A cordoned current-protocol worker accepts no new offers, finishes active
  work, remains online, and becomes schedulable again without process restart.
- Scheduler pause prevents new assignments without rewriting worker registry
  rows or terminating sessions; resume is idempotent and auditable.
- Targeted worker and host maintenance affect only the selected worker. Other
  compatible workers continue accepting work.
- Operator status distinguishes desired admission/scheduling policy, observed
  session state, active attempt state, and scheduler maintenance state.

**Deferred / non-goals:** No daemon-pushed binaries, package repository,
automatic host patching, indefinite attempt leases, cross-region control plane,
PostgreSQL HA, or multi-orchestrator serving (`0083`).

## 0075 — Rolling Worker Protocol Compatibility

- **id:** `0075-rolling-worker-protocol-compatibility`
- **status:** `in_progress`
- **priority:** `high`
- **depends_on:** `0074-protobuf-fleet-protocol`
- **source:** v29 protocol-scope simplification (2026-07), promoted by v35
  lifecycle planning (2026-08)

**Problem:** v29 establishes the first published protobuf/gRPC worker contract
with exact protocol-version matching. Once independently operated workers may
lag a protocol-changing central deployment, exact matching would make those
workers unavailable until upgraded. New live-cordon semantics also cannot be
silently assigned to the deployed revision-1 `Drain` response, whose workers
exit successfully.

**Scope:** Add a bounded current/previous compatibility window, protocol
revision and additive-feature negotiation, server-first rollout policy,
previous-worker runtime fixtures, and schema breaking-change enforcement
against the deployed `0f18950` baseline (operator-approved 2026-10-17; incompatible
pre-v33 revision-1 layouts are unsupported). Persist the negotiated contract on each
worker session and require both task capability and protocol feature support
before offering work. Expose negotiated lifecycle-feature support for `0082`;
the live-cordon behavior itself remains owned by that item.

**Acceptance:**

- The current daemon completes the compatible fleet lifecycle with the current
  and immediately previous supported worker.
- A server-first rollout keeps previous workers available while current workers
  are upgraded one cohort at a time.
- An older or incompatible worker is rejected before assignment with an
  actionable, non-retryable upgrade response.
- A worker is never offered task or lifecycle semantics it did not advertise.
- CI rejects field-number reuse and other wire-breaking schema changes against
  every supported published baseline.
- Operator documentation defines the support window, server-first rollout,
  convergence check, compatibility-floor advancement, rollback, and emergency
  disablement.

**Deferred / non-goals:** No indefinite support for arbitrary historical
workers, daemon-pushed scheduling or binaries, multi-orchestrator HA, or
requirement for a second worker implementation language.

## Carried Qualification

Full entries for `0080`, `0027` and `0039` moved to
[backlog.md](../backlog.md) as unscheduled candidates on 2026-10-19.
Existing implementations and evidence are retained; their remaining acceptance
is not a prerequisite for v35 closure.

| Check formerly blocking v35 | Owner / next gate |
| ---- | ---- |
| Second physical worker, independent maintenance, mixed-cohort real workloads | `0080`: before multi-worker production rollout; v36 snapshot bootstrap is `0084` |
| Overlapping-key rotation, open-connection revocation, capability/profile changes | `0080`: operational worker onboarding; automated authorization/fencing coverage remains mandatory in v35 |
| Natural-language Slack benchmark/validation journeys | `0080`: provider qualification; combine comparison evidence with `0039` |
| Calibration/repeat progress, fallback and reporting behavior | `0027`: representative live progress; automated edge-case coverage |
| Integrated comparison, repeat/calibration reuse and carried database | `0039`: independent feature acceptance |
| Full-history/cross-epoch validation coverage | v36 `0085`: workload qualification; exhaustive history is an optional scale soak, not a lifecycle gate |
| Direct S3 put/HEAD/get/checksum/delete ceremony | Replaced by job artifact upload plus download/content verification; automated promotion/GC checks retained |
| Every signal, race, abort, projection interruption and floor transition on the host | Automated failure-matrix coverage plus representative host scenarios; no destructive production drill solely to tick a box |
| Missing provenance from every historical job | Preserve known gaps; require reproducible identities for the selected closure evidence, not retrospective reconstruction of all history |

Second-worker and standalone build-only preparation can follow v35; neither is
required to finish this deployed single-host release. No performance-isolation,
community trust or multi-host availability claim is implied.

## State Model

| Concern | Authority | Durable | Meaning |
| ------- | --------- | ------- | ------- |
| Worker enabled | worker registry | yes | identity may register and participate |
| Worker schedulable / cordoned | worker registry | yes | new-offer admission policy |
| Scheduler running / paused | control-plane policy | yes | fleet-wide new-assignment gate |
| Worker online / idle / busy / offline | session + lease | observed | current process and assignment state |
| Shutdown after idle | worker session command | bounded | explicit process-lifecycle request |
| Attempt continue / cancel | fenced attempt | yes | authoritative execution lifecycle |
| Daemon quiescing | daemon process | no | local listener/scheduler shutdown only |

Observed state must never silently become desired policy. Process signals must
never silently become fleet-wide maintenance commands.

## Design Rules

- **Lease and fencing remain the safety boundary.** Convenience does not grant
  a worker permission to execute or publish beyond its last confirmed lease.
- **Restart guarantees are explicitly bounded.** Active work is transparent
  only inside its last confirmed attempt lease. Idle workers recover an expired
  session by safely reactivating the same identity in process rather than
  creating a successor or relying on an unbounded session promise.
- **Cleanup delivery is continuous.** Registration-time reconciliation remains
  a safety check, not the only way a resident worker can discover an obligation.
  Revision-1 recovery uses the existing `ListCleanup` RPC without a new
  push field or wire semantic.
  Cleanup acknowledgement precedes any associated requeue.
- **Startup grace reconciles; it does not renew.** On daemon startup, overdue
  attempts receive one bounded heartbeat opportunity before expiry. Only an
  authenticated heartbeat extends authority.
- **Ordinary shutdown is not emergency control.** `SIGTERM` quiesces one daemon;
  cancellation, disablement, revocation, and scheduler pause are explicit API
  mutations.
- **Cordon is reversible without process orchestration.** A current worker
  stays connected while unschedulable. Host shutdown is separate and explicit.
- **Persistent transitions are auditable.** Record actor, reason, timestamp,
  previous state, and resulting state for scheduler and worker-policy changes.
- **Roll server first.** The daemon accepts the bounded previous revision before
  any worker adopts a new revision or semantic feature.
- **Match on negotiated semantics.** Capability strings alone never authorize
  a job whose lifecycle or payload contract the worker did not negotiate.
- **Preserve the worker rollout window.** Protobuf and task contracts remain
  compatible with the declared current/previous workers until the floor
  advances. For this one-host schema rollout, the operator approved coordinated
  backup/restore-only daemon rollback on 2026-10-17; old-daemon operation on the
  expanded schema is not required. Roll-forward is preferred.
- **Keep software distribution external.** The daemon reports versions and
  controls scheduling; signed package/build delivery and systemd execution stay
  with the worker operator or host-management system.
- **Preserve provider convergence.** A control-plane restart may delay progress,
  but replay must update the same comment, Check Run, or Slack message exactly
  once.

## Deliverables

- Accepted Decision 0006 plus a checked-in daemon/worker/network failure matrix.
- Process-local daemon shutdown supervision with explicit normal, forced-local,
  and authenticated fleet-abort paths.
- Continuous cleanup-obligation delivery and in-process stale-session recovery.
- Persistent, audited scheduler pause/resume and worker cordon controls.
- Current/previous protocol negotiation, pinned previous-worker fixtures, and
  protobuf breaking-change enforcement.
- Live-cordon and explicit shutdown-after-idle semantics for negotiated workers.
- Operator CLI status/wait controls and daemon-update, worker-update,
  host-maintenance, rollback, and emergency playbooks.
- A repeatable real-host maintenance qualification record.

## Persistence and Migration

- Retain `worker_registry.draining` as the single persisted cordon policy
  during this rollout; do not add a second writable alias and synchronization
  trigger. New cordon commands and legacy drain endpoints update the same
  audited policy. Negotiated session features determine resident versus legacy
  exit behavior. Process shutdown is a separate session-targeted command.
- Add a singleton scheduler-policy row with mode, monotonic generation, actor,
  reason, and timestamps. Pause/offer races bind to its committed generation.
- Add append-only fleet administration audit rows for pause/resume,
  cordon/uncordon, shutdown-after-idle, abort, and compatibility-floor changes.
- Persist negotiated protocol revision/features and any bounded session command
  needed for shutdown-after-idle, referencing the exact worker session.
- Preserve cleanup obligations by logical worker and fenced attempt. Add
  delivery claim/cursor state only if needed for continuous idempotent delivery;
  completion remains the sole authority that permits requeue.
- Extend the existing registration transaction to reactivate the same expired
  session UUID only when its authenticated registration facts match and no
  other live session exists. This is an additive revision-1 server behavior,
  not a protobuf change; a new session UUID retains the existing successor
  fencing semantics.
- Keep schema changes additive where practical. The previous daemon is not a
  rollback target for the expanded database: restore its matching database,
  binaries and configuration together. Do not erase migration/audit history or
  weaken validation. The October 18 host database restore passed; the operator
  waived isolated application rehearsal for this rollout. Retain the matching
  recovery package and reconciliation procedure; prefer a forward fix once
  new work has been accepted.

## Phases

Implementation detail and historical test runs remain in the
[failure matrix](../design/0082-fleet-lifecycle-failure-matrix.md);
the [review handoff](../reviews/2026-10-18-v35-cleanup-round-2-review.md) and
[rollout record](../qualification/2026-10-18-v35-rollout.md) supply release evidence.
The following replaces stale implementation/review checkboxes; it does not
declare every original host ceremony passed.

### Phase 1: Lifecycle Contract and Failure Matrix

Decision 0006 is accepted. The transition matrix distinguishes authority from
physical teardown, records host H/L/S settings and pins the supported
`0f18950` revision-1 schema and semantic digests. Incompatible pre-v33 layouts
that reused revision 1 are explicitly excluded.

### Phase 2: Resident Session and Cleanup Recovery

Implemented and reviewed. Automated tests cover expired-session reactivation,
superseded-session rejection, stalled RPC lease deadlines, cleanup retries and
acknowledgement loss. October 15–16 host evidence covers idle recovery and the
corrected active lease-loss/cleanup/retry path without a worker process restart.

### Phase 3: Non-Destructive Daemon Quiescence

Implemented and reviewed. Actual daemon-process signal tests, PostgreSQL
startup-grace tests, artifact recovery through MinIO and provider-adapter replay
tests provide the recorded local recovery evidence. Host short-outage continuity and long-outage
fail-closed recovery are retained with their recorded releases. No separate
live-provider interruption at every projection boundary is required.

### Phase 4: Scheduler Pause and Audited Maintenance State

Implemented, reviewed and deployed. Production-store race tests cover pause
versus offer/accept, persisted generations and idempotent audit receipts.
The remaining host scenario is A below.

### Phase 5: Rolling Protocol Negotiation

Implemented, reviewed and deployed server-first. Source-pinned previous/current
runtime probes and Buf checks cover the supported window. On October 18 the
unchanged previous worker registered and exited cleanly on cordon; the current
worker negotiated revision 2 and remained resident. This is not a two-host
mixed-workload test; that operational gate belongs to `0080`.

Keep floor 1; advancing it is an operational decision, not a release exercise.
Old-daemon binary-only rollback onto the expanded schema is unsupported.
The matching host backup restored successfully; isolated application rehearsal
was explicitly waived on October 18. Do not reopen that waived ceremony or
record it as a pass.

### Phase 6: Live Cordon and Targeted Host Maintenance

Implemented, reviewed and deployed. Idle resident cordon/uncordon and worker
replacement passed on the host. Automated tests cover targeted command fencing,
cleanup admission and failed-update isolation. The remaining active-work
scenario is B below. Physical second-host continuity and host-reboot drills
belong to `0080`, not this single-host closure.

### Phase 7: Single-Host Acceptance and Evidence Closeout

Use small compatible workloads, not full-history validation or long performance
runs. Reuse jobs across checks where transitions remain separately observable.
Only the operator runs host mutations; do not inject destructive failure drills
into unrelated work. Follow the
[operations runbook](../../docs/worker-fleet-operations.md).

#### Scenario A — Scheduler pause/resume

- [ ] Pause with active work and a queued canary: the active attempt continues
  under the same identity; the queued work receives no new attempt while paused.
- [ ] Restart the daemon while paused; confirm the pause generation persists
  without changing worker cordon policy or requiring a worker restart.
- [ ] Retry the same administrative request to verify its idempotent receipt,
  resume, and observe queued work admitted. Retain audit generation/request
  identities and successful terminal/cleanup evidence.

Keep the restart within confirmed lease bounds if work is still active; if it
has finished, verify persisted pause before resuming. This is not another
long-outage drill.

#### Scenario B — Active-worker maintenance

- [ ] Cordon an active current worker: the existing attempt finishes normally;
  queued work is not admitted. Capture the worker's resident behavior.
- [ ] Request shutdown-after-idle while work is active; verify successful
  terminal completion, cleanup acknowledgement, clean deregistration and
  process exit. No forced stop or cancellation substitutes for this check.
- [ ] Restart while still cordoned, verify preflight/current protocol and
  persisted policy, then uncordon and observe new work admitted.

Use separate transitions or small canaries if necessary to distinguish
resident cordon from an explicit shutdown request. Finish with the service
healthy, the intended policy restored and zero attempts/cleanup debt.

#### Evidence closeout

- [ ] Download an artifact from an existing successful release canary; record
  job/key, retrieval outcome and checksum, and validate its expected content.
  Compare a producer checksum when available; do not claim end-to-end checksum
  comparison from a newly computed download hash alone.
- [ ] Consolidate A/B and artifact evidence into the rollout record with exact
  release/configuration, job/attempt/session/audit identities and limitations.
  Operator accepts the single-host closure and retained recovery exception.

## Final Validation

These are the complete v35 closure gates. The broader correctness contracts
remain mandatory, but deferred host/feature qualification does not block this
iteration.

- [x] Lifecycle/protocol implementation reviewed; Decision 0006 retained.
- [x] Reviewed automated baseline: 1,114 tests passed, 26 skipped; lint,
  protocol runtime/Buf and documentation checks recorded in the review handoff.
  Skipped tests are not counted as passes.
- [x] Server-first single-host deployment, previous-worker registration/legacy
  exit, current-worker preflight/resident behavior and both workload smoke tests.
- [x] Protected matching recovery package and host database restore evidence;
  isolated application rehearsal explicitly waived, not passed.
- [ ] Scenario A: scheduler pause/resume host acceptance.
- [ ] Scenario B: active-worker maintenance host acceptance.
- [ ] Artifact retrieval and consolidated operator sign-off.

October 15–16 restart/lease-loss evidence and the current automated recovery
suite are retained rather than requiring a replay of the entire host matrix.
A new defect or a relevant implementation/configuration change reopens the
affected gate. Closing v35 does not close `0080`, `0027`, `0039`, or qualify
multi-worker production operation.

## Stop and Rollback Rules

- Never weaken lease expiry, cleanup admission or stale-result fencing.
- Startup reconciliation grace is not lease renewal.
- Never reinterpret revision-1 drain as resident cordon without negotiation.
- Floor advancement requires compatible enabled workers, paused quiescence
  and an explicit recovery decision; leave floor 1 for this rollout.
- Pause and reach quiescence before planned outages exceeding active leases.
- A failed worker update stays cordoned. A failed daemon update keeps intake
  contained and scheduling paused once reachable; do not resume until healthy.
- Prefer reviewed roll-forward fixes. If restoring, restore matching database,
  binaries/configuration/units together, preserve failed-state data and
  reconcile post-backup jobs, artifacts and provider effects before admission.

## Follow-Ups

- `0080`, `0027`, `0039`: see Carried Qualification and the registry.
- v36 `0084`/`0085`: trusted-worker snapshot bootstrap and distributed
  validation workload/coverage qualification.
- `0083`: multi-orchestrator availability, leader election and rolling replicas.
- External package/signing and host automation may consume the maintenance
  interfaces; the daemon does not distribute binaries.
