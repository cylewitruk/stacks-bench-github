# v35: Seamless Fleet Lifecycle and Rolling Maintenance

Successor to
[v34](v34-first-fleet-deployment-readiness.md). Convert the conservative
first-fleet shutdown behavior exposed by real-host qualification into an
operator-safe lifecycle for a centralized daemon and independently maintained
workers. Normal daemon restarts must not drain or stop the fleet; targeted
worker maintenance and bounded protocol upgrades must preserve availability,
leases, fencing, and auditability.

> **Status:** planned — architecture and acceptance are defined; implementation
> starts after v34 qualification is closed or its remaining gates are
> explicitly rescheduled.
>
> v35 owns lifecycle separation and a bounded current/previous protocol rollout
> window. Multi-orchestrator HA is captured separately as `0083`.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0082-seamless-fleet-lifecycle-maintenance` | primary: daemon, scheduler, worker, and host lifecycle separation | planned |
| `0075-rolling-worker-protocol-compatibility` | co-primary: bounded server-first worker upgrades | planned |

## 0082 — Seamless Fleet Lifecycle and Maintenance

- **id:** `0082-seamless-fleet-lifecycle-maintenance`
- **status:** `planned`
- **priority:** `high`
- **depends_on:** `0004-worker-fleet`, `0074-protobuf-fleet-protocol`,
  `0076-database-backed-worker-registry`,
  `0080-first-fleet-deployment-qualification`
- **relates_to:** `0075-rolling-worker-protocol-compatibility`
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
- **status:** `planned`
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
against the published v29 baseline. Persist the negotiated contract on each
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
  Until Phase 5 negotiation exists, revision-1 workers periodically call the
  existing `ListCleanup` RPC; Phase 2 adds no push field or wire semantic.
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
- **Use expand/contract changes.** Database and protobuf changes remain readable
  by every binary inside the declared rollout window until the compatibility
  floor advances.
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

- Add nullable `worker_registry.cordoned`, backfill it from `draining`, and use
  a compatibility trigger to derive it from old-daemon inserts/updates while
  new daemons dual-write both fields. After the previous daemon revision is
  retired, make `cordoned` non-null with a fail-closed default. Keep legacy
  drain API behavior for revision-1 workers until the worker compatibility
  floor advances; only then remove the old column, trigger, and naming.
- Add a singleton scheduler-policy row with mode, monotonic generation, actor,
  reason, and timestamps. Pause/offer races bind to its committed generation.
- Add append-only fleet administration audit rows for pause/resume,
  cordon/uncordon, shutdown-after-idle, abort, and compatibility-floor changes.
- Persist negotiated protocol revision/features and any bounded session command
  needed for shutdown-after-idle on the worker session.
- Preserve cleanup obligations by logical worker and fenced attempt. Add
  delivery claim/cursor state only if needed for continuous idempotent delivery;
  completion remains the sole authority that permits requeue.
- Extend the existing registration transaction to reactivate the same expired
  session UUID only when its authenticated registration facts match and no
  other live session exists. This is an additive revision-1 server behavior,
  not a protobuf change; a new session UUID retains the existing successor
  fencing semantics.
- Apply every schema change expand-first. Contract legacy fields only after all
  serving daemons and required workers have left the previous compatibility
  revision and rollback has been exercised.

## Phases

### Phase 1: Lifecycle Contract and Failure Matrix

**Goal:** Freeze the desired/observed state boundaries and transition rules
before changing shutdown behavior.

**Scope:**

- Accept Decision 0006 after review.
- Specify normal stop, short outage, long outage, explicit pause, emergency
  abort, worker cordon, worker stop, and worker-loss transitions.
- Define invariants for leases, fencing, cleanup, replay, and provider updates.
- Derive transparent-restart, session-recovery, startup-grace, and maximum
  overlap windows from configured heartbeat, attempt-lease, and session TTLs.

**Status:**

- [ ] Decision 0006 accepted
- [ ] Failure matrix and transition table complete
- [ ] Reviewed (Codex)
- [ ] Validation cases pinned before implementation

**Acceptance & Validation:**

- [ ] Every transition has one durable authority and one observable terminal
  state; no transition depends on shell-command ordering for correctness.
- [ ] The failure matrix identifies which work continues, pauses, cancels, or
  expires for each daemon/worker/network failure.
- [ ] The matrix records the deployed timing values and formulas; no acceptance
  claim uses an unbounded meaning of “restart” or “reconnect.”
- [ ] The matrix distinguishes loss of authoritative mutation rights from full
  physical teardown of the VM, mounts, and snapshots; scheduling and requeue
  remain cleanup-gated until the latter completes.

**Tests:**

- State-transition table tests for valid, idempotent, and rejected mutations.

### Phase 2: Resident Session and Cleanup Recovery

**Goal:** Remove process restart as an implicit prerequisite for session and
cleanup convergence.

**Scope:**

- Poll the existing `ListCleanup` RPC on registration and periodically while a
  revision-1 worker is resident; prioritize obligations ahead of new offers.
  Do not add cleanup fields to heartbeat/poll responses before Phase 5.
- Reactivate an idle expired session UUID in process only when the authenticated
  worker and registration facts match and no live successor session exists.
- Treat `stale_session` as a recoverable lifecycle condition only after proving
  that no authoritative local attempt remains.
- After active lease loss, cancel and verify local teardown before reactivating
  the session, reconciling cleanup, or polling for work.
- Phase 2 never creates a new session UUID for recovery. Retry transient
  transport failures with the same live session. Permit same-identity recovery
  only after the server reports it stale or expired, and reject it if another
  live successor exists. Return typed, non-retryable `session_superseded`; the
  resident worker exits without minting a fresh UUID. Only a new worker process
  may initiate the existing successor-session fencing path.
- Deploy the additive daemon-side recovery transaction before enabling the
  worker retry path; an upgraded worker against a previous daemon must retain
  the existing fail-closed exit behavior.
- Keep cleanup completion and job requeue idempotent under retry and reconnect.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] An obligation inserted after worker startup is discovered, executed, and
  acknowledged without process restart; its job is requeued exactly once only
  after acknowledgement.
- [ ] An idle expired session reactivates its original session ID in process
  while keeping the same worker identity and policy.
- [ ] A transient connection failure never creates a successor session; tests
  pin same-session recovery, rejection when a live successor exists, and the
  store's successor-fencing contract.
- [ ] Superseded recovery returns `session_superseded`; the resident worker
  treats it as non-retryable, exits, and never mints a replacement session ID.
- [ ] Server-first rollout leaves previous workers unchanged and prevents an
  upgraded worker from looping against a daemon without recovery support.
- [ ] An expired active attempt cannot overlap a successor session's work:
  local cancellation and cleanup finish before session recovery and polling.
- [ ] Duplicate delivery, acknowledgement loss, daemon restart, and worker
  reconnect converge on one completed obligation.

**Tests:**

- Worker loopback tests for periodic delivery, same-session recovery,
  active lease loss, and cleanup-before-poll ordering.
- PostgreSQL races for obligation insertion/delivery/completion/requeue and
  same-session recovery versus successor-session fencing.
- Store and worker tests pin the typed superseded response and fail-closed exit
  when a live successor owns the worker identity.

### Phase 3: Non-Destructive Daemon Quiescence

**Goal:** Make a compatible single-daemon restart transparent within explicit
session and lease bounds.

**Scope:**

- Remove persistent global worker drain and fleet-wide cancellation from normal
  process signals.
- Map `SIGTERM` and first `SIGINT` to local quiescence; map second `SIGINT` to
  forced local exit without fleet mutation. Keep fleet abort as an explicit,
  authenticated operation.
- Replace the fleet coordinator's sole-exit ownership with a shutdown supervisor
  that waits only for local components and bounded in-flight RPC handling.
- On startup, hold attempt expiry for one configured heartbeat reconciliation
  opportunity without extending an unconfirmed lease.
- Reconstruct scheduling and projection state from PostgreSQL after restart.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] Restarting inside the session TTL reuses the active idle session; an
  outage beyond it uses Phase 2 same-session recovery. Neither path needs worker
  undrain or systemd start.
- [ ] Restarting during an active attempt inside its confirmed lease preserves
  its authoritative generation and terminal result.
- [ ] A healthy worker heartbeat during startup grace preserves its attempt;
  silence beyond the grace expires it once without renewing authority.
- [ ] The failure matrix separates the authoritative overlap bound (startup
  grace, after which fencing rejects stale mutation) from the physical-resource
  bound (startup grace plus the next heartbeat observation and bounded VM,
  mount, and snapshot teardown). Requeue and local admission remain cleanup
  gated through the latter.
- [ ] Restart after reliable terminal acceptance but before projection reuses
  the same provider identity and terminal state.
- [ ] All three signal paths and explicit fleet abort have distinct tests and
  observable outcomes.

**Tests:**

- Daemon composition tests around both signals, forced local exit, listener
  restart, session reuse/recovery, startup grace, and projection replay.
- PostgreSQL tests proving shutdown does not mutate worker registry policy and
  startup expiry remains database-time authoritative after the grace.

### Phase 4: Scheduler Pause and Audited Maintenance State

**Goal:** Provide an explicit control-plane gate for long or incompatible
maintenance without draining workers.

**Scope:**

- Persist running/paused scheduler state with actor, reason, generation, and
  timestamps.
- Add idempotent pause/resume/status/wait operator APIs and CLI commands.
- Prevent preparation and offers while paused; allow heartbeats, cleanup,
  terminal submission, artifact promotion, and projection to continue.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] Pause introduces no new offers after its committed generation and does
  not alter worker registry or session state.
- [ ] Active attempts finish normally and queued jobs remain queued.
- [ ] Resume is idempotent, survives daemon restart, and restores scheduling
  without worker restarts.

**Tests:**

- PostgreSQL race tests for pause generation versus prepare/poll/accept and
  restart persistence.
- API/CLI authorization, audit, idempotency, and wait-command tests.

### Phase 5: Rolling Protocol Negotiation

**Goal:** Land and validate `0075` compatibility infrastructure independently
before assigning any new lifecycle semantics.

**Scope:**

- Add bounded current/previous protocol and additive-feature negotiation.
- Persist the negotiated contract and match offers on task plus semantic
  feature support.
- Pin previous-worker runtime fixtures and published protobuf baselines.
- Document server-first rollout and compatibility-floor advancement.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] Previous and current workers coexist under the current daemon and receive
  only task semantics they negotiated.
- [ ] Revision-1 `Drain` retains its deployed exit behavior; no new semantics
  are silently assigned to an old revision.
- [ ] An incompatible worker is rejected before assignment with an actionable,
  non-retryable response.
- [ ] CI rejects wire breakage against every supported published baseline.

**Tests:**

- Pinned previous-worker runtime and loopback fixtures.
- Protobuf breaking-change, feature-negotiation, offer-matching, floor-advance,
  and rollback tests.

### Phase 6: Live Cordon and Targeted Host Maintenance

**Goal:** Use negotiated lifecycle semantics for safe one-worker-at-a-time
maintenance.

**Scope:**

- Add live cordon/uncordon for current workers; keep them connected through
  heartbeat and bounded polling while unschedulable.
- Add explicit shutdown-after-idle, distinct from scheduling policy.
- Add status and wait commands for schedulability, attempts, cleanup, session
  revision/features, preflight revision, and the effective cordon semantic.
- Document worker binary/config updates, reboot, identity rotation, canary
  cohorts, failure isolation, rollback, and the revision-1 requirement to
  restart a worker after it exits on drain and is later uncordoned.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] A current worker can cordon, reach idle, remain online, uncordon, and
  accept work without systemd intervention.
- [ ] Mixed-cohort status reports `exit_after_idle` for revision-1 cordon and
  `resident` for negotiated live cordon; wait/runbook behavior follows the
  reported semantic rather than assuming one fleet-wide meaning.
- [ ] Shutdown-after-idle never interrupts an attempt and exits cleanly only
  after cleanup and deregistration.
- [ ] Cordon one of at least two workers, update/restart/verify it, and uncordon
  it while another worker continues accepting compatible work.
- [ ] A failed preflight leaves only that worker cordoned; rollback restores a
  compatible worker without daemon restart or attempt duplication.

**Tests:**

- Postgres fencing tests for cordon/uncordon/offer/accept races.
- Installer isolation plus a two-worker rollout integration ceremony.
- Manual real-host worker update and host-reboot qualification record.

### Phase 7: End-to-End Maintenance Qualification

**Goal:** Prove the complete daemon and worker operating paths on deployed
infrastructure.

**Scope:**

- Restart the daemon during idle, active validation, cleanup-obligation
  creation, artifact promotion, and provider projection.
- Run expired-session recovery, startup-grace, paused long-maintenance, and
  emergency-abort drills.
- Roll current and previous worker cohorts, then advance the compatibility
  floor deliberately.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] No ordinary daemon restart requires worker undrain or systemd restart.
- [ ] No compatible rollout cancels, duplicates, or reassigns an authoritative
  in-flight attempt.
- [ ] Every cleanup obligation is delivered and acknowledged before requeue,
  including when created during daemon or session recovery.
- [ ] Every deliberate pause, cordon, shutdown, abort, and floor advancement is
  attributable in durable audit evidence.
- [ ] Provider reports and promoted artifacts converge exactly once across all
  restart points.

**Tests:**

- `just build --no-sccache`
- `just lint --no-sccache`
- `just test --summary --no-sccache`
- `git diff --check`
- Real-host maintenance qualification record with daemon, worker, attempt,
  cleanup, session, protocol, audit, and provider identities.

## Final Validation

- [ ] Decision 0006 is accepted and reflected in active operations docs.
- [ ] Ordinary daemon restart is worker-transparent while short-outage lease
  and long-outage fail-closed behavior are both proven.
- [ ] Idle session expiry, active lease expiry, startup reconciliation grace,
  and continuous cleanup delivery converge without relying on systemd restart.
- [ ] Scheduler pause and worker cordon are independent, persistent, auditable,
  and idempotent.
- [ ] Targeted worker update and host maintenance leave unrelated workers
  schedulable.
- [ ] Current and previous protocol workers complete the supported lifecycle;
  incompatible workers fail before assignment.
- [ ] Rollout, rollback, emergency, and compatibility-floor procedures are
  executable playbooks rather than undocumented shell history.
- [ ] Expand/contract migrations preserve current and previous daemon/worker
  operation until the compatibility floor is deliberately advanced.

## Stop and Rollback Rules

- Do not weaken lease expiry or accept a stale terminal to make a restart pass.
- Do not treat startup reconciliation grace as lease renewal; only a confirmed
  heartbeat extends execution authority.
- Do not reinterpret revision-1 `Drain` as live cordon without negotiation.
- Do not advance the compatibility floor until every required worker is on a
  supported revision and rollback has been exercised.
- Pause scheduling before a planned outage that may exceed active leases; wait
  for attempts and cleanup to reach zero rather than granting an implicit
  indefinite maintenance lease.
- A failed worker update remains cordoned. A failed daemon update leaves the
  scheduler paused until the retained compatible daemon is healthy.

## Follow-Ups

- `0083-multi-orchestrator-high-availability` adds continuous control-plane
  availability, leader election, and rolling daemon replicas after the
  single-daemon lifecycle is correct.
- External package/signing and host-fleet automation may consume v35's
  cordon/wait/status interfaces; they are not implemented by the daemon.
