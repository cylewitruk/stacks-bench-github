# Fleet Lifecycle: Failure Matrix and Timing Bounds

Working Phase 1 contract for [v35](../iterations/v35-seamless-fleet-lifecycle.md),
implementing [Decision 0006](../decisions/0006-fleet-lifecycle-state-separation.md).
Direction accepted 2026-10-14. The matrix states required behavior; the local
implementation section records the tested subset. Host configuration and installed binary hashes were operator-reported
on 2026-10-14. The subsequent
[October 15 qualification](../qualification/2026-10-14-first-fleet.md#v35-deployment-and-restart-qualification--2026-10-15)
records the deployed release, running executable hashes, idle recovery, and
active validation continuity. Later sections are dated evidence snapshots;
their then-open gates are not a cumulative current release checklist.

## Qualification Scope — 2026-10-19

The operator approved [v35's single-host closure scope](../iterations/v35-seamless-fleet-lifecycle.md#final-validation).
The matrix below remains the correctness contract, not a requirement to inject
every failure on production. Production-Postgres, controlled-clock/transport,
real daemon-process and MinIO tests establish their respective invariants.
Host evidence establishes the deployed integration and physical cleanup only
within the observed cases. Prior release evidence is reused with source/config
limitations; rerun checks when a relevant change invalidates it.

The [October 18 rollout](../qualification/2026-10-18-v35-rollout.md) supersedes
earlier pending-deployment/compatibility-fixture notes. Scheduler pause/resume,
active-worker maintenance and evidence closeout remain v35 host work.
Expanded provider/progress/comparison, identity and physical multi-worker
ceremonies moved to named follow-ups; historical entries below are not passes.
Unmeasured G/E/Q/K timing bounds must not be claimed as hard host guarantees.
The operator-waived isolated application rollback rehearsal is not reopened.

## Source Baseline

Pre-change checkout: `c4edb4b03e93a833f4c3d3511ceac15272b938a1`.
The following describes that historical baseline, not the changed working tree
or an assertion about the deployed release.

- [Daemon signals](../../crates/sbgh-daemon/src/shutdown.rs) mapped
  SIGTERM to abort and SIGINT to drain/escalated abort.
- [Coordinator](../../crates/sbgh-daemon/src/fleet/coordinator.rs) persisted
  global drain, requests cancellation, and owns exit after attempts finish.
- [Worker loop](../../crates/sbgh-worker/src/fleet.rs) reconciled cleanup at
  startup and exits on Drain. Heartbeat lease checks run after an RPC error;
  successful responses reset a deadline from response receipt.
- [Registration store](../../crates/sbgh-postgres/src/stores/fleet.rs) and
  [service](../../crates/sbgh-daemon/src/fleet/service.rs) enforce current
  session identity and return `stale_session` on invalid polls. Same-identity
  recovery must preserve the existing successor-session fencing contract.

## Installed Binary Evidence — 2026-10-14

Operator ran `sha256sum /usr/local/bin/sbgh-daemon /usr/local/bin/sbgh-worker`
on `stacks-bench`:

```text
0cd997c1022a75906e24084d5b4079353291be6e1dbda3df4328f604f2fc3ec5  /usr/local/bin/sbgh-daemon
ea454480f46923d5036fc4ba3b53a3442521fdb084e8e23cbbca923e517055a0  /usr/local/bin/sbgh-worker
```

The worker hash matches the September 23 running/installed worker observation
in the [qualification record](../qualification/2026-10-14-first-fleet.md).
That earlier observation included checkout `fd01b1b2d691dcdcc87a13397b2ab9509244a6ac`;
checkout identity alone does not prove build provenance. The new hashes identify
installed files, not currently running `/proc/<pid>/exe` images. No daemon
source revision or deployed semantic-digest fixtures are established by this check.

## Timing Contract

The operator supplied the host configuration below on 2026-10-14. Its six
daemon timing values match the [source defaults](../../crates/sbgh-core/src/config.rs).
This is configuration evidence, not a live audit of the running process or its
overrides. Capture effective settings and build provenance before signing off
the restart window.

| Symbol | Meaning | Source default | Operator-reported host config |
| ---- | ---- | ---- | ---- |
| H | Heartbeat interval | 10 seconds | 10 seconds |
| L | Attempt lease TTL | 45 seconds | 45 seconds |
| S | Session TTL | 90 seconds | 90 seconds |
| O | Offer TTL | 30 seconds | 30 seconds |
| P | Long-poll timeout | 20 seconds | 20 seconds |
| R | Daemon request timeout | 120 seconds | 120 seconds |
| C | Worker control-RPC timeout | 45 seconds; maximum long poll + 15 | Not independently verified in deployed binary |

The same excerpt reported `upload_grant_seconds=300`,
`staging_gc_grace_seconds=86400`, `max_artifact_bytes=8589934592`,
`max_attempt_artifact_bytes=34359738368`, and `max_concurrent_requests=64`.
These are recorded for provenance, not substitutes for attempt/session leases.

Configuration currently requires `H > 0`, `L >= 3H`, `S >= 2L`, and `R > P`.
The [worker transport](../../crates/sbgh-worker/src/transport.rs) also has a
10-second connect timeout. R is not the worker heartbeat deadline.
Active-attempt heartbeats currently refresh the session to `2L`, whereas
registration uses S. Record both paths; do not assume every refresh grants S.

Required bounds:

- Let `D` be the conservative local monotonic deadline for the last confirmed
  lease. Transparent recovery requires outage + reconnect + authenticated
  renewal latency to fit within `D - now`, not simply within a fresh L.
- A renewal cannot grant L starting at response receipt when the server granted
  it earlier. Derive D conservatively from the request start or a validated
  server deadline with clock/transport uncertainty accounted for. Ignore late
  responses after local lease loss; they cannot restart cancelled execution.
- Enforce D independently of blocked heartbeat RPCs. The baseline post-error
  check and response-relative deadline were gaps, fixed by the independent
  deadline tests below; they are not an accepted extension of the lease.
- Idle-session continuity depends on remaining S since the server's last
  session refresh. After expiry, recover the same UUID only after proving no
  local authoritative attempt and no live successor. Retry alone grants no lease.
- Define startup reconciliation grace `G = H + B`, with B a configured, bounded
  heartbeat transport/scheduling allowance. Pin B against observed timings and
  tests before Phase 3. Grace delays expiry processing; it never grants local
  execution authority. A late heartbeat cannot revive an expired/fenced attempt.
- The current store requires `lease_expires_at > NOW()` to renew. Resolve the
  Phase 3 wording about an "overdue" attempt against this rule: grace may defer
  expiry processing, but must not silently allow renewal after lease expiry.
  Pin this boundary explicitly in the transition tests before implementation.
- If startup is at T, unresolved attempts must be fenced by `T + G + E`, where
  E is the bounded expiry-sweep/transaction delay. Physical teardown is separate:
  bound healthy-worker cancellation observation by Q and cleanup by K, then
  verify teardown by `T + G + E + Q + K`. Pin G/E/Q/K before claiming this bound.
  An unreachable or failed host has no finite proven teardown bound: retain the
  cleanup obligation and block reassignment until teardown is acknowledged.

## Transition and Failure Matrix

"Authority" names the owner of the transition, not an inferred process state.
Every retry must preserve the same logical action and be idempotent.

| ID / event | Authority | Required result / observable state |
| ---- | ---- | ---- |
| F01: daemon SIGTERM or first SIGINT | Local shutdown supervisor | Stop local intake/offers, finish bounded in-flight handling, exit without changing worker policy or cancelling attempts; workers reconnect |
| F02: second SIGINT | Local shutdown supervisor | Force local exit only; no global drain/abort mutation |
| F03: short outage with active attempt | Confirmed lease and fenced attempt | Continue inside D, renew before D, replay buffered events with unchanged attempt/generation/provider identities |
| F04: outage or RPC stall crosses D | Worker lease timer; database fencing/cleanup | Cancel locally, reject late results after fencing, verify teardown; requeue only after durable cleanup acknowledgement |
| F05: idle session expires | Authenticated registration transaction | Same UUID resumes after cleanup reconciliation if facts match and no live successor exists; no systemd restart |
| F06: live successor or mismatched recovery facts | Registration transaction | Reject recovery; superseded worker exits without creating another UUID or fencing the successor |
| F07: cleanup obligation appears after startup | Durable cleanup store | Resident worker discovers and completes it idempotently before accepting affected replacement work |
| F08: explicit scheduler pause/resume | Audited scheduler-policy generation | Pause blocks new assignments without changing worker policy; serialize races with assignment creation; resume once |
| F09: cordon/uncordon | Audited worker policy | Current worker finishes active work, stays online, accepts nothing while cordoned, resumes without process restart |
| F10: shutdown-after-idle / host maintenance | Explicit worker command and host operator | Stop selected worker only after active work and cleanup converge; other workers remain schedulable |
| F11: explicit cancellation / fleet abort | Authenticated audited control action | Cancel exactly the selected scope; terminal/requeue transitions remain fenced and cleanup-gated |
| F12: emergency disable / key revoke | Registry authorization transaction | Withdraw authority, fence affected work, retain cleanup; reconnect cannot bypass disabled/revoked identity |
| F13: worker loss during execution or cleanup | Lease expiry and durable cleanup obligations | No successful result inferred from silence; no replacement before verified teardown; recover outstanding cleanup |
| F14: restart after terminal acceptance, before report projection | Durable terminal/event/provider identity | Reconstruct same aggregate result and update the existing provider surface, not a duplicate |
| F15: previous-revision worker | Negotiated protocol contract | Preserve legacy exit-after-idle behavior; advertise it distinctly from resident cordon; no silent reinterpretation |

## Pinned Validation Cases

Implement these cases before declaring the corresponding behavior complete.
Use controlled clocks and narrow transport fakes for timing; use production
Postgres stores for concurrency, persistence, and fencing assertions.

- F01–F02: signal paths never call global drain/cancel; local exit does not wait
  for remote fleet idleness. Crash/restart preserves policy and attempt identity.
- F03–F04: heartbeat before/at/after D; response delayed past D; blackholed RPC;
  cancellation while RPC is pending; terminal versus expiry race; no new lease
  inferred from startup grace. Assert cancellation separately from cleanup.
- F05–F07: expired idle UUID recovery; mismatched facts; concurrent successor;
  lost registration response; obligation inserted after startup; duplicate
  acknowledgement; teardown failure keeps requeue blocked.
- F08–F12: duplicate pause/cordon/cancel actions; concurrent offer/pause commit;
  revocation on an open connection; one targeted worker changes while another
  remains available. Assert audit actor/reason/generation and resulting state.
- F13–F15: execution/cleanup host loss; accepted terminal with lost response;
  projection restart; mixed old/current workers and server-first rollback.

Extend existing [Postgres fleet tests](../../crates/sbgh-postgres/tests/postgres_fleet.rs),
including `response_loss_and_successor_session_are_safely_fenced` and
`terminal_after_orchestrator_lease_expiry_is_fenced`. Their existence does not
qualify new resident recovery. The current
[digest tests](../../crates/sbgh-fleet/src/digest.rs) are also not a substitute
for pinned vectors from the deployed revision-1 implementation.

## Phase 1 Exit Evidence

- [x] Operator accepted Decision 0006's direction on 2026-10-14.
- [x] Source behavior/defaults and required failure cases recorded.
- [x] Operator-reported host timing configuration recorded on 2026-10-14.
- [x] Installed daemon/worker binary SHA-256 recorded from operator output.
- [ ] Effective runtime timing settings and running executable identities verified.
- [ ] Previous revision's exact source/build provenance and semantic-digest
  vectors retained for payloads, reliable events, and terminal outcomes.
- [ ] G/E/Q/K and conservative lease-deadline derivation reviewed and fixed.
- [ ] Executable transition/timing cases established; matrix reviewed as complete.

Do not claim transparent production restarts on local evidence alone.

## Local Implementation — 2026-10-14

Implemented without schema or protobuf payload changes:

- **F01–F02:** process-local SIGTERM/first SIGINT quiescence and second SIGINT
  forced exit. The coordinator no longer persists fleet drain or cancellation.
  Pending local services have H shutdown grace; remote idleness is not required.
  Poll handlers stop offering once they observe local quiescence.
- **F03–F04:** a monotonic lease deadline bounds heartbeat waits and RPCs.
  Acceptance is timed from request dispatch; a first heartbeat must confirm
  authority before execution. Renewal is capped by both request-dispatch + L
  and conservatively translated server expiry. Deadline wins over simultaneous
  success; a late response cannot revive execution. Execution cancellation does
  not stop heartbeats during orderly teardown, but authority loss does cancel
  execution. Recovery waits for driver completion and verified cleanup.
- **F05–F07:** immutable same-UUID registration recovery rejects changed facts,
  explicit deregistration, and successors. Recovery waits for fencing of any
  previous active attempt. Cleanup is checked before every poll; completed
  acknowledgements are idempotent and do not repeat job requeue. If the session
  expires during teardown, the worker retains the verified cleanup acknowledgement
  across same-session recovery instead of repeating teardown on every expiry.
- **Compatibility subset:** new workers recover only on the new daemon's
  `session_recovery_required` code. Older-daemon rejection remains fail-closed;
  explicit `session_recovery_pending` responses wait with capped backoff until
  success, shutdown, or permanent rejection. Other consecutive registration
  failures remain bounded to eight attempts. Legacy Drain still exits
  the worker. No new protocol version, live cordon, or compatibility window is
  claimed by this implementation.

Startup grace is now **G = 2H**, choosing **B = H**. With reported configuration,
local shutdown grace is 10 seconds and startup sweeping begins after 20 seconds.
Sweeps then tick every two seconds. The database still rejects renewal after
lease expiry: grace only delays fencing/cleanup processing. The sweep interval
is not a proven upper bound on database or coordinator latency E. Physical
cancellation/teardown bounds Q/K remain host-qualification work.

### Executable Evidence

| Boundary | Coverage |
| ---- | ---- |
| Lease timing | Six paused-clock tests in `sbgh-worker/src/lease.rs`: stalled RPC, delayed success, exact-deadline ordering, cancellation, delayed acceptance, shorter server expiry |
| Resident worker | TLS loopback recovery, legacy-server rejection, supersession, periodic cleanup, acknowledgement recovery without repeated teardown, and heartbeat-loss cancellation during pending credentials |
| Registration persistence | Production Postgres tests for expired UUID recovery, immutable facts, deregistration, concurrent successor/recovery, active-attempt recovery gating |
| Cleanup persistence | Fenced attempt remains blocked until cleanup; duplicate acknowledgement succeeds without a second requeue |
| Recovery timing | Paused-clock reconciliation beyond 2H at H = 10/15/30 seconds, plus shutdown and successor rejection after more than eight pending responses |
| Daemon exit | Three signal/supervisor tests plus production-coordinator test preserving persisted enabled/draining policy and the full active-attempt row |

Validation commands on the changed tree:

```sh
just test --no-sccache --failures
just lint --no-sccache
```

Nextest run `8bbeeec0-a4fa-4180-a475-70fd61a3a63e`: **1,026 passed,
24 skipped**. Lint, formatting, documentation/registry checks, dependency
checks, and supporting script tests passed. Docker and loopback sockets required
running tests outside the filesystem sandbox. No physical-host VM restart or
live provider-replay canary was performed.

Two earlier revision-check runs passed with one nextest child-process leak
warning each. Both the follow-up per-test-status run and the final small-cleanup
run above were clean. The bounded investigation did not reproduce or attribute
the intermittent warning; it is not claimed fixed and is not attributed to
the loopback `LeaseLoss` fixture without evidence.

### Deployment and Remaining Gates

Review follow-up: Opus identified the bounded-recovery/startup-grace race and
inactive-session cleanup acknowledgement failure. Both have regression coverage
and implementation revisions. Core lifecycle errors now use typed variants;
shutdown tokens are supplied at construction; the test-only abort assertions
were removed. In the operator-supplied second review, Opus confirmed all first-round
findings resolved and found nothing blocking; Opus did not independently rerun
the gates. Final nits consolidated cleanup fallback checks into one query while
retaining worker ownership, and unified inactive-session RPC errors through the
same `FailedPrecondition` mapping. Phase 2 code review is closed; host
qualification remains open. Decision 0006 remains accepted.

Use the [server-first operations guidance](../../docs/worker-fleet-operations.md).
The first upgrade must be quiescent because stopping the currently installed
old daemon still has its old destructive shutdown semantics. Install the new
daemon before the new worker, retain rollback binaries, and keep timing settings
unchanged during the active-attempt canary. Record running executable hashes,
source/build identities, and attempt/session identities before and after it.

Then qualify idle and active short restarts, an outage beyond the confirmed
lease, cleanup/requeue convergence, and post-terminal provider projection.
Audited scheduler pause is now locally implemented (see the October 16 record
below), but not host-qualified. At this milestone resident cordon, negotiation,
previous-revision fixtures, fleet-wide abort controls and second-host operation
were still later work. Subsequent local evidence is recorded below; the full
iteration is not complete.

## Execution Sequence — 2026-10-16

The operator requested end-to-end v35 closure in this order:

1. Qualify F04 active lease loss and cleanup-gated retry on the deployed release.
2. Implement and qualify persistent audited scheduler pause/resume.
3. Pin revision-1 fixtures and implement current/previous protocol negotiation.
4. Implement negotiated resident cordon and separate shutdown-after-idle.
5. Commission a second physical worker and complete the remaining maintenance,
   provider, progress, comparison, identity-policy, and rollback checks in v35.

### Next Gate: Disposable Active Lease-Expiry Canary

This is a deliberate interruption, not a transparent-restart test. Before
enqueuing, capture fresh fleet/queue state, process/session identity, running
binary hashes, and chainstate prefix. Proceed only in an operator-confirmed
maintenance window with no unrelated attempts or queued work. Existing global
scheduler pause is not yet deployed; a quiet queue alone does not prevent new
provider submissions.

Use the previously successful one-block validation commit on the regular
mainnet origin with a new idempotency key. Once validating, capture its
submission/job/attempt IDs and a fresh heartbeat. Stop only the daemon for a
bounded interval exceeding the reported 45-second lease and 90-second session
TTL; retain a daemon restart recovery path. Do not drain or restart the worker.

Capture timestamped local cancellation and VM/mount/snapshot teardown evidence
during the outage. After daemon recovery, inspect durable attempt and cleanup
records as well as current CLI state. Acceptance requires:

- The original attempt loses authority and cannot submit an accepted success.
- Local execution stops and teardown is verified before replacement execution.
- The original session/process recovers without a systemd restart.
- One logical cleanup obligation is acknowledged before requeue; retries do
  not cause duplicate requeues.
- The same job runs under a new fenced attempt, completes successfully, and
  returns the fleet to zero cleanup debt.

An idle final fleet alone does not establish ordering or physical teardown.
If any check fails, preserve journals and database evidence; do not manually
remove resources, abandon cleanup, or restart the worker to make the test pass.
The [October 16 execution](../qualification/2026-10-14-first-fleet.md#active-lease-expiry-canary--2026-10-16-resident-recovery-failed)
showed cancellation and resource teardown, but failed resident recovery because
an unstructured `Unknown` reconnect error was treated as permanent. The
[corrected repeat](../qualification/2026-10-14-first-fleet.md#active-lease-expiry-repeat--2026-10-16-passed)
passed with unchanged worker process/session, completed cleanup before the
replacement offer, and successful replacement completion. This qualifies that
F04/F05/F07 path, not every failure-matrix permutation. Next: scheduler pause
release review and host qualification.

Use [the read-only recovery inspection query](../../scripts/inspect-fleet-recovery.sql)
before interruption and after recovery to retain all attempt generations and
cleanup rows, not just the currently selected attempt. On the combined host,
from the repository root, with `job_id` set to the captured job UUID:

```bash
sudo docker exec -i sbgh-postgres psql -X -U sbgh -d sbgh \
  -v ON_ERROR_STOP=1 -v job_id="$job_id" \
  < scripts/inspect-fleet-recovery.sql
```

The query uses a bounded read-only transaction and excludes credential/payload
contents. An absent job is reported explicitly; empty rows are not a pass.
Pair these records with timestamped journals and local resource inventories;
database timestamps alone do not prove the physical teardown boundary.

Local query smoke check (2026-10-16): applied all repository migrations to a
fresh local Postgres database, executed all inspection queries successfully for
an absent UUID (`job_exists = false`), and confirmed malformed UUID input fails.
The isolated database was dropped afterward. This validates schema compatibility
and input rejection, not a populated recovery history or the production canary.

## Scheduler Admission — Local Implementation 2026-10-16

The additive scheduler-policy migration starts in running generation zero.
Pause/resume serialize with source freezing, payload preparation, offer creation,
and acceptance using the singleton policy lock. A pause withdraws unaccepted
offers and requeues their claimed jobs without changing worker registry/session
rows; accepted attempts retain their leases. Stale preparation generations and
pre-pause offers cannot cross a pause/resume boundary.

Administration is admin-authenticated and append-only audited by request UUID.
Duplicate requests return their original receipt without reapplying it; changed
input conflicts. Status exposes policy independently from fleet liveness. CLI
wait requires a stable paused generation and zero durable work counters, with
a bounded timeout and clean JSON stdout. Diagnostics now use stderr.

Validation:

- Ten focused scheduler tests passed across production PostgreSQL stores,
  authenticated API routing, and real CLI/loopback HTTP calls. The admission
  race test observes three database lock waiters before committing pause.
  The reverse-order test proves pause waits for in-flight acceptance and then
  preserves that accepted attempt.
- Full nextest run `6c758aae-8df9-4db7-a5a6-258f93672433`: **1,038 passed,
  24 skipped**, with two intermittent child-process leak warnings.
- Bounded leak-reporting rerun `58c07941-ce5c-4586-a427-c43306ee698d`:
  **1,038 passed, 24 skipped**, no leak warnings. The earlier warnings were not
  reproduced or attributed; they are not claimed fixed.
- `just lint --no-sccache`, documentation checks, and `git diff --check` passed.

This is local implementation evidence, not a host gate or release review.
Remaining F08 checks: deploy daemon/CLI at quiescence, pause while a canary is
active, prove queued work cannot start, wait for cleanup/projection/artifacts,
restart the daemon while paused, then resume without changing worker process
identity. Keep previous-daemon rollback separate: it does not enforce scheduler
policy and startup against the newer migration ledger is not yet qualified.
See the [operator procedure](../../docs/worker-fleet-operations.md#scheduler-maintenance).

### Scheduler Review Follow-up — 2026-10-16

Local review found a deployment regression: printing scheduler policy before
fleet counters broke the first-line contract used by both host update scripts.
The counters remain first; a real CLI test covers old-daemon responses without
scheduler policy and new-daemon responses with it. The test failed before the
fix (nextest `cf22a687-f057-47e0-8cd8-9e9b56a79edc`).

Additional tests verify that pending cleanup and a zero-byte staged artifact
remain separate maintenance debts, and that every nonzero counter prevents
quiescence. The store test uses production registration, artifact grant,
cleanup acknowledgement, and reap acknowledgement operations while paused.

Combined follow-up gate `0b128e15-88f4-4332-be07-c44e43903b91`: **1,044 passed,
24 skipped**, no leak warnings. Lint passed. The operator procedure now includes
the host evidence sequence; no production deployment or independent review was
performed. Phase 4 host acceptance remains open.

## Deployed Protocol Baseline — 2026-10-16

The [revision-1 fixture](../../fixtures/fleet/revision-1/README.md) pins the exact
`0f18950` schema and 14 semantic-digest vectors, with source and operator-reported
running-binary hashes. Two Rust tests protect fixture identity and typed-domain
serialization. CI now runs strict Buf compatibility against this frozen schema.
Buf 1.69.0 passed unchanged and additive-field checks; a valid schema reusing
registration field 2 as `uint64 worker_epoch` failed on type/name/JSON-name
changes as expected.

Historical inspection found incompatible registration changes in v31 and
validation changes in v33 under the same revision number as original v29.
The operator approved the deployed `0f18950` contract as the previous cohort on
2026-10-17, excluding incompatible pre-v33 layouts; no claim of compatibility
with every historical revision-1 binary is made. At this milestone, live
registration negotiation and previous-worker runtime qualification were pending.

Final combined local gate: nextest run
`f05037e2-7c1a-42f3-b14b-bcab66ca4334` — **1,041 passed, 24 skipped**, no leak
warnings on this run. Lint, formatting, documentation checks and diff whitespace
checks passed. Earlier intermittent leak warnings remain unproven, not fixed.

## Negotiation Core — 2026-10-17

Operator approval resolves the supported previous-contract boundary. The new
transport-neutral negotiation core selects the highest shared revision within
the bounded 1–2 window, intersects explicit features, validates server selections,
and rejects malformed advertisements or an incompatible floor. Revision 1 keeps
its fixed deployed semantics; unknown optional features never become enabled.
Task-semantic support is distinct from capability authorization. At this initial
core milestone no live-cordon or shutdown semantics were advertised; their
later implementation is recorded in the resident-maintenance section below.

Nextest `4396406e-f411-45ec-864e-314a365bf6ae`: all **19 sbgh-fleet tests passed**,
including five negotiation tests and the frozen revision-1 digest fixtures.
Workspace lint passed. At this milestone runtime still advertised revision 1: registration wire
extensions, immutable session persistence, offer admission, audited floor control,
and mixed-version runtime checks must land before activation. No host changes
were made. Phase 5 implementation and host acceptance remain open.

## Registration Negotiation Integration — 2026-10-17

Additive registration fields now carry revision/feature offers and selected
contracts. Legacy registrations retain revision-1 responses. Session persistence
preserves immutable advertisements and selections across recovery; offer admission
checks task semantics separately from capability authorization. The current
worker advertises revision 2 and validates the selected contract before execution.

Nextest `1b514585-ad7d-483f-84ea-311d89a047fd`: **1,054 passed, 24 skipped**, with
no leak warnings. Workspace lint, Buf 1.69.0 compatibility against the frozen
revision-1 schema, and diff whitespace checks passed. Tests cover wire round trips,
malformed selections, immutable recovery facts, and mixed-feature task admission.

The operator confirmed that only the Hetzner worker is deployed. Compatibility
scope is its pinned `0f18950` contract plus the successor. These local tests are
not proof of a second physical worker or a pinned previous binary's runtime.
Audited floor control, previous-worker runtime and rollback
gates remain open. No host changes were made.

### Contract visibility follow-up

Worker inspection now exposes the latest session's advertisement and selected
revision/features. Legacy sessions have a null explicit offer but their fixed
selected contract remains visible. Unregistered workers and older API responses
do not invent a contract. `fleet status` appends selected revision/features while
preserving the counter line consumed by deployment scripts. Historical session
facts are not presented as proof that an offline worker has converged.

Focused nextest runs `17783a23-9d75-4523-b544-7951aaa95f6d` (10 negotiation tests)
and `68e52707-3b79-4148-a6ad-f8e23d9737ff` (9 protocol tests) passed. Coverage includes
production-store visibility after recovery, HTTP serialization with read scope,
and real CLI rendering against legacy and current responses. Workspace lint
passed. These focused runs supplement, not replace, the preceding full-suite gate.

## Audited Compatibility Floor — 2026-10-17

The persisted floor defaults to 1. Registration/recovery, offer admission and
acceptance enforce it. Administration serializes with those paths and changes to
enabled-worker membership. Raising the floor requires paused scheduling, no
attempt/cleanup/projection/artifact debt, and a live compatible session for every
enabled worker. An expired or merely historical compatible session does not count.

Read-scoped inspection and admin-scoped changes are available through
`fleet protocol status` and `fleet protocol set-floor`. Changes retain immutable,
retry-safe receipts with principal, reason and an explicit rollback-evidence
reference. Lowering the floor is an audited maintenance action; existing session
contracts remain immutable. This is not proof that an old daemon can safely run
against the migrated schema or enforce newer maintenance policies.

Full nextest run `55c2a9dd-6b63-40d6-a64c-8ec9a0dc736e`: **1,061 passed, 24 skipped**,
with no leak warnings. Workspace lint passed. New checks cover convergence,
expired sessions, active-work refusal, immutable audit receipts, explicit floor
rollback, authenticated HTTP and CLI operations, and a real PostgreSQL lock race
where an in-flight legacy registration prevents advancement. No host changes.

Pinned previous-worker runtime qualification, broader rollout/rollback matrix
coverage and operator-run host acceptance remain open; Phase 5 is not closed.

## Pinned Worker Runtime Boundary — 2026-10-17

`just qualify-protocol-runtime` builds a native launcher against the unchanged
`0f18950` archive and its lockfile, plus the same launcher against the current
worker. Both run as separate processes against the production daemon mTLS
listener, RPC service and migrated PostgreSQL stores. No GitHub/provider access
is needed; host resources are injected and local command fixtures replace
libvirt/LVM preflight.

The gate proves both cohorts register with the expected contracts, recover the
same session without replacing the process, and preserve legacy drain/exit and
deregistration semantics. After retiring revision 1, the previous worker reports
`protocol_incompatible` and creates no additional session. This is a pinned-source
runtime test, not the production Linux executable or a task-execution test.

The complete entry point passed: nextest
`64211136-c210-4254-ae18-5264bf5857a7` — **1 test passed**, exercising both cohorts.
Its predecessor probe SHA-256 was
`a7388e8fbbb3f191293f207daaa554786ef2dfe01fbf2301105bf9c4a5412b7c`;
temporary build paths mean this debug artifact is not byte-reproducible across
invocations. Source identity is the pinned commit, not a match to the host binary.

Full regression run `6f76d5d9-dfe8-4881-a5bf-57272683d93f`: **1,061 passed, 25 skipped**,
with no leak warnings. The added runtime test is intentionally skipped by the
default suite and run explicitly by CI after building its probes. Lint and diff
whitespace checks passed. Task execution, artifact/provider recovery, actual
daemon restart, old-daemon schema compatibility and host rollback remain open.
No host changes were made.

Follow-up runtime run `c0a3914a-9e92-42be-8a1f-fbc85b1e464a`: **1 passed**
in 40.691 seconds. Both source-pinned worker processes survived short and
session-expiring outages of the production listener/service composition. Each
restart rebuilt in-memory service state over the same PostgreSQL database and
required a fresh heartbeat with unchanged process and session identities.
This replaces synthetic session expiry in the fixture; it does not exercise a
full daemon process, coordinator startup, active task or physical-host rollback.

## Predecessor Schema Rollback Boundary — 2026-10-17

The same source archive now builds `schema-probe.rs` against the unchanged
predecessor persistence crate. Its production startup migrator accepts the
original ledger through `20260731000002`, but returns the typed
`MigrateError::VersionMissing` after the v35 migrations. The failed check leaves
the complete ledger unchanged, and the current migrator still accepts it.
Binary-only rollback to this predecessor is therefore unavailable.

Runtime entry point `10614e41-fa71-4784-b500-1734f040af4f`: **2 passed** (both
worker cohorts plus the initial schema refusal check). The predecessor schema
probe SHA-256 was
`7e443cc222c6aaabb635e8b5da9f61e389f440b1dd61ee39a73f284525c5e9cc`.
As with the worker probe, source identity is pinned; temporary debug build paths
can change the executable hash.

Expanded schema run `eb45424d-dd80-4cb5-822d-368ae89786ff`: **1 passed**. A real
PostgreSQL custom-format dump taken before migration restored into a separate
generated database. The exact migration ledger and seeded disabled/drained
worker row survived, and the predecessor migrator reopened the restored copy.
The upgraded source database remained unchanged. CI runs this expanded check
through `just qualify-protocol-runtime`.

This qualifies an isolated database-startup boundary, not a running old daemon,
host backup ceremony or restoration of later accepted work. No migration guard
was weakened and no production state was changed. Host restore and full runtime
rollback remain open; a restore after intake resumes requires explicit handling
of submissions, results, audit and external effects since the backup.

Regression run `d37b017c-bbe8-4d98-bf3f-f0bceef77bc2`: **1,061 passed, 26 skipped**,
with no leak warnings. The two pinned compatibility tests are explicit CI gates,
not default-suite coverage. `just lint --no-sccache`, documentation checks and
diff whitespace checks also passed.

## Resident Worker Maintenance — 2026-10-17

New negotiated features separate resident cordon from explicit session-targeted
shutdown. The existing `draining` column remains the single scheduling policy;
no dual-write alias is needed. Administrative mutations retain immutable audit
receipts, serialize with offer acceptance and never interrupt accepted work.
Shutdown replay retains its original session target. Cleanup gates exit and a
lost deregistration acknowledgement is retried by the same worker process.

`fleet worker-status` reads policy/session/attempt/cleanup state in one database
snapshot. `fleet wait-worker` requires cordon, pins generation and session, and
waits for either zero execution/cleanup debt or acknowledged deregistration.
Expiry alone is not exit; a host service check remains necessary. The command
deadline includes HTTP time and structured output excludes progress messages.

Runtime gate `5e144563-7e5d-4fee-a1ff-551b0da88f20`: **2 passed**. The current
worker remained in the same process/session through cordon and uncordon, then
exited on explicit shutdown. The pinned predecessor retained drain-and-exit;
both cohorts retained the existing restart and compatibility-floor checks.
The isolated predecessor schema/restore boundary also passed.

Initial lifecycle regression `fd90110f-27b3-4bee-8f5e-da1b5ea45a89`:
**1,068 passed, 26 skipped**, including both registry-lock race orderings.
Targeted wait/API checks `608282be-3f1e-40d3-815e-f821862da755`:
**14 passed**. These include cleanup debt, changed generations/sessions,
expiry-versus-deregistration, read/admin authorization and stalled HTTP deadlines.
Final regression `6f1a2240-ea5f-419b-9b2b-bb916255803d`: **1,073 passed, 26 skipped**,
with one nextest leak warning on
`sbgh-api::dto::tests::block_validation_selector_round_trips_and_rejects_retired_plan_fields`.
That test only serializes/deserializes DTOs and contains no process spawning.
An unchanged isolated 20-repeat run (`e0ea9fa0-19fc-4025-a8cc-e8503c7a33d8`)
passed without warnings. This narrows the observation but does not establish
the cause or resolve the intermittent warning. No leak threshold was changed.
Build, lint, documentation and diff checks passed. Cached Buf 1.69.0 passed
schema lint and breaking checks against the pinned predecessor; a direct Buf
release download was rejected with HTTP 403 before using the cached container.

This is local evidence only. Later sections record effective admission and
worker-reported preflight provenance. Host installer qualification, full task/provider mixed-version execution,
physical host maintenance and the second-worker ceremony remain open.

### Targeted Worker Updater

`update-worker-host.sh` now requires the maintenance API before mutation,
cordons only its authenticated target, waits against the immutable receipt's
generation and requests explicit shutdown when supported. It no longer blocks
on unrelated fleet cleanup or projection work. A resident replacement starts
while cordoned and must report a new live session before admission reopens.
The previous worker retains uncordon-before-start ordering. Readiness output
reports negotiated cordon semantics rather than inferring them from revision.

Seven Python interface/behavior tests passed, running a disposable copy of the
actual shell workflow against command fixtures. Cases cover both cohorts,
unrelated worker debt, missing API refusal before mutation, failed quiescence,
failed preflight and failure after uncordon. The latter must re-cordon without
stopping potentially newly accepted work. An unconfirmed failure-recovery
cordon is reported explicitly. This is not a physical two-worker rollout.

Regression `8aeb3aaa-4ebf-4a69-a5f7-faacd00284a3`: **1,073 passed, 26 skipped**,
without a leak warning on this run. This does not resolve the earlier
intermittent warning. Build, lint (including all seven updater tests), shell
syntax, documentation and diff whitespace checks passed. No host changes.

### Effective Admission and Startup Provenance

Worker maintenance status now separates session liveness from effective
worker-wide admission. The snapshot reports registry/cordon/scheduler policy,
session state/TTL, key revocation, negotiated task support and protocol floor,
shutdown intent, active attempts and pending cleanup. `wait-worker --until
schedulable` requires explicit uncordon and pins the session/generation. It
fails on an older API without admission data and does not reserve capacity or
promise job-specific placement compatibility.

Initial targeted run `d79672a2-43fc-4f19-9a57-91143adb6419` had two CLI fixture
failures: expected raw JSON omitted the new optional `admission: null` field.
After correcting those fixtures, `454a9d41-78cb-4d90-b2d0-7d4c402a308e` passed
17 tests. Full admission regression `a6cdac81-fdb5-4416-b9f3-7b099e7d4a41`:
**1,076 passed, 26 skipped**, no leak warning on this run.

Successful startup preflight now reports its check revision, completion time,
running executable digest and default-expanded configuration digest. Registration
stores the optional report as immutable session facts; recovery cannot alter or
remove it. Previous workers remain explicitly unreported. This is self-reported
provenance, not remote attestation or authorization. The executable hash does
not establish a source commit without retained operator build evidence.

Focused provenance/lifecycle run `04542428-e8b1-468e-9e8f-af6a06c60a53`:
**30 passed**. Final regression `ce1d7e9a-29b7-4924-8ba0-975a676891a9`:
**1,083 passed, 26 skipped**, without a leak warning on this run. The earlier
intermittent leak observation remains unresolved. Tests cover invalid reports,
configuration changes versus formatting/key-file contents, failed resource
preflight, immutable registration facts, API serialization, CLI absence/reporting
and unchanged evidence through worker recovery.

Pinned runtime/schema gate `c15b33ea-9fc6-4640-b0d4-9fb0575da8e3`: **2 passed**.
The current worker's report remained unchanged across both restart cases; the
exact predecessor remained unreported and compatible. The separate-database
restore boundary still passed with the added nullable provenance migration.
Build, lint, Buf lint/breaking checks and whitespace checks passed. Local
checks do not replace the host ceremony. No deployment, staging or commit.

### Audited Explicit Fleet Abort

Fleet abort requires an explicit scope, a paused scheduler generation, a reason
and an idempotency UUID. Its immutable receipt records selected and accepted job
IDs; accepted cancellation is not proof of completed teardown. Replaying the
same request returns the original receipt even after resume, without selecting
new work. Signals do not invoke this operation.

Cancellation now also handles expired attempts awaiting cleanup: it suppresses
requeue but leaves the job running until cleanup acknowledgement. Production
Postgres tests cover both abort/resume and abort/cleanup lock orders, audit
failure rollback, immutable receipts and replay. HTTP/CLI tests cover admin
authorization, explicit scope, authenticated actor and truthful cleanup status.

Focused regression `3edb6696-e67a-45be-84c3-794e2059481b`: **23 passed**.
Full regression `af265973-a21d-4cf6-9700-2445a5dcc385`: **1,092 passed,
26 skipped**, without a leak warning on this run; the earlier intermittent
warning remains unresolved. Lint passed. Pinned runtime/schema gate
`6be5f4ac-b463-4273-8118-dc7095302d74`: **2 passed**, including the separate
database restore boundary with the new abort audit migration.

These are local checks, not a production abort or provider-convergence drill.
Host qualification remains open. No deployment, staging or commit.

### Terminal Provider Projection Reconstruction

Six coordinator tests now cover successful, failed and cancelled terminals at
two interruption points: after durable terminal acceptance but before provider
projection, and after the provider update but before its database acknowledgement.
They use production PostgreSQL stores, job reconstruction, coordinator projection
and report rendering, with a recording GitHub adapter. Reconstructed coordinators
start with empty reporting caches and reuse the persisted Check Run identity.

The acknowledgement-loss case injects a database failure after the external
update. Replay issues another update to the same Check Run, then advances the
durable projection cursor once. Further reconstruction or duplicate terminal
submission produces neither another update nor another execution. This proves
convergence of a single provider object, not exactly-once HTTP delivery.

Initial focused run `61243070-eb30-464a-a9d0-1509e77cfdbd` exposed two test
assertion decoding errors: PostgreSQL enum columns needed explicit text casts.
After correcting the assertions, the expanded focused run
`2f059dcb-aee6-4c22-9cb7-9ba7db6c77e4` passed **7 tests**, including the existing
process-local shutdown test. No production behavior changed in this slice.
Full regression `c393069e-82fd-4b4a-8d65-b9fd1cafae39`: **1,098 passed,
26 skipped**, without a leak warning on this run. Lint passed. The earlier
intermittent leak observation remains unresolved.

These tests do not exercise a full daemon process restart, live GitHub/Slack,
artifact promotion, or comparison/repeat results. Those qualification gates
remain open; the successful benchmark fixture has no measured performance data.

### Coordinator Startup Grace Composition

Two PostgreSQL-backed tests run the actual coordinator loop with H=3s and
G=6s, using database time for leases. An expired lease cannot heartbeat during
grace and remains unchanged until the scheduled sweep. A live lease renewed
during grace survives beyond its original deadline; subsequent expiry still
creates cleanup debt. Both paths expire once, reject later heartbeats, retain
the job until cleanup acknowledgement and preserve registry policy.

The test explicitly polls coordinator startup before observing grace. It uses
real timers rather than advancing Tokio time independently of PostgreSQL.
Focused run `7e488b49-3d2f-4141-b92c-93d73f5fda9d`: **10 passed**, including
terminal projection and process-local shutdown regression checks. Shared
coordinator fixtures reconstruct fresh adapters/caches for each instance.
Full regression `a530666f-c27f-4001-b1b5-bfb2caa2f797`: **1,100 passed,
26 skipped**, without a leak warning on this run. Build and lint passed;
the previously observed intermittent warning remains unresolved.

This covers local coordinator/store composition, not the complete daemon
listener/signal topology, delayed provider calls or physical teardown timing.
The host G/E/Q/K qualification and production startup ceremony remain open.

### Artifact Promotion Recovery Composition

Three tests combine production PostgreSQL terminal/manifest persistence, the
service's promotion function and real local MinIO. They reconstruct persistence
and S3 adapters with empty local mirrors after terminal acceptance, after a
partial two-object batch, and after both copies succeed but the database
promotion acknowledgement fails. The partial-batch case temporarily removes the
second staged object and restores its checksum-bound upload before retry.

Reporting remains gated while promotion is pending. Recovery reuses the exact
accepted manifest, verifies already-copied logical objects after staging removal,
and acknowledges both objects, including a zero-byte artifact. Fresh-reader GETs
match the original bytes. Duplicate terminal submission remains idempotent and
does not create another execution. Shared MinIO/store fixtures avoid separate
test-only promotion logic; nextest starts MinIO for these targeted tests.

Focused run `7dc490d0-2fcc-4898-b490-79f56f50957b`: **13 passed**, including
startup-grace and terminal-projection regression checks. This is storage/service
composition, not process interruption of the complete daemon, its maintenance
timer or a deployed S3 endpoint. Those host gates remain open.
Full regression `2045a840-feb4-4dea-a8b7-318adadade82`: **1,103 passed,
26 skipped**, without a leak warning on this run. Initial lint rejected the
extracted fixture because the kernel-boundary guard recognizes function-level
`#[cfg(test)]`, not its enclosing module annotation. The helper now explicitly
declares that boundary; the guard was not relaxed.
Build, lint and documentation checks subsequently passed. The earlier
intermittent leak observation remains unresolved. Nothing deployed or staged.

### Full Daemon Process Signals and Artifact Recovery

The `daemon_lifecycle` integration binary launches the shipped daemon executable
with isolated configuration, generated disposable keys, loopback listeners,
PostgreSQL and MinIO. The child environment excludes deployment secrets; Slack
and LLM intake are disabled and the GitHub endpoint is an unused loopback port.
Every child is explicitly reaped, including failed-start and test-error paths.

Three tests deliver actual Unix signals: SIGTERM, first SIGINT, and second
SIGINT while an incomplete authenticated HTTP body holds graceful shutdown open.
The second interrupt exits before the configured ten-second grace. All paths
leave the accepted attempt, worker registry, cleanup count and paused scheduler
policy unchanged. Restart over the same database preserves those facts and
rotates the admin cookie: the previous cookie is rejected and the new one works.
Signal reception now logs the signal and whether it forces local exit.

A fourth test starts the complete daemon after accepting a terminal with a
verified S3 artifact. Its maintenance loop copies/removes staging, hits an
injected promotion-acknowledgement failure, and leaves terminal projection
blocked. After a real daemon stop/start, the fresh maintenance loop verifies
the existing logical object and commits promotion; coordinator projection then
advances once. Fresh-reader bytes match and the job still has one attempt.

Initial run `c78f740f-e54b-497e-be9f-5bfd7915eebf` failed before child startup:
the new snapshot query used the wrong scheduler table name. Corrected signal
run `a1ff4938-7869-4673-a013-8bbd7d2c1d2c`: **3 passed**. Expanded process run
`a9764555-b7ea-4442-90eb-f94d86693d6a`: **4 passed**. The final artifact check
also requires the daemon log to show the injected acknowledgement failure.
Full regression `a4c5dc34-b6f7-433b-98f0-e0835692e57a`: **1,107 passed,
26 skipped**, without a leak warning on this run. Lint passed. The earlier
intermittent leak observation remains unresolved.

These are local full-daemon process tests, not running worker/VM qualification.
The active attempt is seeded through production stores; no actual workload is
executing. Provider projection in the artifact test is silent build-only
reporting, not live GitHub or Slack. Production rollout and physical-host
failure/teardown evidence remain open.

### Second-Worker Functional Test Scope

The operator confirmed only one physical Hetzner worker is deployed and proposed
a local VM/container as an initial second-worker test. A separate Linux VM with
its own identity, libvirt environment and LVM storage can provide functional
multi-worker evidence before a second machine is available. Nested VM execution
must be verified before relying on that environment. Sharing production libvirt
or LVM resources with a privileged container is not an isolated test boundary.

Record same-host VM tests separately from physical-host failure isolation and
benchmark performance qualification. No second worker has been provisioned and
the second-physical-host gate remains open. Do not silently count the existing
mock-command worker probes as real libvirt/LVM execution.

### Validation Initialization and Processing Progress

The carried reporting finding exposed an implementation gap: the guest emitted
`validate 0/N` during command initialization and no progress while discovering
the range. It now emits total-less `initialize` snapshots before each probe and
labels commands without positive counters as initializing or waiting for
processed-block progress. This does not infer internal producer state from
silence or count initialization work as validated blocks.

An optional processing-only ETA uses at most sixty one-second intervals from
the same active command set. It requires ten seconds, three positive advances,
half-window rates within a factor of two, no queued/next commands and no
initializing commands. Ten seconds without an advance suppresses it; command-set
changes reset sampling. The initial scan and teardown never enter this rate.
It remains best-effort message text in the existing progress contract, not a new
protocol field or a change to terminal coverage/validation authority.

The Python suite exercises the actual guest functions and pure helper: probe
ordering, long initialization, silent completion, bounded samples, unstable
rates, stalls, command changes, pending epochs, five-second debounce and
nonfatal progress-file failures. Driver fixtures additionally forward total-less
initialization, command startup and processed counters with run attribution.
Live guest/provider evidence and performance accuracy remain unqualified.

Validation: **17 Python tests passed**; focused nextest
`2b6cb4da-4782-4612-83b3-8c359c727d67`: **44 passed**. Full regression
`911b6e1d-cff3-4cfb-8829-97e4dcbd5869`: **1,107 passed, 26 skipped**, without
a leak warning on this run. Build, lint, documentation checks and diff whitespace
checks passed. These are local results, not deployment or second-worker evidence.

### Release Compatibility Decision — Resolved 2026-10-17

The original v35 final gate required previous-daemon operation against the
expanded schema. The exact predecessor migration probe contradicted that gate:
startup rejects the new ledger. Isolated backup restoration proves a different
recovery path and cannot silently replace the requirement.

Read-only source inspection of `0f18950` additionally confirms that its
`prepare_execution` and `poll_offer` lack the new scheduler policy lock/check;
offers also lack protocol-floor and session-targeted shutdown admission.
Disabling migration verification would therefore not produce a safe bridge.
On 2026-10-17 the operator explicitly approved backup/restore-only rollback for
the one-host rollout, restoring PostgreSQL and matching binaries/configuration
together. Roll-forward fixes are preferred. This supersedes the old-daemon-on-
expanded-schema gate; the current/previous worker contract is unchanged and no
bridge release is required.

The host restore/reconciliation drill remains open. Preserve failed-state data
and account for post-backup work and external provider effects before any restore
is returned to service. The [recovery policy](../../docs/worker-fleet-operations.md#recovery-policy-for-the-v35-schema-rollout)
records the operating constraints. No deployment or restore was performed.
Second-worker placement and operator-run host/provider evidence remain open.
