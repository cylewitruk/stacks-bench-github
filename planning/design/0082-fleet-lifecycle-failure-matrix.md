# Fleet Lifecycle: Failure Matrix and Timing Bounds

Working Phase 1 contract for [v35](../iterations/v35-seamless-fleet-lifecycle.md),
implementing [Decision 0006](../decisions/0006-fleet-lifecycle-state-separation.md).
Direction accepted 2026-10-14. The matrix states required behavior; the local
implementation section records the tested subset. Host configuration and installed binary hashes were operator-reported
on 2026-10-14. The subsequent
[October 15 qualification](../qualification/2026-10-14-first-fleet.md#v35-deployment-and-restart-qualification--2026-10-15)
records the deployed release, running executable hashes, idle recovery, and
active validation continuity. Effective runtime overrides and previous-release
compatibility fixtures remain open.

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
Audited scheduler pause, resident cordon/shutdown-after-idle, protocol negotiation,
previous-revision fixtures, fleet-wide abort controls, and second-host operation
remain later v35 work; the full iteration is not complete.

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
scheduler pause is not yet available; a quiet queue alone does not prevent new
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
an unstructured `Unknown` reconnect error was treated as permanent. Keep F04–F07
host recovery open until the corrected build passes a repeat canary.

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
