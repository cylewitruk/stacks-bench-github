# v34: First Fleet Deployment Readiness

> **Status:** superseded — closed as an iteration on 2026-10-14 and replaced by
> [v35](../../iterations/v35-seamless-fleet-lifecycle.md). This is a scope
> handoff, not a claim that every qualification gate passed.

## Outcome and Handoff

The first same-host deployment proved separate daemon/worker processes,
sandbox and LVM isolation, production identity/transport, controlled and local
multi-shard validation, cancellation, and GitHub benchmark execution. The
[dated evidence record](../../qualification/2026-10-14-first-fleet.md) records
the demonstrated outcomes and their limits.

All unfinished item definitions and acceptance work now live in v35's
[carried qualification](../../iterations/v35-seamless-fleet-lifecycle.md#carried-qualification):
`0080-first-fleet-deployment-qualification`,
`0027-fine-grained-progress`, and `0039-multi-variant-benchmark-comparisons`.
Their statuses did not become shipped when this iteration closed.
v35 prioritizes lifecycle reliability, then completes the carried host gates.

The sections below preserve the historical design, evidence, playbook, and
checkbox state at handoff. They are not a second active work queue. Follow v35
for current ownership and sequencing, and the
[deployment playbook](../../../docs/deployment-qualification.md) for host
commands.

## Design Rules

- **Prove the production path.** Qualification uses the shipped daemon API,
  task-submission kernel, pull scheduler, gRPC transport, worker runtime,
  libvirt driver, and report projector. Do not add a host-execution fallback or
  a second diagnostic executor.
- **Isolate substrate risk before provider risk.** Use a tiny typed admin
  block-validation submission first. It must boot the real guest, attach a
  writable snapshot of the read-only origin, run the actual numeric probes,
  validate a small range, reduce the result, and clean up before GitHub or
  Slack is introduced.
- **Audit the executed validation commands.** Emit the exact argv before every
  guest `stacks-inspect` spawn, retain command-specific logs, and have the
  trusted reducer reconstruct the expected epoch-local half-open ranges. A
  terminal result is valid only when every command and its reported processed
  count exactly cover the trusted global plan.
- **Keep processes and authority separate even on one machine.** The daemon and
  worker use different service users, configuration, units, filesystem
  ownership, and privileges. The daemon receives no libvirt, LVM, mount, or
  worker-key authority.
- **Compose existing qualification tools.** Reuse the sandbox-network, LVM,
  chainstate, preflight, API, and fleet-status commands. Add small scripts only
  for missing repeatable checks; do not create a monolithic deployment shell
  orchestrator.
- **Record evidence, not narrative.** The operator playbook is a present-tense
  checklist with exact commands, expected outcomes, failure stops, rollback,
  and a qualification-record template.
- **Inventory before touching a host.** Every playbook action must name its
  backing script, unit, config, or external/manual prerequisite. An unmapped
  action is a visible documentation/tooling gap, not an assumption discovered
  halfway through commissioning.
- **Fail closed between gates.** Do not enable provider-triggered demand until
  the preceding substrate and controlled canaries pass. A failed or ambiguous
  gate leaves the worker drained.
- **Keep VM placement worker-owned.** Discover host VM capacity from Linux's
  online CPU set rather than the housekeeping-confined adapter process. Validate
  guest and emulator CPU sets locally and reject daemon-supplied placement.
- **Use one dual-capability process first.** A checked-in combined profile
  proves capability inference and the common sandbox model. Dedicated workers
  remain an operator policy choice, not a separate code path.
- **Do not mix measurement environments silently.** Comparison submissions
  remain on one compatible worker/execution generation. A failed partial group
  requires explicit recovery from the first spec/run.
- **Treat cancellation narrowly.** The v34 cancel canary proves durable
  cancellation, worker observation, teardown, fencing, and reporting. It does
  not claim that user-facing Slack/GitHub lifecycle controls exist.
- **Freeze the first deployed protocol baseline.** After successful deployment,
  pin semantic-digest vectors for the deployed protobuf revision. This protects
  future edits; it does not preserve an undeployed JSON protocol.
- **Keep the deployment milestone independent.** `0080` and the declaration
  that the first fleet is live depend on the substrate, topology, primary
  journeys, recovery, and identity gates. The carried `0027`/`0039` closure is
  a v34 fast-follow and may keep the iteration open, but a progress-format or
  comparison-polish issue cannot make an otherwise qualified fleet undeployed.

## Deliverables

- Daemon-only and worker-only installers with idempotency and ownership tests.
- A parse-tested combined benchmark + block-validation worker example.
- Corrected setup/operations terminology for the v31 identity model.
- A canonical `docs/deployment-qualification.md` playbook linked from setup and
  fleet operations.
- Any small, safe qualification checks needed to compose existing scripts.
- A completed first-deployment qualification record with identifiers and
  cleanup evidence; secrets and tokens must never enter the record.

## Host-Independent Progress

- [x] Map every host action to a checked-in asset or explicit manual/external
  prerequisite.
- [x] Split daemon-only and worker-only installation with staging-root
  ownership/idempotency tests.
- [x] Apply the worker hardening drop-in to every worker profile.
- [x] Add a parse-tested combined-capability worker example.
- [x] Correct active docs to the public Web-PKI + worker-key identity model.
- [x] Add the ordered deployment-qualification playbook and typed CLI report
  view used by its canaries.
- [x] Complete local build, lint, and test validation for this slice.
- [x] Correct first-host CPU discovery and placement after isolated CPUs made
  process-affinity capacity under-report the VM host.
- [ ] Execute the real-host gates below.

## Real-Host Progress — 2026-08-07

The first deployment is a separate-process, same-host daemon/worker topology on
`stacks-bench`. This historical record is retained as reported. The
[October reconciliation](../../qualification/2026-10-14-first-fleet.md) adds later
evidence and identifies missing raw records; it does not rerun these checks.

### Completed evidence

- [x] Created a protected rollback bundle, verified a restorable PostgreSQL
  backup, rehearsed every pending migration against a scratch restore, and
  retained an off-host checksum-matched backup copy.
- [x] Installed daemon-only and worker-only artifacts without crossing their
  ownership boundaries; the daemon has no worker libvirt/LVM/sudo authority.
- [x] Installed a public Web-PKI IP-address certificate, verified TLS 1.3 +
  HTTP/2 through the production worker connector, and passed Certbot's renewal
  dry run plus the daemon certificate deployment hook.
- [x] Qualified the managed sandbox network from a disposable guest, including
  dependency egress and host/private/metadata/operator-address denial.
- [x] Qualified two simultaneous writable LVM snapshots, origin and peer write
  isolation, complete snapshot cleanup, and an inactive read-only chainstate
  origin (`vg0/mainnet-2026-07-04`).
- [x] Passed the combined worker's production preflight, enrolled one P-256
  identity, and registered worker
  `11db06ad-fe64-457b-ac2b-b495c6628989` with the intended capability
  intersection and measurement profile.
- [x] Completed the controlled one-block canary with one resolved Nakamoto
  block, one processed block, promoted checksum-verified artifacts, and no
  leaked execution resources (submission
  `fd86d74d-5eba-4c39-80ff-48a6f2410621`, job
  `0c9350c3-349a-4d18-9965-8b795d5b572a`, attempt
  `33386a78-e2c3-49d8-af59-782209ad334c`).
- [x] Completed recent block-validation canaries with exact command records,
  command-specific stdout/stderr artifacts, trusted processed-count reduction,
  delegated artifact upload, terminal promotion, and resource cleanup. The
  audit canary used submission `0958e0d7-2736-4336-abee-02ee4b4bafc6`, job
  `7a01427a-7a84-49f6-8fee-74712bdb3c14`, and attempt
  `007d9f97-e6c7-4123-a90c-51a0cda52385`.
- [x] Completed an explicit two-block cross-epoch canary with adjacent Epoch 2
  and Nakamoto segments and exact trusted coverage (submission
  `58a16d6b-eef2-4261-834f-aa710f2e2496`, job
  `407adf75-9b59-47a0-a9db-b155c64a1c58`).
- [x] Completed GitHub `/validate` intake from an allowed fork. Its marked PR
  comment and `stacks-block-validation` Check Run were persisted before
  assignment, updated in place through execution, and converged to success.
  Canonical identities include submission
  `e538271c-5646-4a43-94ef-fa81858b2a2e`, job
  `2db866d1-8351-414e-8e3a-975120a9a80e`, comment `5201711352`, and Check Run
  `92549878067`.
- [x] Redelivered that GitHub webhook (`82403bb0-9168-11f1-954b-413c8d9eeec6`).
  Ingress retained one processed webhook row and reused the same submission,
  completed job, comment, Check Run, and external identity without enqueuing
  duplicate work or creating another provider surface.
- [x] Completed a natural-language Slack recent-validation canary against an
  immutable commit. One canonical threaded snapshot was created before work
  was offered, updated in place through queued/running phases, and converged to
  the terminal block-validation result without an event transcript or a second
  message.
- [x] Proved live block-weighted validation progress from guest
  `stacks-inspect` output through worker aggregation, protobuf fleet progress,
  daemon projection, and the same Slack snapshot. The 1,000-block canary
  (submission `42a67e3d-878d-4ada-8024-994f49186ff1`, job
  `fa66a893-cf19-454e-9fce-78429e1fcbdf`, attempt
  `92fe7501-dc92-4270-89c4-db2440df1b3f`) produced monotonic exact-block
  updates and converged the same Slack snapshot to a valid terminal result:
  `1,000/1,000` blocks, range `8476060..=8477059`, one shard, one concurrent
  validator, and origin `vg0/mainnet-2026-07-04`.
- [x] Completed the forced five-shard recent-validation canary under the
  temporary 2,000-block shard target and five-shard/four-concurrent ceiling.
  Job `195ac363-9ab1-491c-9d65-c998f4f319ab` and attempt
  `518bf860-8c61-4b62-846f-e72b8b40be1d` validated the exact 10,000-block
  range `8467060..=8477059` with a valid five-shard reduction and one canonical
  Slack snapshot. The trusted reducer accepted the exact gap-free command
  partition and processed counts; all 17 command/result/console artifacts were
  checksum-verified and promoted. All five snapshots, mounts, tmpfs/job
  directories, attempts, cleanup obligations, and reliable-event gaps cleared.
  The production profile was restored and the worker returned online and idle.
- [x] Cancelled an active Slack block-validation canary through the admin fleet
  command. Job `c953da9c-34dd-46c9-a661-84c8152690fa` and attempt
  `cb00a2fa-328f-4dfd-80c6-86d953cad538` reached `validating`, observed the
  durable cancellation, stopped the VM, completed teardown, and updated the
  canonical Slack reply in place to `Cancelled`. The fleet subsequently
  returned to zero attempts, cleanup obligations, and reliable-event gaps.
- [x] Retained deterministic Slack retry/idempotency coverage instead of
  fault-injecting the production Socket Mode connection. Duplicate dispatch of
  one stable request identity, a lost post response, and failure to persist the
  returned message timestamp are tested to reconcile to one submission and one
  canonical message. The live canary independently proved that normal Socket
  Mode delivery created and updated only that message.

## Reconciliation — 2026-10-14

- [x] Record September's scratch backup restore and submission/spec/run
  consistency checks, quiescent updates, and production transport/preflight.
- [x] Record successful one-block validation on the newer September chainstate,
  the schema-v3 command artifact, and observed attempt/resource cleanup.
- [x] Complete the GitHub benchmark journey: September 24/25 jobs posted their
  terminal results, retained DB/summary/progress artifacts, and returned the
  fleet to idle with zero attempts/cleanup/gaps.
- [x] Record `develop2` baseline execution and the later 20,000-measured-block
  baseline/optimized pair. These are separate jobs, not the `0039` integrated
  comparison canary.
- [x] Consolidate the available August–October evidence and give every remaining
  gate an owner/checkpoint in the [qualification record](../../qualification/2026-10-14-first-fleet.md).

### Open or partial gates

- [ ] Complete raw evidence references and operator sign-off, including exact
  deployed release/image/tool versions, missing benchmark provider/attempt
  identities, checksums, and the direct S3 put/HEAD/get/checksum/delete ceremony.
- [ ] Complete the Slack benchmark provider journey. The v20/v22 benchmark
  progress and integrated comparison closure remains a separate fast-follow.
- [ ] Distinguish the long first-block chainstate range-discovery/initialization
  scan from steady-state block validation in provider UX. Until the first
  processed-block sample exists, report initialization rather than implying a
  time-linear `0%`; derive any ETA only from stable post-initialization samples.
  The 1,000-block canary spent about 883 seconds reaching its first processed
  block, then accelerated through the remaining range. This reporting
  refinement does not weaken terminal reduction or block the already-proven
  Slack validation journey.
- [ ] Run full-history validation in an explicitly scheduled maintenance window.
- [ ] Recovery drills: rescheduled to v35 Phases 2–3 and 7 (active/post-terminal
  restart, worker loss during execution/cleanup, and cleanup-obligation recovery).
- [ ] Identity drills: rescheduled to v35 Phases 6–7 (overlapping keys,
  normal/emergency revocation, capability/profile changes, policy persistence).
- [ ] Protocol baseline: capture deployed semantic-digest vectors in v35 Phase 1
  before Phase 5 changes protocol negotiation.
- [x] Explicitly defer second-machine commissioning to v35 Phase 6. Actual
  two-host operation remains unproven and is required before claiming readiness.

### Accepted finding carried into v35

- [x] Reschedule the lifecycle finding to v35: revision-1 shutdown
  persists a global drain and idle workers exit successfully. v34 records the
  conservative behavior; v35 (`0082`) owns the non-destructive lifecycle change
  and its recovery qualification. This checkbox records the handoff, not a
  successful transparent restart.

## Execution Playbook

### Phase 0: Host-Action Inventory

Before changing a host, build the first section of
`docs/deployment-qualification.md` as an inventory with these columns:

| Area | Backing asset | Host / authority | Required inputs | Pass evidence |
| ---- | ------------- | ---------------- | --------------- | ------------- |
| Control-plane install | daemon-only installer; `sbgh-daemon.service` | daemon host / root | release binaries | installed paths + active unit |
| Worker install | worker-only installer; `sbgh-worker@.service` | worker host / root | release binary + profile | installed paths; unit initially stopped |
| Database and backup | Docker Compose, migrations, `sbgh-pg-backup.*`, restore check | daemon host / root + `sbgh` | DB secrets/storage | migration + restore evidence |
| Daemon directories/secrets | setup checklist and systemd credentials/env | daemon host / root | API, lease, provider, S3 secrets | ownership/mode checks; redacted config load |
| Public TLS | external DNS/ACME or existing Web-PKI tooling | daemon/network operator | hostname, firewall policy | hostname-verified TLS chain |
| Worker user/directories | setup checklist | worker host / root | UID/GID and canonical paths | ownership/mode checks |
| Worker sudoers | checked command allowlist + `visudo` | worker host / root | installed command paths | `visudo -cf` + denied extra command |
| Sandbox network | `install-`, `apply-`, `check-`, and `qualify-sandbox-network.sh`; `sbgh-sandbox-egress.service` | worker host / root | protected CIDRs | live verifier + guest ceremony |
| Golden image | `build-golden-image.sh` | worker host / root | Ubuntu source/network | image digest + boot proof |
| LVM isolation | `qualify-block-validation-lvm.sh` | worker host / root | VG, thin pool, origin | two-snapshot isolation + cleanup |
| Chainstate | `download-chainstate.sh` or managed-node snapshot tooling | worker host / root | source/archive | newest inactive read-only LV |
| Worker configuration | checked benchmark, validation, and combined examples | worker host / `sbgh-worker` | local resources/paths | parse + production preflight |
| Worker identity/registry | `sbgh-worker identity`; `sbgh-cli fleet` | worker + daemon admins | private key/public SPKI/policy | authorized session + audit rows |
| Provider configuration | setup checklist + provider APIs | daemon/provider admin | GitHub/Slack credentials and policy | authenticated health + dry intake |
| Qualification record | `docs/deployment-qualification.md` template | operator | revision and canonical IDs | timestamped redacted record |

For each row, verify that the asset exists and matches current paths and
arguments. Manual/external work—especially DNS/ACME, daemon secret creation,
directory ownership, sudoers, firewall ingress, and provider credentials—must
have explicit commands or operator actions even when a repository script would
add no value. Record package/kernel/tool version assumptions exposed by the
scripts. Do not begin Phase 2 while a required row is unmapped.

### Phase 1: Packaging and Documentation Cleanup

- Split the current coupled installer into control-plane and worker entry
  points. Share common shell helpers only where they remove actual duplication.
- Make first worker installation explicit: install the template unit but do not
  guess a profile or start before config, preflight, enrollment, and policy.
- Add and parse-test a combined worker config.
- Remove private-CA/public-leaf wording and document public SPKI enrollment,
  proof-of-possession, Web-PKI daemon validation, key rotation, and revocation.
- Add shell/static tests proving each installer touches only its owned binaries
  and units.

**Gate:** local build, lint, tests, docs checks, installer tests, and config
parse tests pass before touching the host.

### Phase 2: Host and Sandbox Qualification

1. Record the host, OS/kernel, libvirt/QEMU/LVM/nft versions, release revision,
   golden-image digest, and newest chainstate LV.
2. Install/start the sandbox policy and require its live structural verifier.
3. Run the disposable-guest positive-egress/protected-destination ceremony,
   including the daemon's public endpoint where safely probeable.
4. Run LVM two-snapshot isolation qualification and prove origin/peer
   non-mutation plus complete cleanup.
5. Verify the selected chainstate origin is inactive/read-only and sufficiently
   current for the intended benchmark and validation ranges.
6. Run the combined worker's production preflight as `sbgh-worker`.

**Gate:** all checks pass on the intended host; otherwise keep the worker
disabled and drained.

### Phase 3: Separate-Process Same-Host Bring-Up

1. Install and start the daemon as its own service user with PostgreSQL,
   provider, artifact-store, lease, and public Web-PKI credentials.
2. Generate the worker P-256 identity as the worker user; enroll only its public
   SPKI through the admin API.
3. Require a standard gRPC `SERVING` response through the production TLS 1.3
   worker connector before enrollment, then create one disabled/drained
   registry row allowing benchmark, build-only, and block-validation; configure
   a measurement profile for benchmark work.
4. Install the worker separately, run preflight again, verify read-only
   registration readiness without creating a session, start its service, and
   verify authenticated gRPC registration, protocol revision, discovered
   resources, advertised/authorized capability intersection, and no offer while
   drained.
5. Enable and deliberately undrain only after all registry/session observations
   match the intended policy.

**Gate:** daemon restart preserves registry policy; worker restart preserves
logical identity while creating a new fenced session.

### Phase 4: Controlled Sandbox and Probe Canary

1. Submit a one-block `range` validation through `sbgh-cli jobs
   validate-blocks` with a unique idempotency key and immutable full commit.
2. Observe offer, accept, attempt, VM, snapshot, probe, validation, terminal,
   artifact promotion, and report projection using their canonical IDs.
3. Confirm recorded pre-Nakamoto/Nakamoto counts are sane for the selected
   origin and the resolved segment covers exactly one block. Retain the exact
   command audit record and require its final processed count to be one.
4. Submit a tiny range crossing the observed epoch boundary and require two
   exact, gap-free segments, two exact epoch-local command records, and summed
   processed counts equal to the global range.
5. Confirm domains, mounts, thin snapshots, temporary files, and staging
   artifacts are absent after both attempts.

**Gate:** probe output, exit-code classification, trusted reduction, and
cleanup all match the production contracts before provider traffic is enabled.

### Phase 5: Primary Provider Canaries

Run and retain evidence for:

1. GitHub `/benchmark` on an allowed PR.
2. GitHub `/validate` on an allowed PR with `TriggerBlockValidation`.
3. A natural-language Slack benchmark.
4. A natural-language Slack recent block validation.

For each, verify immutable commit resolution, source-specific authorization,
one canonical submission, compatible pull placement, VM isolation, terminal
task report, provider identity reuse, and promoted artifacts. Redeliver the
same GitHub webhook and require no duplicate submission, check, or comment.
Socket Mode has no operator redelivery control: retain deterministic automated
coverage for duplicate dispatch, lost post responses, and report-identity
persistence failures, while live Slack evidence must contain one canonical
message rather than an event transcript.

For the GitHub validation canary, keep the worker drained until the queued
submission has created and persisted its configured `stacks-block-validation`
Check Run and marked PR comment. Then undrain and require the same identities
to receive phase and terminal updates. Missing pre-assignment identities or
late duplicate surfaces fail the gate.

Rapid adjacent phase transitions must converge both GitHub surfaces to the
latest durable phase; provider-update debouncing must not acknowledge and drop
those transitions.

### Phase 6: v20 and v22 Benchmark Closure (Fast-Follow)

1. During a real single-ref benchmark, inspect live fine progress and archived
   JSONL; record calibration and total-less behavior.
2. Submit a natural-language two-ref comparison over a small workload with one
   measured run per variant.
3. If healthy, repeat with two clean runs per variant to prove fresh VMs,
   per-variant calibration reuse, shared-DB carry-forward, noise aggregation,
   and progress reset/attribution.
4. Restart the daemon after at least one durable event and require the same
   final aggregate/provider snapshot after replay.

**Gate:** every acceptance check under `0027` and `0039` is evidenced or an
observed upstream limitation is explicitly accepted without weakening terminal
correctness. This closes those items but does not gate `0080` or the declaration
that the first fleet is live.

### Phase 7: Extended Validation and Recovery

1. Temporarily configure a 2,000-block shard target, five-shard ceiling, and
   four-way concurrency, then run a 10,000-block recent validation. Require
   five exact 2,000-block ranges, four concurrent validators followed by the
   fifth shard, block-weighted live progress across active and completed
   shards, a 10,000-block terminal reduction, and cleanup of every snapshot and
   result directory. Restore the commissioned worker profile and rerun
   preflight afterward.
2. Run default recent validation and confirm it saturates against the observed
   Nakamoto count when necessary.
3. Run full-history validation and require both epoch segments and a negative
   verdict only after every shard exits normally.
4. Run another explicit cross-epoch range and verify exact shard-plan/reducer
   agreement.
5. Cancel a running validation with the admin fleet command; require prompt VM
   stop, complete teardown, a fenced cancelled terminal, and converged report.
6. Restart the daemon during an active attempt and again after terminal event
   acceptance; require session/lease recovery and replay convergence.
7. Kill a worker during execution and during cleanup; require expiry/fencing,
   visible cleanup obligation, and no unsafe automatic cross-worker comparison
   continuation.

The daemon-restart drill records the conservative revision-1 baseline and its
fencing/replay evidence. It does not endorse global drain/abort as the durable
operations contract; v35 (`0082`) supersedes that lifecycle behavior.

### Phase 8: Identity and Dynamic Registry Drills

- Authorize an overlapping replacement public key, restart under it, verify the
  same worker UUID, then revoke the old identity.
- Prove normal revocation and emergency revocation reject the next RPC on an
  already-open connection according to their lifecycle contracts.
- Drain, change authorized capabilities/profile, start a new session, and prove
  the advertisement intersection cannot expand server policy.
- Restart the daemon and prove policy, identity history, and drain state remain
  database-authoritative.

### Phase 9: Second-Machine Commissioning

- Install only worker-owned artifacts on a second machine.
- Repeat host/sandbox/LVM/preflight checks, generate and enroll a distinct key,
  and register on the same protobuf revision.
- Prove capability-aware pull scheduling and independent drain/revocation.
- Do not introduce rolling-version negotiation merely to add this worker;
  `0075` becomes mandatory before the first incompatible protocol change while
  independently upgraded workers exist.

## Required Validation

### Core Fleet-Live Gate (`0080`)

- [x] `just build --no-sccache`
- [x] `just lint --no-sccache`
- [x] `just test --summary --no-sccache`
- [x] `git diff --check`
- [x] Installer isolation/idempotency tests pass.
- [x] Daemon, benchmark-worker, block-validation-worker, and combined-worker
  examples remain parse-tested.
- [x] Sandbox policy service starts and its live post-start verifier passes.
- [x] Disposable-guest network and two-snapshot LVM ceremonies pass.
- [x] Combined-worker preflight and database enrollment pass.
- [x] Controlled one-block and cross-epoch sandbox/probe canaries pass with
  exact argv and processed-count audit evidence.
- [x] GitHub validation canary passes with one marked comment and one Check Run
  updated in place through terminal success.
- [x] GitHub benchmark canary passes (September 24/25 evidence in the
  [qualification record](../../qualification/2026-10-14-first-fleet.md#github-benchmark-evidence)).
- [x] Slack recent-validation canary passes with one canonical threaded
  snapshot updated in place through terminal success.
- [x] Slack retry/idempotency paths reconcile duplicate dispatch and ambiguous
  post/persistence outcomes to one submission and snapshot in deterministic
  automated tests; the live canary creates no duplicate message.
- [ ] Slack benchmark canary passes.
- [x] Live block-validation progress reaches the protobuf/snapshot path as
  monotonic exact-block counts without changing terminal authority.
- [x] A forced 10,000-block canary executes as five exact shards with four-way
  concurrency and proves live aggregation, terminal reduction, artifacts, and
  five-snapshot cleanup end to end.
- [x] Controlled active-attempt cancellation converges the canonical provider
  report and leaves no VM, snapshot, mount, cleanup obligation, or event gap.
- [ ] Full-history validation passes (v34 maintenance-window gate).
- [ ] Daemon restart, worker loss, and cleanup recovery pass (v35 Phases 2–3/7).
- [ ] Identity rotation/revocation and restart persistence pass (v35 Phases 6–7).
- [ ] First deployed protobuf revision has pinned semantic-digest vectors
  (v35 Phase 1 prerequisite to Phase 5).

### Carried Feature Closure (`0027` / `0039`)

- [x] Completed real benchmark jobs retain `run.progress.jsonl` as a promoted
  artifact (September 24/25 listings).
- [ ] Live `stacks-bench` progress is observed through the fleet and provider
  snapshot path with correct attribution; archived JSONL is inspected.
- [ ] Calibration/repeat attribution, optional fallback, debounce, and terminal
  precedence pass on the host.
- [ ] A two-variant comparison proves serial execution, per-variant
  calibration, carried DB, repeated-run aggregation, artifacts, and one report.

### Multi-Machine Readiness

- [x] Second-machine commissioning is explicitly deferred to v35 Phase 6.
- [ ] Second-machine commissioning passes; multi-machine readiness remains
  unclaimed until this gate has real evidence.

## Stop and Rollback Rules

- Keep provider triggers disabled and workers drained after any ambiguous
  authorization, fencing, cleanup, result-reduction, or provider-idempotency
  outcome.
- Stop a canary before retrying if its prior attempt still owns a live domain,
  mount, snapshot, lease, staging object, or provider identity.
- Roll back daemon and workers as a coordinated revision while exact protocol
  matching remains required. Restore the matching database backup when the
  failed revision applied a schema change that the retained binary cannot read.
- Do not bypass sandbox verification, make a base chainstate writable, relax
  worker identity authorization, accept a partial negative validation verdict,
  or mix comparison measurements across worker generations to make a gate pass.

## Closure

v34 is archived as superseded. Full qualification and multi-machine readiness
remain unclaimed until the carried v35 gates pass. Historical unchecked boxes
above remain unchecked; closing this document does not waive them.
