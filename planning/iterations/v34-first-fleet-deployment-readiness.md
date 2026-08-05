# v34: First Fleet Deployment Readiness

Successor to
[v33](../archive/completed/0079-dynamic-block-validation-planning.md).
Turn the locally validated daemon/worker architecture into one repeatable,
evidence-backed deployment on real hardware. This iteration also closes the
remaining host-verification work from v20 and v22 against the current fleet and
snapshot-reporting architecture.

> **Status:** in_progress — host-independent inventory, packaging, config, and
> playbook work is implemented locally; real-host gates remain open.
>
> v34 is a deployment-readiness and qualification iteration, not a new task
> feature. Fixes discovered by the playbook are in scope; unrelated feature
> expansion is not.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0080-first-fleet-deployment-qualification` | primary: installation, commissioning, canaries, and evidence | in_progress |
| `0027-fine-grained-progress` | co-primary: real-host progress verification | planned |
| `0039-multi-variant-benchmark-comparisons` | co-primary: real-host comparison verification | planned |

## 0080 — First Fleet Deployment Qualification

- **id:** `0080-first-fleet-deployment-qualification`
- **status:** `in_progress`
- **priority:** `high`
- **depends_on:** `0004-worker-fleet`, `0017-generic-phase-events`,
  `0062-sandboxed-worker-execution`, `0063-libvirt-block-validation`,
  `0066-task-aware-reporting`, `0067-github-block-validation-submission`,
  `0074-protobuf-fleet-protocol`, `0076-database-backed-worker-registry`,
  `0077-worker-identity-and-config-simplification`,
  `0079-dynamic-block-validation-planning`
- **relates_to:** `0027-fine-grained-progress`,
  `0039-multi-variant-benchmark-comparisons`,
  `0075-rolling-worker-protocol-compatibility`
- **source:** first-deployment readiness review after v33 (2026-08)

**Problem:** Submission, fleet execution, sandboxing, identity, and reporting
are locally validated but have not run as separately managed daemon and worker
processes on a production-like host. Installation currently couples control
plane and worker artifacts, a few identity instructions still describe the
retired private-CA model, and the libvirt/LVM/probe contracts have no real-host
evidence.

**Scope:** Separate daemon and worker installation, provide one parse-tested
dual-capability worker example, correct current identity documentation, and
publish a rerunnable qualification playbook. Commission one same-machine
daemon/worker topology, prove the sandbox before provider traffic, exercise
benchmark and block-validation submission from GitHub and Slack, verify
reporting/replay/cancellation, and then repeat worker commissioning on a second
machine using the same protocol revision.

**Acceptance:**

- Daemon-only installation never installs or starts a worker; worker-only
  installation never installs or starts the daemon or operator CLI.
- A combined worker example advertises benchmark, build-only, and
  block-validation capability from its recipe sections and passes preflight.
- Setup and operations docs describe public Web-PKI server authentication plus
  worker P-256 proof-of-possession and database authorization; no private
  worker CA or certificate enrollment remains.
- The managed network, golden image, LVM snapshots, read-only chainstate
  origin, guest probes, cleanup, public-key enrollment, and gRPC session all
  pass on the real host before provider canaries.
- GitHub and Slack can each submit and report a benchmark and a block
  validation without duplicate work or provider messages.
- Recent, full-history, explicit cross-epoch, cancellation, and daemon-restart
  canaries preserve fencing, exact coverage, cleanup, and report convergence.
- A second worker can be installed, enrolled, authorized, and scheduled without
  a daemon restart or a static fleet file.
- Every gate records the command/revision, timestamp, outcome, and relevant
  submission/job/attempt/provider identities in a durable qualification record.

**Deferred / non-goals:** No lifecycle UX (`0065`, `0070`, `0071`), watched-ref
automation (`0068`), rolling multi-revision protocol support (`0075`), package
repository, auto-enrollment, common chainstate distribution, multi-orchestrator
HA, performance certification, or new task kind.

## 0027 — Fine-Grained Benchmark Progress: Real-Host Closure

- **id:** `0027-fine-grained-progress`
- **status:** `planned`
- **priority:** `medium`
- **depends_on:** `0017-generic-phase-events`,
  `0060-slack-snapshot-reporting`, `0074-protobuf-fleet-protocol`
- **relates_to:** `0039-multi-variant-benchmark-comparisons`
- **source:** v20 implementation audit during v34 planning (2026-08)

**Problem:** Fine-grained `stacks-bench` JSONL parsing, bounded delivery,
archiving, fleet transport, and reporting are implemented and tested, but the
current pinned binary has not proven the complete path inside a real libvirt
guest. Calibration/repeat attribution, total-less events, and final-line drain
remain empirical.

**Scope:** Exercise the existing production path during benchmark canaries.
Confirm workflow-step and run-index attribution, bounded/debounced snapshot
updates, optional fallback for a silent or older producer, raw JSONL artifact
retention, and terminal precedence. Record whether calibration emits progress
and whether guest shutdown loses a final best-effort line; fix only defects
that violate the current contract.

**Acceptance:**

- A real benchmark emits parseable progress from the guest and reaches the
  daemon through the protobuf fleet path without delaying reliable phase or
  terminal events.
- Calibration and measured-run events, when emitted upstream, carry the correct
  workflow step and repeat index; a silent calibration remains a supported
  coarse-phase fallback.
- Slack renders bounded canonical snapshots rather than an event transcript;
  GitHub/check reporting remains bounded and terminal state wins over delayed
  progress.
- Archived `run.progress.jsonl` and, when present,
  `calibration.progress.jsonl` match the executed attempt and remain useful for
  forensics.
- Missing, malformed, unknown-version, total-less, or backpressured progress
  never changes the task terminal outcome.

**Deferred / non-goals:** No return to Slack streaming/append semantics, no
reconstruction from console text, no change to upstream `stacks-bench`, and no
native result-envelope cleanup (`0050`).

## 0039 — Multi-Variant Benchmark Comparisons: Real-Host Closure

- **id:** `0039-multi-variant-benchmark-comparisons`
- **status:** `planned`
- **priority:** `high`
- **depends_on:** `0041-shared-benchmark-calibration`,
  `0060-slack-snapshot-reporting`, `0064-task-submission-kernel`,
  `0066-task-aware-reporting`
- **relates_to:** `0027-fine-grained-progress`
- **source:** v22 implementation audit during v34 planning (2026-08)

**Problem:** Two-variant request validation, atomic multi-spec persistence,
serial fleet execution, per-variant calibration, artifact carry-forward,
noise-aware comparison, and one-message reporting are implemented and tested.
They have not completed together on real hardware.

**Scope:** Submit a natural-language Slack comparison for one small workload
and two explicit refs, first with one measured run and then with two clean
repeats if the first canary is healthy. Prove same-worker serial placement,
one calibration per variant, per-variant calibration reuse, shared SQLite
carry-forward, bounded progress, comparison aggregation, artifact provenance,
and one final Slack snapshot.

**Acceptance:**

- One request creates one submission with two ordered specs and never executes
  variants or repeats concurrently.
- Each variant receives its own calibration identity; repeats reuse only their
  variant's identity and run in fresh VMs.
- Variant 1 receives the carried submission database rather than a fresh file,
  while artifacts and metrics remain attributable to their spec/run.
- The terminal task-aware report and Slack snapshot identify both refs,
  calibration provenance, completed/missing runs, metric deltas, and the
  conservative noise-aware verdict.
- Redelivery does not duplicate the submission or Slack message, and daemon
  restart reconstructs the same aggregate report.

**Deferred / non-goals:** No deterministic Slack flag grammar, variant matrix,
parallel variants, automatic ref discovery, or cross-worker comparison-group
recovery. Slack benchmark intake remains natural-language/LLM-only; typed
HTTP/CLI and exact GitHub triggers remain provider-free.

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
same GitHub/Slack input and require no duplicate submission, check/comment, or
Slack message.

For the GitHub validation canary, keep the worker drained until the queued
submission has created and persisted its configured `stacks-block-validation`
Check Run and marked PR comment. Then undrain and require the same identities
to receive phase and terminal updates. Missing pre-assignment identities or
late duplicate surfaces fail the gate.

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

1. Run default recent validation and confirm it saturates against the observed
   Nakamoto count when necessary.
2. Run full-history validation and require both epoch segments and a negative
   verdict only after every shard exits normally.
3. Run another explicit cross-epoch range and verify exact shard-plan/reducer
   agreement.
4. Cancel a running validation with the admin fleet command; require prompt VM
   stop, complete teardown, a fenced cancelled terminal, and converged report.
5. Restart the daemon during an active attempt and again after terminal event
   acceptance; require session/lease recovery and replay convergence.
6. Kill a worker during execution and during cleanup; require expiry/fencing,
   visible cleanup obligation, and no unsafe automatic cross-worker comparison
   continuation.

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
- [ ] Sandbox policy service starts and its live post-start verifier passes.
- [ ] Disposable-guest network and two-snapshot LVM ceremonies pass.
- [ ] Combined-worker preflight and database enrollment pass.
- [ ] Controlled one-block and cross-epoch sandbox/probe canaries pass with
  exact argv and processed-count audit evidence.
- [ ] GitHub benchmark and validation canaries pass.
- [ ] Slack benchmark and recent-validation canaries pass.
- [ ] Full-history, cancellation, daemon restart, worker loss, and cleanup
  recovery canaries pass.
- [ ] Identity rotation/revocation and restart persistence pass.
- [ ] First deployed protobuf revision has pinned semantic-digest vectors.

### Carried Feature Closure (`0027` / `0039`)

- [ ] Real `stacks-bench` progress reaches the current snapshot/fleet path and
  its raw JSONL is retained.
- [ ] Calibration/repeat attribution, optional fallback, debounce, and terminal
  precedence pass on the host.
- [ ] A two-variant comparison proves serial execution, per-variant
  calibration, carried DB, repeated-run aggregation, artifacts, and one report.

### Multi-Machine Readiness

- [ ] Second-machine commissioning passes, or is explicitly deferred without
  claiming multi-machine readiness.

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

## Completion

The first fleet may be declared live and `0080` archived once Phases 0–5, 7,
and 8 have durable evidence and the daemon plus at least one worker run as
separately managed processes. Phase 6 closes `0027` and `0039` independently;
it does not block that declaration. v34 completes once all three selected items
are shipped or any unfinished carried item is explicitly rescheduled. If the
second physical machine is not yet available, the record must say so and
multi-machine readiness remains unclaimed.
