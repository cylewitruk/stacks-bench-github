# v33: Dynamic Block-Validation Planning

Successor to
[v32](../archive/completed/0069-task-aware-intent-resolution.md). Replace
static validation ranges and daemon-selected shard counts with
chainstate-relative selection and worker-local execution planning.

> **Status:** in_progress — implementation and local validation are complete;
> first-deployment canaries remain open.
>
> v33 changes how block-validation demand is described and resolved. It does
> not change pull scheduling, worker authorization, lifecycle controls, or
> chainstate distribution. This is also the first-deployment target: v32 was
> not deployed, and its real Slack canaries transfer here.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0079-dynamic-block-validation-planning` | primary: chainstate-relative selection and local shard planning | in_progress |
| `0067-github-block-validation-submission` | co-primary: task-specific GitHub validation authorization and default trigger | in_progress |

## 0079 — Dynamic Block-Validation Planning

- **id:** `0079-dynamic-block-validation-planning`
- **status:** `in_progress`
- **priority:** `high`
- **depends_on:** `0019-block-validation-recipe`,
  `0063-libvirt-block-validation`, `0064-task-submission-kernel`,
  `0066-task-aware-reporting`, `0078-slack-task-submission`
- **relates_to:** `0015-resource-aware-admission`,
  `0052-managed-stacks-node-chainstate-producer`,
  `0068-watched-ref-task-actions`,
  `0075-rolling-worker-protocol-compatibility`
- **source:** post-v32 validation-selection and worker-policy review (2026-07)

**Problem:** Block-validation submission currently freezes a configured epoch,
absolute range, requested shard count, and concurrency before a worker is
selected. The daemon cannot know the tip of a worker-local nightly chainstate,
so the configured default range becomes stale. Epoch is unnecessary for the
normal Nakamoto path, while shard and concurrency limits duplicate policy that
already belongs to the worker operator.

**Scope:** Freeze a semantic validation selector at submission, resolve it
against the exact chainstate attached to the accepted attempt, and derive the
execution plan from the resolved block count plus the worker's local resource
profile. Make the normal default the latest one million Nakamoto blocks,
support full-history and explicit-range requests, remove the default epoch and
daemon shard/concurrency fields, and report both requested selection and
resolved execution coverage.

**Acceptance:**

- Default validation resolves to at most the latest one million available
  Nakamoto blocks on the attached chainstate.
- Full validation covers all available pre-Nakamoto and Nakamoto entries.
- Explicit inclusive ranges are validated exactly once and may cross the epoch
  boundary.
- The worker derives shard count and concurrency without accepting those
  values from Slack, GitHub, an LLM, or the daemon task policy.
- Accepted results bind the chainstate origin, observed coverage, resolved
  range, epoch segments, shard count, and concurrency.
- Static default range, default epoch, requested-shard, and daemon concurrency
  configuration cannot return unnoticed.

**Deferred / non-goals:** No chainstate replication or common-generation
coordinator, global tip service, cross-worker shard scheduling, automatic
throughput tuning, rolling protocol compatibility, or independently scheduled
epoch segments.

## 0067 — GitHub Block-Validation Submission

- **id:** `0067-github-block-validation-submission`
- **status:** `in_progress`
- **priority:** `high`
- **depends_on:** `0064-task-submission-kernel`,
  `0066-task-aware-reporting`
- **relates_to:** `0036-pr-comment-llm-intent`,
  `0071-github-job-lifecycle-controls`
- **source:** block-validation user-story audit and v33 authorization review
  (2026-07)

**Problem:** GitHub currently exposes an argument-style
`/validate-blocks <epoch> <start> <end>` command and routes it through the
benchmark-only `TriggerPrBenchmark` role. It has no simple default `/validate`
trigger and no validation-specific authorization.

**Scope:** Replace the argument grammar with an exact `/validate` comment that
freezes the current full PR-head SHA and submits the configured recent
selector through the shared application service and task-submission kernel.
Add a repository/install-scoped `TriggerBlockValidation` role to the existing
database-backed GitHub user-role model and its admin API/CLI. Keep `Admin`
implication within the same scope. Retire `/validate-blocks`; there is no
deployed compatibility surface to preserve.

**Acceptance:**

- An active `TriggerBlockValidation` or scoped `Admin` grant authorizes
  `/validate`; `TriggerPrBenchmark` alone does not.
- `/benchmark` continues to require `TriggerPrBenchmark`; a validation-only
  grant does not authorize it.
- Role lookup failure, revoked/wrong-install/wrong-repository grants, target or
  source policy denial, and stale PR identity fail closed before submission.
- Authorized redelivery freezes one immutable PR-head commit and returns the
  canonical task-aware submission/report identity.
- The GitHub adapter performs no scheduling, worker selection, or direct task
  persistence.

**Deferred / non-goals:** No natural-language PR intent (`0036`), full/range
arguments in comments, watched-ref automation (`0068`), lifecycle commands
(`0071`), or static-config GitHub user allowlist.

## Outcome

```text
Slack / GitHub / admin request
              |
              v
 immutable validation selector
 recent(1,000,000) | full | range(start, end)
              |
              v
 task-submission kernel + pull scheduler
              |
              v
 worker chooses local RO chainstate origin
              |
              v
 trusted guest runner probes epoch index counts
              |
              v
 concrete range + epoch segments + block count
              |
              v
 worker profile derives shards + concurrency
              |
              v
 one VM / K snapshots / K shard processes
              |
              v
 typed result binds requested and resolved plans
```

The selector is durable demand. The concrete plan is attempt-scoped because
only the worker can observe its local chainstate. Fencing still decides which
attempt may terminalize the job.

## Design Rules

- **Separate requested selection from resolved execution.** Submission and
  idempotency hash the selector. They must not claim that a moving worker-local
  range was known at enqueue time.
- **Resolve only inside the sandbox.** Repository-built `stacks-inspect` and
  chainstate inspection remain inside the disposable VM. There is no host
  execution fallback and no daemon access to worker storage.
- **Use the numeric index probes.** As documented by
  [`stacks-inspect`](https://github.com/stacks-network/stacks-core/tree/main/contrib/stacks-inspect),
  `get-nakamoto-tip` returns an index-block hash, not a numeric height. The
  runner uses the existing argument-free `validate-block ... index-range` and
  `... naka-index-range` probes and rejects malformed output.
- **Remove epoch from normal intake.** The public selector is `recent`, `full`,
  or `range`. Epoch segmentation is an execution detail. Recent covers
  Nakamoto only; full covers both epochs; an explicit range is split when it
  crosses the observed epoch boundary.
- **Do not parse flags from conversational text.** Authorized Slack text goes
  through the task-aware LLM intent resolver. GitHub comments may expose an
  exact `/validate` default trigger, but not an argument grammar. Structured
  HTTP/CLI DTOs remain machine interfaces and do not justify a second
  conversational parser.
- **Keep one canonical coordinate system.** Preserve the current contiguous
  validation-index space: pre-Nakamoto entries occupy
  `[0, pre_count)`, followed by Nakamoto entries in
  `[pre_count, pre_count + naka_count)`. Document it as a validation index,
  not an on-chain height.
- **Make worker resource policy authoritative.** The worker's
  `[block_validation]` profile owns target blocks per shard, maximum shards,
  and maximum concurrency. Fleet demand carries none of those values.
- **Keep shards and concurrency distinct.** Shards are the total partition
  count. Concurrency is the number of shard processes allowed to run at once
  and therefore must be no greater than the actual shard count.
- **Retain one assignment and one VM.** Shards remain local implementation
  units backed by distinct writable snapshots. They are not fleet jobs.
- **Fail closed before validation.** Reversed/out-of-coverage ranges, empty
  probes, arithmetic overflow, impossible segment plans, insufficient devices,
  or a resource plan outside the local profile fail the attempt without
  producing a validation verdict.
- **Bind results to the attempt.** The trusted reducer verifies that returned
  segments exactly and gaplessly cover the resolved range and that the
  reported shard plan matches the local planning algorithm.
- **Be honest about relative retries.** A retry of `recent` or `full` may
  resolve against a newer local origin. Reporting must expose that origin and
  coverage. Operators who need an exact rerun submit the previously resolved
  explicit range.
- **Do not add a tip coordinator yet.** Workers are assumed to receive
  sufficiently current nightly chainstates. Exact generation coordination
  remains with `0052`.

## Stable Selection Contract

The task-owned contract becomes:

```rust
enum BlockValidationSelection {
    Recent { block_count: NonZeroU64 },
    Full,
    Range(InclusiveRange),
}

struct BlockValidationPayload {
    selection: BlockValidationSelection,
    timeout_secs: NonZeroU64,
}
```

Selection semantics:

- `Recent { block_count }` resolves to the last
  `min(block_count, observed_nakamoto_count)` Nakamoto entries.
- `Full` resolves to every observed pre-Nakamoto and Nakamoto entry.
- `Range` is an inclusive range in the contiguous validation-index space. It
  must be fully covered by the attached chainstate.

The resolved plan is conceptually:

```rust
struct ResolvedBlockValidationPlan {
    selection: BlockValidationSelection,
    chainstate_origin: String,
    observed: ObservedValidationIndex,
    range: InclusiveRange,
    segments: Vec<EpochSegment>,
    shard_count: NonZeroU32,
    max_concurrency: NonZeroU32,
}
```

`segments` contains one entry for a single-epoch range and two entries when
full or explicit coverage crosses the epoch boundary. Commands receive
epoch-local indices; progress, persistence, and reporting retain the global
validation indices. The global range is partitioned into at most K shards
first. If one shard crosses the epoch boundary, that shard executes two
epoch-local commands sequentially on its one snapshot; crossing the boundary
does not allocate an extra shard or device.

## Worker-Local Resource Plan

Extend the existing worker-owned profile:

```toml
[block_validation]
vcpus = 48
memory_bytes = 206158430208
target_blocks_per_shard = 25000
max_shards = 48
max_concurrency = 24
```

The example values are operational starting points, not protocol defaults.
The planner computes:

```text
desired_shards = ceil(resolved_block_count / target_blocks_per_shard)
shard_count = clamp(desired_shards, 1, max_shards)
concurrency = min(shard_count, max_concurrency)
```

For `recent` and explicit ranges, the worker knows an upper-bound block count
before boot and provisions that many snapshot devices. `full` provisions
`max_shards`; the guest may use a prefix of those devices after probing.
Unused snapshots remain attempt-scoped and are removed by the existing
cleanup path.

The fleet offer needs only the block-validation capability. A capable worker
uses its own profile to decide how quickly to execute the assignment rather
than declining demand because another machine chose a different shard count.

## Daemon Policy and Intake

The daemon policy becomes:

```toml
[tasks.block_validation]
default_recent_blocks = 1000000
max_recent_blocks = 1000000
timeout_secs = 86400
allow_full_validation = true
allow_range_override = true
```

Remove:

- `default_epoch`;
- `default_range_start` / `default_range_end`;
- `requested_shards`; and
- daemon-owned `max_concurrency`.

Slack and future GitHub natural-language intake can classify only:

- recent validation, optionally with a requested block count;
- full validation; or
- an explicit inclusive range.

Natural language such as:

```text
Validate the latest 500k blocks on abc123.
Run full block validation on abc123.
Validate blocks 200000 through 300000 on abc123.
```

converts to the closed selector through the LLM intent contract. Omitting a
recent count uses `default_recent_blocks`. A supplied count must be non-zero
and no greater than `max_recent_blocks`; invalid or over-policy values fail
rather than clamp. The default must also be no greater than the configured
maximum.

The model may extract the requested selector and recent block count because
they define semantic task scope. It cannot set the configured default/maximum,
timeout, shard size, shard ceiling, concurrency, worker, or chainstate origin.
The daemon validates every provider-produced value before submission.

The low-level admin API uses the same selector DTO. It no longer accepts epoch,
shards, concurrency, or timeout from the caller. Retire the argument-style
GitHub `/validate-blocks` parser. An exact `/validate` comment may submit the
configured default; scoped GitHub natural language remains with `0036`.

Remove the v32 deterministic Slack flag parser rather than extending it.
Conversational benchmark and validation requests share the provider-backed
task-aware resolver. Provider-disabled operation continues through typed
admin/API producers and exact policy-owned triggers, not ad hoc text parsing.

## Human Authorization

Slack retains v32's task-specific allowlists for this iteration:

- workspace and membership in the union of benchmark/validation grants are
  checked before an LLM call;
- the resolved task is checked against its own user list before source
  resolution or submission; and
- the model cannot grant permission or weaken full/range/recent-count policy.

GitHub uses database-backed roles keyed by immutable numeric user ID,
installation, and optional target repository:

- `TriggerPrBenchmark` authorizes benchmark creation only;
- `TriggerBlockValidation` authorizes `/validate` only; and
- `Admin` implies both only within the admin grant's installation/repository
  scope.

Revoked, wrong-scope, missing, or lookup-failed grants deny. Logins remain
display fields and never authorize.

Static Slack user configuration is transitional. Moving human grants into the
database, managing them through the authenticated admin CLI, and optionally
adding a Slack DM/App Home administration surface is a separate follow-up.
That path must bootstrap administrators outside Slack, audit every grant and
revocation, and never treat access to the bot as self-enrollment. V33 does not
build that control plane.

## Reporting and Persistence

The durable submission stores the selector in the existing task payload JSON.
The report read model exposes:

- requested selection;
- chainstate origin;
- observed pre-Nakamoto and Nakamoto coverage;
- concrete resolved range;
- epoch-local execution segments;
- actual shard count and concurrency;
- checked blocks and invalid-block details.

Queued surfaces describe `latest 1,000,000`, `full history`, or the explicit
range. Running and terminal surfaces switch to concrete coverage once the
worker reports it. They must never render a relative selector as though it
were a submission-time absolute range.

The block-validation payload needs no relational schema change. As of
2026-07-31 the fleet has not been deployed and no production block-validation
payload exists, so v33 changes the initial payload shape without a legacy
decoder or data migration. Reconfirm that fact immediately before
implementation; if it has changed, stop and design the forward compatibility
path rather than making historical reports undecodable.

`0067` adds one forward, add-value-only database migration for
`user_role.trigger_block_validation` plus migration/role-store tests. The
migration must not seed or otherwise use the new enum value in the transaction
that adds it; grants are created only after that migration commits through the
admin API/CLI. Rollback revokes/removes grants of that role before deploying an
older binary; the additive PostgreSQL enum label may remain.

## Protocol and Rollout Boundary

This is an incompatible fleet payload change: epoch/range/shard fields are
replaced by a selector, and block-validation offer requirements lose resource
fields. The selected branch is the undeployed one: update the initial protobuf
schema in place without compatibility scaffolding. If any worker is deployed
before implementation starts, stop and reassess; an independently operated
worker requires `0075` before v33.

v33 does not bump a deployed protocol or implement rolling compatibility.

## Phases

### Phase 1: Freeze Selection and Index Semantics

**Goal:** Establish one precise public coordinate system and selector contract
before changing transport or execution.

**Scope:**

- Capture real `stacks-inspect` probe fixtures for both epoch index counts.
- Record the existing global-to-epoch-local translation.
- Add pure selection-resolution and segment-partition tests.
- Decide the deployment/protocol branch from the actual fleet state.

**Acceptance & Validation:**

- [x] `get-nakamoto-tip` is not parsed as a numeric height.
- [x] Empty, malformed, overflowing, and inconsistent probes fail closed.
- [x] Recent never includes pre-Nakamoto entries.
- [x] Full covers both observed epochs.
- [x] Explicit ranges at, below, above, and across the epoch boundary have
  unambiguous results.

### Phase 2: Replace the Task and Wire Contracts

**Goal:** Carry immutable selection rather than a guessed concrete plan.

**Scope:**

- Add the exhaustive selector to `sbgh-fleet` and `sbgh-driver`.
- Update protobuf messages and exhaustive edge conversions.
- Remove epoch/shards/concurrency from offer and assignment payloads.
- Keep semantic digests deterministic over the selector and timeout.

**Acceptance & Validation:**

- [x] Proto3 unspecified/missing selector variants fail closed.
- [x] Core, reporting, PostgreSQL, and driver crates do not import generated
  protobuf types.
- [x] Task digests change when selection changes and ignore no semantic field.
- [x] No worker accepts caller-provided shard or concurrency values.

### Phase 3: Consolidate Daemon Policy and Intake

**Goal:** Remove moving chainstate facts and worker capacity from daemon
configuration.

**Scope:**

- Land `0067` first within this phase as a self-contained, independently
  committable and reviewable authorization slice: enum migration, role store,
  admin API/CLI, and exact `/validate` gate.
- Replace the v32 static plan with recent/full/range policy.
- Remove conversational flag parsing and update the provider intent plus
  shared submission service.
- Adapt GitHub and admin producers to the same selector.
- Reject retired configuration and request fields.

**Acceptance & Validation:**

- [x] Default Slack validation stores `Recent { 1_000_000 }`.
- [x] “Latest 500k” stores `Recent { 500_000 }`; zero, over-policy, and
  cross-selector count fields fail closed.
- [x] Full and range permissions are enforced before submission.
- [x] The LLM can extract semantic selection/count but cannot set policy
  bounds, timeout, shards, concurrency, or worker.
- [x] Slack/GitHub conversational text has no CLI-style flag parser.
- [x] Benchmark-only and validation-only GitHub grants cannot authorize the
  other task; scoped `Admin` implies both.
- [x] Revoked and wrong-scope GitHub grants fail closed before submission.
- [x] The role migration only adds the enum label; a separate committed
  application action creates the first grant, and rollback tests remove all
  such grants before an older binary reads `user_role`.
- [x] Checked-in daemon configuration loads and retired fields fail loudly.

### Phase 4: Implement Worker-Local Planning

**Goal:** Resolve and execute the selector against the attached origin.

**Scope:**

- Add `target_blocks_per_shard` to the worker profile.
- Generalize the guest plan from one epoch/range to a selector and device
  capacity.
- Probe counts, resolve the range, split epoch segments, derive resources, and
  partition exactly.
- Reuse existing snapshot, timeout, cancellation, and cleanup machinery.

**Acceptance & Validation:**

- [x] One block uses one shard and one process.
- [x] Remainders are balanced without gaps or overlaps.
- [x] `max_concurrency < shard_count` runs deterministic waves; concurrency
  never exceeds shard count.
- [x] Full validation can use all local shards without requiring a daemon tip.
- [x] Cancellation, lease loss, setup failure, and malformed results remove
  every domain, mount, and snapshot.

### Phase 5: Converge Results and Reporting

**Goal:** Make relative demand and concrete execution equally visible.

**Scope:**

- Extend typed block-validation output with the resolved plan.
- Harden the reducer against partial or contradictory plans.
- Update the v28 read model, API DTO, GitHub check, and Slack snapshot.
- Keep invalid-block output bounded and escaped.

**Acceptance & Validation:**

- [x] API, GitHub, and Slack consume one typed report projection.
- [x] Terminal output distinguishes requested selector, observed coverage, and
  resolved range.
- [x] Replayed older progress cannot regress a terminal snapshot.
- [x] A retry on a newer origin is visible rather than silently presented as
  the same concrete execution.

### Phase 6: Ratchets, Documentation, and Real-Host Proof

**Goal:** Remove the static model completely and validate the dynamic plan on
the real worker.

**Scope:**

- Update setup, architecture, API, Slack, and worker examples.
- Add source/config/proto ratchets for retired epoch/range/resource fields.
- Run the transferred natural-language Slack benchmark canary plus recent,
  full, explicit, cross-epoch, cancellation, and restart validation canaries.

**Acceptance & Validation:**

- [x] No current code or reference docs describe a configured absolute default
  range or daemon-selected shard/concurrency count.
- [ ] A real natural-language Slack benchmark completes through v32's retained
  intent, authorization, submission, and reporting path.
- [ ] Real-host recent validation resolves to the expected chainstate tail.
- [ ] A real natural-language Slack validation selects recent coverage and
  reaches its task-aware report.
- [ ] Real-host full validation enters both epoch segments.
- [ ] Explicit range coverage and shard accounting match the requested indices.
- [ ] Daemon restart/replay preserves the accepted resolved-plan report.

## Test Matrix

| Area | Required coverage |
| ---- | ----------------- |
| Selection | default/overridden recent count, saturation, full, explicit range, invalid/overflow/policy bounds |
| Epoch mapping | pre-only, Nakamoto-only, exact boundary, crossing boundary |
| Resource plan | one block, exact division, remainder, shard cap, concurrency waves |
| Protocol | missing/unknown selector, exhaustive round trip, retired-field rejection |
| Submission | selector digest, idempotent retry, conflicting selection |
| Intent | natural-language recent/full/range, count default/override, ambiguous and flag-like input rejection |
| Authorization | Slack pre-provider union/post-resolution task check; GitHub benchmark-only, validation-only, scoped admin, revoked/wrong-scope/lookup failure |
| Guest | real probe fixtures, malformed probe, insufficient coverage/devices |
| Reduction | gap, overlap, duplicate shard, wrong segment, partial terminal |
| Reporting | queued relative view, running concrete view, terminal/replay convergence |
| Recovery | cancellation, lease loss, newer-origin retry visibility, cleanup |

## Final Validation

- [x] `just build --no-sccache`
- [x] `just lint --no-sccache`
- [x] `just test --summary --no-sccache`
- [x] `git diff --check`
- [x] Protobuf/domain boundary checks pass.
- [x] The configured daemon example and benchmark/block-validation worker
  examples remain parse-tested.
- [x] PostgreSQL migration and task-specific GitHub role/API/CLI tests pass.
- [x] The `0067` authorization slice receives a focused review of enum
  migration, scoped role lookup, `Admin` implication, and the `/validate`
  fail-closed gate before the remaining Phase 3 intake changes land.
- [x] Existing benchmark planning, submission, execution, and reporting are
  unchanged. Slack benchmark intake intentionally becomes LLM-only; typed
  API/CLI producers and exact GitHub triggers remain provider-free.
- [ ] One natural-language Slack benchmark and one natural-language Slack
  recent-validation request complete without duplicate submissions/messages.
- [ ] Recent, full, and explicit validation each complete on a real worker.
- [ ] Cancellation and daemon restart preserve fencing, cleanup, and reporting
  convergence.

## Rollout

1. Reconfirm that no daemon/worker fleet or persisted production validation
   payload has been deployed; stop if that precondition changed.
2. Drain block-validation attempts and deploy the coordinated daemon/worker
   binaries.
3. Replace static daemon validation defaults and add the worker's target shard
   size.
4. Run the transferred natural-language Slack benchmark canary.
5. Run a small explicit-range canary, then a natural-language Slack recent
   canary; redeliver both Slack envelopes and verify exactly-once behavior.
6. Run and cancel a full validation after plan resolution; verify cleanup and
   concrete reporting.
7. Run the full-history canary through both epoch segments before enabling
   natural-language full validation.
8. After the first successful fleet deployment, pin the deployed protocol
   revision's canonical semantic-digest vectors before making later wire or
   digest changes.

Rollback drains workers and restores the prior coordinated daemon/worker
binaries and configuration. If v33 changes persisted production payloads,
rollback follows the explicit migration/legacy-decoder plan selected in Phase
1. Before restoring a pre-v33 daemon, hard-delete every
`github_user_role.granted_role = 'trigger_block_validation'` row after taking
a database backup. Soft revocation is insufficient because older binaries
still deserialize revoked rows when listing grants.

## Deferred / Follow-Ups

- `0052` owns common chainstate-generation production and distribution.
- `0015` may later advertise richer capacity for scheduler-side admission.
- `0075` owns independently coordinated worker upgrades and compatibility
  windows.
- `0068` and `0070` add producer/lifecycle surfaces over the selector after
  their task-neutral application services exist.
- Database-backed Slack human grants plus CLI and DM/App Home administration
  remain a separate follow-up; v33 deliberately retains static Slack user
  allowlists.
