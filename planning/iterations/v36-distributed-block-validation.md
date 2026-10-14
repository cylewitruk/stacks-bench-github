# v36: Distributed Block Validation

Successor to [v35](v35-seamless-fleet-lifecycle.md). Move block-range planning
and placement from one worker into the daemon so a validation can use several
heterogeneous workers with different, canonically verified chainstate tips.
Preserve one user-visible submission, exact range coverage, fenced retries,
and convergent GitHub/Slack reporting.

> **Status:** planned — architecture and acceptance are defined for review;
> implementation starts after v35 establishes resident workers and negotiated
> protocol evolution.
>
> v36 factors the canonical block identity and snapshot-compatibility layer out
> of parked v23. v23 (`0026`) remains responsible for transaction facts,
> workload import packs, and benchmark pre-seeding.
>
> **Entry condition:** at least two v35-compatible, operator-trusted workers can
> be enrolled and one qualified canonical snapshot can be provisioned manually
> onto each. v36 owns that repeatable bootstrap/verification runbook; automated
> snapshot production and distribution remain later work.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0084-canonical-chainstate-worker-inventory` | foundation: canonical coverage and worker snapshot inventory | planned |
| `0015-resource-aware-admission` | co-primary: validation slots and benchmark exclusion | planned |
| `0085-distributed-block-validation-orchestration` | primary: durable planning, placement, and reduction | planned |

## Delivery Strategy

Phases 1–4 form the first deployable slice: a fresh canonical ledger, explicit
provider-visible freshness, worker inventories, and daemon-owned planning with
single-worker parity. Phase 5 adds the first multiworker capability; Phase 6
optimizes placement; Phase 7 qualifies failures and reorg behavior. Community
result authority is deliberately split into [v37](v37-community-worker-validation-trust.md)
so it does not block trusted-fleet distribution.

## 0084 — Canonical Chainstate and Worker Inventory

- **id:** `0084-canonical-chainstate-worker-inventory`
- **status:** `planned`
- **priority:** `high`
- **depends_on:** `0075-rolling-worker-protocol-compatibility`,
  `0082-seamless-fleet-lifecycle-maintenance`
- **relates_to:** `0026-central-block-index-cache`,
  `0052-managed-stacks-node-chainstate-producer`
- **unblocks:** `0085-distributed-block-validation-orchestration`
- **source:** [Decision 0007](../decisions/0007-daemon-owned-distributed-validation-plans.md)

**Problem:** The daemon cannot determine whether a worker's local snapshot is a
canonical chain prefix or which global validation ordinals it covers. A height,
snapshot name, or worker-local probe count alone is not a fleet-wide
compatibility contract.

**Scope:** Add a generation-aware canonical block ledger to PostgreSQL and a
negotiated worker inventory for immutable local snapshots. Populate the ledger
with an operator-run read-only indexer against the already qualified mainnet
origin; ongoing refresh initially consumes another explicitly qualified
snapshot, while `0052` may later automate production. Record canonical
height/hash/parent identity, global and epoch-local validation ordinals,
finality, provenance, conflicts, and ingestion freshness. Match each reported
snapshot tip and coverage to one canonical generation; expose eligible coverage
to the planner using opaque snapshot IDs rather than host paths. Provide a
checksum/provenance-verified manual bootstrap for a second trusted worker.

**Acceptance:** Canonical inventories become eligible for exactly their proven
coverage; an older canonical tip remains usable below its tip; non-canonical,
conflicting, expired, unverifiable, or too-stale ledger/inventory state is
rejected with an actionable reason; reorg generations never rewrite evidence
used by an active plan; workers cannot make their own block claims canonical;
and two trusted workers can be provisioned and matched to the ledger through a
repeatable operator procedure.

**Deferred / non-goals:** No transaction ledger, benchmark pre-seed pack,
automated snapshot refresh/transfer service, or managed chainstate production.
Those remain `0026`, a future distribution item, and `0052` respectively.

## 0015 — Resource-Aware Validation Admission

- **id:** `0015-resource-aware-admission`
- **status:** `planned`
- **priority:** `high`
- **depends_on:** `0075-rolling-worker-protocol-compatibility`,
  `0082-seamless-fleet-lifecycle-maintenance`
- **relates_to:** `0038-isolated-benchmark-repetitions`,
  `0039-multi-variant-benchmark-comparisons`,
  `0052-managed-stacks-node-chainstate-producer`,
  `0085-distributed-block-validation-orchestration`
- **unblocks:** `0087-general-resource-lane-topology`
- **source:** `archive/completed/0008` and v34/v36 host qualification

**Problem:** Flat job counts cannot say how many block-validation VMs a worker
permits or prevent offer races from exceeding that limit. They also do not
encode the commissioned-fleet invariant that benchmark and validation work are
mutually exclusive.

**Scope:** Add an operator-authorized `max_parallel_block_validations` and an
explicit benchmark/validation exclusion policy to the worker profile and
negotiated registration facts. The daemon atomically reserves validation slots
with allocations and releases them only through fenced terminal/cleanup
transitions. v36 keeps benchmark and validation mutually exclusive.

**Acceptance:** Concurrent offers cannot exceed a worker's validation-slot
count, including under scheduler races; operator status explains eligibility
and reservations; benchmark and validation allocations cannot overlap; and
restart/reconciliation cannot leak or revive a stale slot reservation.

**Deferred / non-goals:** No general CPU/memory/storage/I/O lane graph,
automatic hardware topology as policy, or benchmark/validation co-scheduling.
Those are tracked by `0087` after the minimal v36 slot contract is proven.

## 0085 — Distributed Block-Validation Orchestration

- **id:** `0085-distributed-block-validation-orchestration`
- **status:** `planned`
- **priority:** `high`
- **depends_on:** `0015-resource-aware-admission`,
  `0075-rolling-worker-protocol-compatibility`,
  `0082-seamless-fleet-lifecycle-maintenance`,
  `0084-canonical-chainstate-worker-inventory`
- **relates_to:** `0027-fine-grained-progress`,
  `0066-task-aware-reporting`,
  `0079-dynamic-block-validation-planning`,
  `0086-community-worker-validation-trust`
- **source:** [Decision 0007](../decisions/0007-daemon-owned-distributed-validation-plans.md)

**Problem:** A block-validation submission is currently offered as one attempt
to one worker, which derives the concrete range and shard layout locally. The
daemon cannot distribute exact coverage, account for heterogeneous startup and
throughput, or retry one incomplete range independently.

**Scope:** Persist an immutable canonical execution plan, durable plan shards,
and fenced worker allocations. The daemon freezes the canonical target,
partitions and places eligible ranges using snapshot coverage and reserved
validation slots, and reduces shard progress/results into one submission. Each
allocation contains one durable plan shard plus daemon-defined execution slices
for concurrent `stacks-inspect` processes inside one VM. Workers verify and
execute both levels unchanged or reject the assignment. Bundling several
durable shards into one allocation remains a later optimization.

**Acceptance:** One request executes across at least two workers with different
canonical tips; every worker receives only eligible ranges; the union is exact
and non-overlapping; weighted progress and the final verdict converge on one
provider surface; worker loss retries only incomplete shards; cancellation
fans out and cleans every allocation; and stale fenced results cannot alter the
parent result.

**Deferred / non-goals:** No general distributed DAG engine, arbitrary
cross-task sharding, speculative execution by default, community-result
authority (`0086`/v37), or dependence on multi-orchestrator HA (`0083`).

## Authority and Scheduling Model

| Concern | Authority | Durable identity |
| ------- | --------- | ---------------- |
| Canonical block and ordinal mapping | trusted ledger ingestion | network + canonical generation + block identity |
| Local snapshot existence and availability | worker observation | worker + opaque snapshot ID + inventory generation |
| Snapshot compatibility and eligible coverage | daemon ledger query | canonical generation + verified ordinal ceiling |
| Validation-slot limit and benchmark exclusion | operator-approved worker profile | worker + profile generation |
| Current validation-slot availability | worker observation + daemon reservation | session + slot reservation/fence |
| Requested semantic range | task submission | immutable selection |
| Plan shards, execution slices, and placement | daemon planner | execution plan + shard + allocation |
| Local execution safety | worker | accepted/rejected assignment |
| Parent progress and verdict | daemon reducer | accepted fenced shard results |

Capabilities answer what code a worker supports. Validation slots answer how
much work the host can safely run now. v36 schedules only operator-trusted
workers; community result authority is a separate v37 concern. These dimensions
are never collapsed into one flag.

## Design Rules

- **Freeze semantic intent before placement.** Resolve `latest`, `full`, and
  explicit ranges against one canonical generation and persist the inclusive
  global validation range before offering any shard.
- **Capacity cannot redefine the request.** `latest` resolves to the ledger's
  configured finalized target, not the highest tip currently advertised by an
  eligible worker. Missing upper coverage leaves the plan visibly waiting (or
  rejects it by policy); it never silently validates an older range.
- **Height is not a validation ordinal.** Store the mapping required for both
  Epoch 2 and Nakamoto `stacks-inspect` ranges; never derive it from height by
  arithmetic.
- **Canonical admission is server-owned.** Worker inventory is a claim checked
  against the ledger. Only trusted ingestion advances canonical state.
- **Ledger health is an admission gate.** A configured maximum generation age,
  continuous coverage, and conflict-free state are required for new central
  plans. Stale or broken ingestion is visible and fails closed; it never
  silently falls back or redefines `latest`.
- **Use finalized coverage first.** The initial authoritative planner selects
  finalized ledger entries. Provisional near-tip mode is a later explicit
  policy with generation-pinned supersession.
- **Give each range level one name and one planner.** A plan shard is the
  independent placement/retry/fence/result unit. Its daemon-defined execution
  slices are concurrent commands within one VM and have no independent lease
  or placement. Workers derive neither set of boundaries.
- **One plan shard per allocation first.** Multi-shard bundles are an
  optimization only after partial terminal persistence is specified and tested.
- **Reserve before offer.** Snapshot eligibility, negotiated features, and a
  validation slot are checked and reserved in the same transaction that
  creates an allocation.
- **Verify locally, never re-plan locally.** A worker rejects a missing or
  changed snapshot and invalid range; it does not substitute another snapshot
  or expand/shrink the range.
- **Retry missing coverage only.** Completed accepted shards remain immutable.
  Reassignment gets a new allocation/fencing identity and cannot duplicate an
  authoritative terminal.
- **Aggregate by blocks.** Parent progress is the sum of accepted per-shard
  processed counts over the frozen plan total, independent of worker count or
  shard count.
- **Treat estimates as hints.** Placement may learn startup and throughput per
  worker/profile, but stale estimates affect latency, not correctness.
- **Negotiate every new semantic.** Inventory, validation slots, explicit
  assignments, execution slices, and shard results roll out server-first under
  `0075`; previous workers only receive the monolithic revision-1 payload.

## Deliverables

- Accepted Decision 0007 and a checked-in planning/failure matrix.
- Canonical block ledger ingestion, inspection, conflict, finality, and
  generation tooling reusable by v23.
- Ledger freshness health, alerts, provider-visible generation/age, and an
  explicit fail-closed/rollback policy.
- Negotiated immutable-snapshot inventory and operator-visible compatibility
  reasons.
- Repeatable checksum/provenance-verified snapshot bootstrap onto a second
  trusted worker.
- Per-worker validation-slot limits, atomic reservations, and benchmark
  exclusion.
- Durable block-validation plans, shards, allocations, fenced results, and a
  block-weighted parent reducer.
- Startup/throughput-aware heterogeneous placement with conservative defaults.
- Multi-host canaries proving heterogeneous tips, failure recovery, progress,
  result reduction, provider convergence, and cleanup.

## Persistence and Migration

- Add canonical chain generations and canonical block rows keyed by network,
  generation, and block identity. Store height, parent, global validation
  ordinal, epoch/local ordinal, finality, source provenance, and conflict state.
  Canonical rows are append/supersede, never in-place reorg rewrites.
- Add immutable worker snapshot inventories keyed by worker and opaque snapshot
  ID, with inventory generation, tip identity, observed coverage, provenance,
  health, freshness, and expiry. Persist the daemon's compatibility decision
  and canonical generation separately from the worker claim.
- Add operator-approved maximum validation slots and benchmark exclusion to the
  worker profile. Add durable, fenced slot reservations tied to allocations.
- Add one execution-plan row per block-validation job, plan-shard rows with
  exact inclusive ranges and execution slices, allocation rows, and accepted
  per-shard results.
  Preserve the existing task selection as the user request; the plan is a
  separate derived, immutable record.
- Start with one plan shard per allocation. Any future bundle uses a mapping
  table so plan-shard identity and completion survive allocation changes.
- Use expand/contract migrations. New daemons dual-read old monolithic jobs and
  new plans. Previous workers receive only the old payload until negotiated
  feature support and the compatibility floor permit contract cleanup.

## Phases

### Phase 1: Authority Contract and Failure Matrix

**Goal:** Freeze canonical, scheduling, performance, and lifecycle invariants
before schema or protocol work.

**Scope:**

- Review and accept Decision 0007.
- Specify canonical generation/finality/reorg semantics and the exact mapping
  between height, global validation ordinal, and epoch-local ordinal.
- Specify inventory/ledger freshness, slot reservations, plan-shard and
  execution-slice semantics, fencing, cancellation, retry, aggregation, and
  provider convergence.
- Pin operator-trusted-only admission and previous-worker compatibility.
- Extract comparable timing baselines from the v34 1,000-block single-shard and
  10,000-block five-shard/four-concurrent canaries, including VM/tool
  initialization, first-block latency (about 883 seconds in the 1,000-block
  canary), steady-state blocks/second, and total wall time.

**Status:**

- [ ] Core contract and failure matrix complete
- [ ] Unit/integration test matrix complete
- [ ] Reviewed (Codex)
- [ ] Validated against v34 multi-shard evidence

**Acceptance & Validation:**

- [ ] Every mutable fact has one authority and every durable transition has a
  stable identity, idempotency key, and terminal state.
- [ ] Reorg, worker loss, duplicate offer/result, cleanup debt, cancellation,
  and mixed-version behavior are specified without shell-order assumptions.
- [ ] A worked example assigns one cross-epoch request across workers with
  different tips and proves exact inclusive coverage.
- [ ] The v34 baseline record names the exact jobs/attempts, configuration,
  timestamps, startup cost, steady-state throughput, and total duration used by
  Phase 6.

### Phase 2: Canonical Block Ledger

**Goal:** Make canonical validation coverage queryable in PostgreSQL.

**Scope:**

- Implement ledger schema, generations, finality, conflicts, quarantine,
  freshness health, and operator inspection.
- Implement an operator-run read-only indexer/exporter against the qualified
  mainnet origin and import its versioned, checksummed manifest into Postgres.
  Worker-reported inventory is never an ingestion source.
- Backfill that origin and prove its global/epoch-local ordinal mapping against
  `stacks-inspect` probes. Until `0052`, refreshes repeat this process against a
  newly qualified operator snapshot.
- Expose a narrow typed query seam reusable by v23 without importing v23's
  transaction/index-pack scope.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests
- [ ] Reviewed (Codex)
- [ ] Validated against a real canonical snapshot

**Acceptance & Validation:**

- [ ] Repeated ingestion is idempotent; conflicting identities fail closed and
  remain inspectable.
- [ ] Reorg ingestion creates a new generation and cannot mutate a pinned plan.
- [ ] Coverage queries return exact canonical ordinal ceilings and reject gaps.
- [ ] Epoch-boundary fixtures match v34 cross-epoch plan/result evidence.
- [ ] A configured maximum ledger age is enforced and alerted. Stale, gapped,
  or conflicting state blocks new central plans with an actionable status;
  operator rollback to the revision-1 monolithic path is explicit and audited.

### Phase 3: Snapshot Inventory and Validation Slots

**Goal:** Give the daemon trustworthy compatibility inputs and reservable host
capacity without exposing local storage paths.

**Scope:**

- Add negotiated `chainstate_inventory_v1` and `validation_slots_v1` features.
- Publish multiple immutable snapshots per worker with bounded inventory size,
  freshness, health, and opaque IDs.
- Publish the operator-authorized validation-slot ceiling and benchmark
  exclusion; reserve slots atomically with allocations.
- Add a repeatable checksum/provenance-verified procedure for placing the
  qualified snapshot on a second trusted worker and matching its inventory to
  the ledger.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests
- [ ] Reviewed (Codex)
- [ ] Validated on heterogeneous worker fixtures

**Acceptance & Validation:**

- [ ] Canonical, lagging-canonical, conflicting, stale, missing, and expired
  inventories produce deterministic eligibility reasons.
- [ ] Concurrent schedulers cannot overbook a worker's validation slots.
- [ ] Restart/re-registration reconciles inventory and reservations without
  reviving stale availability.
- [ ] Benchmark and validation remain mutually exclusive.
- [ ] Two trusted workers independently advertise canonical compatible
  inventories produced by the documented bootstrap.

### Phase 4: Central Planner with Single-Worker Parity

**Goal:** Move concrete range planning into the daemon without changing the
proven v34 single-worker result.

**Scope:**

- Persist the canonical target, durable plan-shard ranges, and exact
  intra-allocation execution slices in the daemon.
- Add negotiated `explicit_block_assignment_v1`; workers verify and execute the
  assigned shard and slices unchanged.
- Retain monolithic worker-local planning for previous workers during rollout.
- Compare old/new semantic plan and terminal result digests on pinned fixtures.
- Surface canonical generation, resolved range, finality, and ledger age in the
  job report and GitHub/Slack terminal details.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests
- [ ] Reviewed (Codex)
- [ ] Validated with a single-worker parity canary

**Acceptance & Validation:**

- [ ] `recent`, `full`, explicit, and cross-epoch selections produce exact,
  stable plans with no off-by-one drift.
- [ ] Worker availability cannot change a frozen semantic target; missing
  coverage is explicit rather than silently truncating or moving the range.
- [ ] A worker cannot silently change snapshot, generation, or range.
- [ ] The guest runs daemon-defined execution slices concurrently without
  deriving another partition; cross-epoch slice boundaries remain exact.
- [ ] Old and new planners agree on v34 fixtures and terminal evidence.
- [ ] Previous workers remain schedulable only for the semantics they support.
- [ ] Provider output makes a stale-but-within-policy canonical target visibly
  distinguishable from the network's current tip.

### Phase 5: Durable Multiworker Execution

**Goal:** Execute one logical validation across several workers and reduce it
into one authoritative submission.

**Scope:**

- Add plan-shard, allocation, per-shard lease/fence/result, execution-slice
  evidence, and parent reducer state.
- Allocate one plan shard per worker attempt; run its exact slices concurrently
  inside that worker's single VM.
- Aggregate progress by processed blocks and project one GitHub/Slack surface.
- Fan out cancellation and gate reassignment on cleanup acknowledgement.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests
- [ ] Reviewed (Codex)
- [ ] Validated with a two-worker canary

**Acceptance & Validation:**

- [ ] Two or more workers with different canonical tips receive disjoint ranges
  within their eligible coverage; union equals the frozen plan exactly.
- [ ] Plan-shard and execution-slice evidence use distinct identities and the
  reducer proves exact coverage at both levels without double counting.
- [ ] A lost worker causes only incomplete shards to be retried after cleanup.
- [ ] Duplicate/stale progress and terminals are idempotent or fenced.
- [ ] Parent completion, artifacts, comments, checks, and Slack snapshots
  converge exactly once.

### Phase 6: Startup- and Throughput-Aware Placement

**Goal:** Gain useful parallel speedup without multiplying the fixed
`stacks-inspect` startup cost.

**Scope:**

- Persist bounded per-worker/profile startup and block-throughput observations.
- Choose eligible worker count and weighted shard sizes from slots, coverage,
  startup cost, throughput, and queue state.
- Keep deterministic conservative fallbacks for cold or stale estimates.
- Specify a later bundle optimization without making it part of first rollout.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests
- [ ] Reviewed (Codex)
- [ ] Validated with heterogeneous worker timing fixtures

**Acceptance & Validation:**

- [ ] Small requests remain single-worker when parallel startup would cost more.
- [ ] Larger requests use available eligible slots and assign larger ranges to
  faster workers without exceeding canonical coverage.
- [ ] Bad/missing estimates can reduce performance but cannot alter correctness
  or starve all eligible work.
- [ ] Operator status explains each placement and its cost inputs.
- [ ] The v34 1,000/10,000-block baselines calibrate the model. At least one
  qualified large-range multiworker canary completes at least 20% faster than
  its measured fastest eligible single-worker plan; 10,000 blocks remains a
  calibration case and need not be distributed when startup dominates.

### Phase 7: Failure and Reorg Qualification

**Goal:** Prove distributed results from trusted workers remain correct under
real failure modes.

**Scope:**

- Exercise cancellation, worker loss, daemon restart, stale terminals, cleanup,
  artifact promotion retry, and canonical-generation changes.
- Run a multi-host canary with heterogeneous canonical tips on at least two
  operator-trusted workers.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests
- [ ] Reviewed (Codex)
- [ ] Validated on the production qualification fleet

**Acceptance & Validation:**

- [ ] A reorg cannot rewrite a completed plan; affected provisional evidence is
  superseded and operator/provider-visible.
- [ ] Failure injection leaves no leaked VM, mount, snapshot, reservation,
  attempt, artifact, or reliable-event gap.
- [ ] End-to-end progress, terminal result, artifacts, and provider surfaces
  prove exact plan coverage across all accepted shards.

## Final Validation

v36 is complete when one block-validation submission is planned centrally and
executed across multiple independently scheduled, operator-trusted workers with
different canonically verified snapshot tips; exact coverage, resource
isolation, failure recovery, cleanup, a measurable speedup over the pinned
single-worker baseline, and one convergent GitHub/Slack result are all proven
by durable evidence.
