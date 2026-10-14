# Decision 0007: Daemon-Owned Distributed Validation Plans

- **status:** draft
- **date:** 2026-08
- **related items:** `0015-resource-aware-admission`,
  `0026-central-block-index-cache`,
  `0052-managed-stacks-node-chainstate-producer`,
  `0075-rolling-worker-protocol-compatibility`,
  `0084-canonical-chainstate-worker-inventory`,
  `0085-distributed-block-validation-orchestration`,
  `0086-community-worker-validation-trust`,
  `0087-general-resource-lane-topology`

## Decision

The daemon owns the authoritative block-validation execution plan. It selects
one immutable canonical target, partitions the requested inclusive validation
range into durable plan shards, assigns eligible shards to workers, and
reduces their fenced results into one submission result. A worker owns local
admission and execution safety: it may reject an assignment that its current
snapshot, resource state, or sandbox cannot satisfy, but it may not silently
change the assigned range.

PostgreSQL is the compatibility authority for the canonical chain. It stores a
generation-aware canonical block ledger populated only by an operator-run
indexer over a qualified read-only chainstate snapshot. The ledger maps Stacks
height and block identity to the global validation ordinal and epoch-local
ordinal used by `stacks-inspect`, with parent identity, finality, source
provenance, and conflict state. A worker publishes an inventory of immutable
local chainstate snapshots, including a stable opaque snapshot ID, network,
tip height and hash, observed validation coverage, provenance, and freshness.
The daemon admits a snapshot only when its claimed tip and coverage agree with
one canonical ledger generation.

A matching tip establishes that the snapshot is compatible with a canonical
prefix; it does not make the worker or its results trustworthy. Workers cannot
promote blocks into the canonical ledger. Result authority is a separate
policy based on worker trust tier, identity, fencing generation, result digest,
and optional redundant or sampled validation.

Resource capability and resource availability are also separate. The first
contract advertises an operator-authorized maximum count of block-validation
VM slots and whether benchmark work excludes those slots. The daemon atomically
reserves a slot when placing work. Benchmark execution is mutually exclusive
with block validation in v36; general CPU, memory, storage, I/O, and
co-scheduling lane topology is later work (`0087`).

The first distributed contract assigns one durable plan shard per worker
allocation. The daemon may also include several exact execution slices inside
that shard for concurrent `stacks-inspect` processes in one VM. Slices are not
independent scheduling or retry units, and the worker does not derive their
boundaries. This preserves proven intra-worker parallelism without creating a
second range planner. A later compatible extension may bundle several durable
shards in one VM, but each shard retains its own identity and result.

If canonical ingestion exceeds its configured maximum age, has a gap, or is in
conflict, new centrally planned validations fail closed in a visible waiting or
rejected state. Capacity never selects an older semantic meaning for `latest`.
During rollout, an explicit operator rollback may route a monolithic request to
the previous worker-local contract; that is compatibility mode, not a silent
distributed-planner fallback.

## Rationale

Worker-local planning is safe for one host but cannot optimize or prove exact
coverage across a heterogeneous fleet. The daemon knows queued demand, worker
leases, negotiated features, canonical coverage, reported slots, and prior
throughput. It can therefore account for the roughly fixed VM/tool startup cost
and assign larger ranges to faster or more capable workers without making the
worker a second scheduling authority.

Using a canonical block ledger permits workers with different tips to
participate safely: each receives only ranges at or below the canonical prefix
its snapshot can cover. It also gives reorgs and finality explicit identities
instead of treating a human snapshot name or tip height as sufficient proof.
The canonical block identity layer is shared with `0026`; v23 remains the owner
of transaction facts, import packs, and benchmark pre-seeding.

## Consequences

- Distributed validation requires negotiated worker inventory, validation-slot,
  and explicit-assignment features; revision-1 workers retain monolithic
  worker-local planning during the compatibility window.
- A request such as “latest N” is frozen to a specific canonical generation and
  terminal ordinal before any shard is offered. Later tip movement cannot
  change an active plan. Reports expose the generation, range, finality, and
  ledger age used to resolve it.
- Initial authoritative scheduling uses finalized canonical ledger entries.
  Provisional near-tip validation requires an explicit policy and remains
  generation-pinned so a reorg can supersede, never rewrite, prior evidence.
- Workers with older but canonical snapshots remain useful for ranges within
  their coverage. A non-canonical, conflicting, stale, or unverifiable
  inventory is ineligible and operator-visible.
- Scheduler estimates use startup cost plus learned per-worker throughput, but
  correctness never depends on those estimates. Small requests stay on one
  worker when distribution would add more startup cost than useful parallelism.
- Parent completion requires exact, non-overlapping canonical coverage and an
  accepted terminal result for every shard. Worker loss reassigns only
  incomplete shards; stale terminals are rejected by shard fencing.
- Public-key worker identity authenticates the sender, not the computation.
  Community capacity must be corroborating, redundantly checked, sampled, or
  otherwise admitted by explicit trust policy before it can authoritatively
  determine a verdict. v36 therefore admits only operator-trusted workers;
  community result authority is owned by v37 (`0086`).
- v36 includes a repeatable manual bootstrap that places and verifies a
  qualified snapshot on a second trusted worker. Automated snapshot production,
  refresh, and distribution remain separate: `0052` owns managed production,
  and a future cache/transfer item may extend the inventory contract.
