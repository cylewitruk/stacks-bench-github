# Design 0052: Managed stacks-node chainstate producer

- **id:** `0052-managed-stacks-node-chainstate-producer`
- **status:** `backlog`
- **depends_on:** `0015-resource-aware-admission`
- **relates_to:** `0026-central-block-index-cache`,
  `0039-multi-variant-benchmark-comparisons`,
  `0063-libvirt-block-validation`
- **source:** chainstate freshness/provenance design discussion (2026-06)

Run a real `stacks-node` under sbgh control so chainstate freshness, snapshot
creation, and benchmark admission are one coordinated system. The node advances
a writable base LV while the host is idle; sbgh gracefully stops it before
benchmarks, index jobs, or snapshot cut points.

## Goals

- Replace external archive/nightly-LV production with sbgh-managed chainstate
  production.
- Produce read-only LVM-thin snapshots tagged with the node version, network
  epoch, tip identity, and cut reason.
- Support fresher benchmark requests from monitors without waiting for archive
  downloads when the managed node is sufficiently caught up.
- Build a curated chainstate timeline by syncing forward through historical
  release/epoch/daily cut points.
- Keep benchmark/index jobs isolated from live node writes and host IO churn.

## Non-Goals

- No snapshots from a live writing node in the first implementation.
- No benchmark or index job runs directly against the mutable node LV.
- No automatic "all releases in a time window" ref expansion; that remains a
  separate request-planning feature.
- No assumption that near-tip snapshots are safe for `0026` reusable index facts
  until the finality boundary marks their contents reusable.

## Model

### Two Freshness Modes

This item covers two related but different workflows:

- **Historical backfill:** sync forward from an older release through known
  release/epoch/daily cut points. This is one-time, can run for days or weeks,
  and tolerates being interrupted by benchmark work.
- **Near-tip maintenance:** keep a managed node close enough to tip that
  monitors can request fresh benchmarks. This is best-effort under an idle-only
  policy: a busy benchmark host may not give the node enough sync time to catch
  up.

Near-tip freshness must therefore be explicit policy, not assumed. Options
include best-effort idle sync with archive fallback, reserved sync windows, or a
separate non-benchmark host for the producer node.

### Mutable Base LV

The managed node owns one writable base LV. It is the only process allowed to
mutate that LV. sbgh treats it as a production source, never as a benchmark
input.

### Read-Only Snapshot LVs

Snapshots are taken only after a graceful node shutdown and quiescence checks.
They are tagged with provenance and used by benchmark/index jobs. The intended
thin-LVM shape is:

```text
managed writable node LV
  sync until cut point
  graceful stop
  snapshot read-only checkpoint
  restart/sync forward
```

The snapshots are not full chainstate copies. Storage growth is primarily the
changed extents retained by old checkpoints, plus thinpool metadata.

[v26](../archive/completed/0062-sandboxed-worker-execution.md) is the current
consumer contract: every published chainstate is a read-only origin LV selected
locally by prefix. Manifests and tags remain useful inventory/provenance
metadata, but consumers do not require them for admission; block-validation
guests probe and
report actual coverage.

### Cut Plan

A cut plan is the ordered set of snapshot targets. Initial backfill can sync
forward from an older release (for example a 3.3.x-era point) and stop at:

- stacks-node release/version boundaries
- network epoch boundaries
- daily points, defined by canonical block timestamp policy
- explicitly requested incident/slow-block heights

Ongoing operation can add:

- nightly point-in-time snapshots
- release/epoch transition snapshots
- operator-requested snapshots before risky maintenance

For historical "daily" cuts, define the policy explicitly, such as "first
canonical block at or after UTC midnight." Block timestamps are approximate; the
policy only needs to be stable and documented.

## Provenance

Each snapshot record should include:

- LV name and thinpool
- parent/base LV identity
- network
- producer node version and commit
- node config hash or named profile
- chain tip height/hash and burn height/hash
- network epoch
- created_at and cut reason (`daily`, `release`, `epoch`, `manual`, `pre-job`)
- replayable floor / squash height when known
- validation status and notes

Snapshots are selected by provenance, not by name strings alone. A "3.4.0.0.2
snapshot" means "produced by node version/commit X at chain tip Y", not merely
"intended for benchmarks of X."

## Lifecycle

### Idle Maintenance

When the benchmark queue is idle and resource policy allows it:

1. Start or resume the managed node.
2. Monitor sync progress and host resource health.
3. Stop at the next cut point or continue toward tip.
4. If a benchmark/index job appears, begin graceful shutdown.

This "sync while idle" mode trades freshness for benchmark start latency: a new
benchmark may wait for graceful node shutdown before it can claim the host. A
more conservative mode is "sync only in explicit windows," which avoids the
per-benchmark shutdown cost but produces staler snapshots. The iteration that
implements this item must choose that policy deliberately.

### Snapshot Protocol

1. Request graceful node shutdown.
2. Wait for process exit and filesystem quiescence.
3. Run validation probes (`xfs` mountability, expected dirs, optional SQLite
   quick checks).
4. Create the read-only LVM-thin snapshot.
5. Record provenance and validation state.
6. Optionally restart the node if the host is idle.

### Benchmark / Index Admission

Managed-node sync and snapshot work is exclusive with benchmark submissions and
chainstate-index jobs. A v22 comparison group or clean-repeat submission is an
indivisible host unit: the node must not restart between variants/repeats.

If a job needs a fresh snapshot, sbgh stops the node and snapshots before
claiming/running the job. Jobs use only the resulting read-only snapshot or a
per-job writable descendant.

Producer/consumer compatibility is part of snapshot selection. A chainstate
produced by node version X must be readable by every benchmark ref that will
consume it. This is not new — external archives already carry an implicit
producer version — but sbgh-managed provenance should make the constraint
visible and enforceable.

## Thinpool Safety

LVM-thin snapshots are the right primitive, but sbgh must track pool health:

- thinpool `Data%` and `Meta%`
- per-LV `Data%`
- snapshot lineage
- retained cut count by class
- minimum free data/meta headroom before sync/snapshot/job admission

The retention policy should prefer release/epoch snapshots over daily points
when space is tight. Pool exhaustion must be fail-closed: stop node sync and
reject/defer new snapshot work before writes can suspend LVs.

## Interaction With `0026`

`0052` produces chainstate snapshots and provenance. `0026` builds the central
ledger of reusable finalized index facts. The two should meet at explicit
metadata boundaries:

- snapshots expose tip/finality/replayable-floor information to `0026`
- `0026` decides which finalized facts are reusable
- near-tip snapshots may be useful for benchmarks before they are useful for
  the central ledger

Do not merge the two workstreams: node lifecycle/provenance and index-ledger
semantics are separate concerns.

The managed node is also the natural producer for proactive `0026` ledger
updates. At selected cut points, sbgh can run the `stacks-bench chainstate
export` path against the freshly quiesced snapshot and import finalized facts
into the central ledger before any benchmark asks for them. That hand-off should
stay behind the `0026` contract, not leak SQLite/index semantics into node
supervision.

## Open Questions

- What is the initial historical cut plan (release tags, epoch boundaries, daily
  cadence, start height)?
- Which exact node versions should produce compatibility snapshots?
- Which benchmark refs can consume each producer-version snapshot?
- Is near-tip maintenance best-effort idle sync, scheduled sync windows, or a
  separate producer host?
- Should the node normally run while idle, accepting graceful-stop latency on
  benchmark arrival, or stay stopped except during explicit sync windows?
- What validation probes are required before a snapshot becomes selectable?
- Should node upgrades happen in-place on the mutable base LV, or through a
  writable descendant of the latest validated snapshot?
- What thinpool headroom thresholds should block sync, snapshot, and benchmark
  admission?

## Known Limitations

### Epoch-Boundary Cuts Are Best-Effort

The spike scripts observe the node tip through `/v2/info` while the node is
running, then gracefully stop it before snapshotting. The node keeps processing
blocks during graceful shutdown, so between the tip read and the frozen snapshot
the tip can advance — during fast historical sync, far enough to cross an epoch
boundary.

`managed-node-snapshot.sh` therefore only applies a **pre-stop early-rejection
guard**: it fails a cut whose tip has *already* overshot the target epoch at read
time, and warns when a cut is boundary-adjacent (`epoch_guard.boundary_adjacent`
in provenance). It cannot detect a crossing that happens during shutdown. An
overshoot is **not** recoverable by resuming the backfill — the node has passed
the boundary and must be reset to an earlier chainstate checkpoint.

Snapshots produced this way are best-effort operational cut points, **not**
authoritative epoch boundaries. The provenance sidecar records this in
`epoch_guard.limitation`.

**Required follow-up:** node-level stop-at-height (halt processing at an exact
burn/stacks height, or read the persisted tip from the stopped chainstate) is a
prerequisite before any epoch-activation snapshot can be treated as an exact
consensus boundary. Until then, treat pre-activation cuts as approximate.

## First Slice

The first implementation should be a host-local spike:

1. Add a snapshot provenance table/model without changing benchmark selection.
2. Manually configure one managed node LV and systemd service.
3. Teach sbgh to stop the node, create one tagged snapshot, record provenance,
   and restart the node.
4. Enforce exclusive admission while the node is stopping/snapshotting.
5. Validate on-host that a benchmark can run against the produced snapshot.
