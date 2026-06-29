# v23: Central Chainstate Index Ledger

Promote `0026` into an operational fix for expensive first-run indexing on
fresh or ad-hoc chainstates: reuse provenance-checked canonical block/tx index
facts from Postgres instead of rediscovering millions of historical
blocks/transactions.

> **Status:** in_progress - planning complete; implementation not started.
>
> v22 remains in progress for comparison validation, but host testing exposed a
> separate blocker: older tx/range workloads on fresh ad-hoc chainstates can
> spend over an hour indexing before measurement. This iteration addresses that
> first-run cost without changing benchmark semantics.
>
> This is a larger, two-codebase iteration: upstream `stacks-bench` gains the
> export/import boundary, while sbgh adds the Postgres ledger, an exclusive
> `chainstate-index` job type, and benchmark pre-seed orchestration. The spike
> and manual-tooling phases keep that scope controlled before daemon automation.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0026-central-block-index-cache` | primary | in_progress |

## Why

`stacks-bench` stores block/transaction discovery metadata in its SQLite app DB.
v15/v19/v22 carry that DB across repeats and variants once a group is running,
but the first measured execution for a fresh chainstate still starts with an
empty DB. For old txids or deep ranges, that means walking millions of blocks
from tip before any benchmark work begins.

Host observation from 2026-06-25: a benchmark against a newly-loaded ad-hoc
squashed chainstate had indexed 5M blocks after ~1h15m and still had not started
measurement. That turns ad-hoc/pre-release chainstate testing into an indexing
job, not a benchmark job.

The fix is to make the reusable part of `stacks-bench.db` explicit through
`stacks-bench` itself, then store the exported finalized index facts in sbgh's
Postgres instance. At run time, sbgh generates a workload-scoped import pack
from that central ledger and asks `stacks-bench` to import it into the run DB.
Each run still owns its calibration/run/metric rows as it does today.

Indexing itself should be a first-class sbgh job, not surprise work hidden
inside a user benchmark. A `chainstate-index` job can run on a cron or be queued
manually for a source LV, use an sbgh-selected `stacks-bench` binary, and claim
exclusive access so no benchmark runs concurrently with heavy indexing.

A Postgres ledger is intentionally heavier than storing exported pack files in
artifact storage. It buys cross-chainstate deduplication for shared finalized
history, queryable coverage, conflict/quarantine visibility, and
workload-scoped pack generation so a tx/block/range run imports only what it
needs.

## Scope

- Add upstream `stacks-bench chainstate export` and `chainstate import`
  commands, or an equivalent library API, so SQLite schema semantics stay in the
  tool that owns the DB.
- Define a stable export/import contract for reusable block/tx discovery facts.
- Add a Postgres ledger for finalized canonical index facts, indexed by network
  and canonical block identity rather than human chainstate labels.
- Add a `chainstate-index` job type that can be queued manually or by cron and
  is exclusive with benchmark execution.
- Capture enough provenance to prove ledger facts are compatible with the
  target chainstate before they are packed for import.
- Add operator tooling to inspect/export/import ledger coverage before the
  daemon uses it automatically.
- Pre-seed a fresh or carried group DB by generating a workload-scoped import
  pack from Postgres and invoking `stacks-bench chainstate import`, never by
  daemon-side SQLite table surgery.
- Import newly indexed material into Postgres as the terminal step of successful
  `chainstate-index` jobs by invoking `stacks-bench chainstate export`.
- Keep ledger use best-effort and fail-closed on incompatibility: missing
  coverage should, by default, enqueue a chainstate-index job and defer the
  benchmark until coverage exists. Reject-only and live-index fallback are
  explicit policy choices; incompatible selected facts must not be imported.

**Non-goals:** no daemon-side SQLite row mutation, no direct clone of
`stacks-bench` app tables into Postgres, no cross-network sharing, no
not-yet-final block pre-seeding, no portal UI, and no changes to benchmark
result interpretation.

## Sources

Upstream source of truth lives in `cylewitruk/stacks-core` branch
`feat/stacks-bench`:

- `contrib/stacks-bench/migrations/` — SQLite app DB migrations.
- `contrib/stacks-bench/src/db/app/schema.rs` and
  `contrib/stacks-bench/src/db/app/models.rs` — generated schema and typed app
  DB models.
- `contrib/stacks-bench/schema/v1.json` — current versioned CLI/result/event
  schema.
- `contrib/stacks-bench/src/cli/chainstate/` — proposed home for the new
  `chainstate export` / `chainstate import` commands.

## Design Decisions

- **The carried group DB remains authoritative for a group.** If a run already
  has a group SQLite seed, v23 may import additional compatible index rows into
  that DB through `stacks-bench`, but it must never overwrite existing
  run/calibration/metric state.
- **The schema owner owns schema semantics.** Because we control the upstream
  `stacks-bench` branch, v23 should implement `chainstate export/import` there
  first. The daemon is best at orchestration; the tool that owns the SQLite
  schema is best at deciding which rows are reusable and how finality/reorg
  rules apply.
- **Chainstate indexing is a first-class job.** Heavy indexing runs as a
  `chainstate-index` job, not as incidental benchmark setup. It is queueable by
  cron or by an operator and is exclusive with benchmark groups so disk-heavy
  indexing cannot distort benchmark measurements.
- **sbgh chooses the indexing binary.** `chainstate-index` jobs use an
  sbgh-configured `stacks-bench` binary or build, normally the latest known-good
  version with the richest export support. Benchmark jobs may use older or
  branch-specific binaries, so generated packs must declare importer
  compatibility.
- **The import actor is a Phase 1 decision.** v23 must explicitly choose whether
  a benchmark's own `stacks-bench` binary imports the pack, or whether sbgh's
  configured indexing binary imports the pack into `stacks-bench.db` before the
  benchmark binary starts. The latter may be required for old-ref benchmarks
  whose binaries can read pre-populated index tables but do not have the new
  `chainstate import` command.
- **Postgres is the central ledger.** Exported index facts are imported into
  sbgh's Postgres DB and deduplicated across compatible chainstates. The ledger
  stores canonical facts and source/provenance records; it does not store
  benchmark runs, calibrations, metrics, or a copy of the `stacks-bench` app DB.
- **The ledger is a remodel, not a table clone.** Postgres may represent the
  same reusable semantics as `stacks-bench` index tables, such as canonical
  height-to-block mappings and tx locations, but it should model them as
  network/provenance-keyed discovery facts rather than mirroring SQLite table
  names, columns, or app-state relationships.
- **Import packs are generated views of the ledger.** A pack should contain only
  the facts needed for a requested tx/block/range workload plus a manifest. The
  measured run still owns its `stacks-bench.db` app state.
- **Use SQLite as the pack primitive, but keep it opaque to the daemon.** Prefer
  an index-only SQLite file with a manifest table/sidecar for the
  `stacks-bench` import/export boundary. sbgh may create that file from
  Postgres rows, but `stacks-bench` owns how the file maps into its app DB.
- **Packs are versioned for importer compatibility.** The pack manifest must
  include producer `stacks-bench` version/commit, DB/app schema version,
  pack schema version, and supported importer version/schema range. sbgh may
  generate different pack shapes from the same ledger if different benchmark
  refs require different import contracts.
- **Provenance is load-bearing.** Ledger facts and generated packs are keyed by
  network, canonical block identity, source identity, `stacks-bench`
  DB/schema version, index-pack schema version, and the finalized height range
  they cover. A mismatch means no import.
- **Chainstate identity must be content-derived.** Human labels such as
  `adhoc-squash-01` are useful operator handles but are not safe provenance.
  The identity should include a content-derived marker from finalized chainstate
  data, for example canonical mapping/hash data at a known finalized height.
- **Finality gates pre-seeding.** Ledger-backed imports may cover finalized
  history only. Anything near tip must be indexed by a `chainstate-index` job
  and ingested later once final.
- **Conflict handling is conservative.** Idempotent duplicates are fine. A
  canonical mapping/tx-index disagreement below finality is a correctness
  failure for the source/export and must be quarantined, not silently
  overwritten.
- **Whole-file seed first, table merge only when needed.** A fresh first run can
  start from a generated compatible index-only DB. Table-level merge is required
  only when the destination already has app state, such as a carried group DB
  with calibration/run rows, and that merge should live inside `stacks-bench`.
- **Manual tooling comes before daemon automation.** The first green checkpoint
  should let an operator export, ingest, generate, and import against a copied
  DB. Only then should the runner wire it into job execution.
- **Exclusivity is group-scoped.** A benchmark group, including clean repeats
  and multi-variant comparisons, is the unit of measurement isolation. A
  chainstate-index job must not start between runs of the same group, and a
  benchmark group must not start while chainstate indexing is active.
- **Benchmarks consume prepared coverage.** Benchmark jobs should not silently
  perform hours of missing chainstate indexing by default. Missing coverage
  should default to enqueuing a `chainstate-index` job and deferring the
  benchmark. Reject-only and live-index fallback are explicit operator
  policies.
- **Ad-hoc chainstates are first-class.** The feature must work for temporary
  LVM base LVs such as `adhoc-squash-01`, not only the daily `mainnet-*`
  baseline.
- **Compatibility with v19/v22 is required.** Pre-seeding must compose with
  shared baseline calibration, clean repetitions, and multi-variant
  comparisons. It should reduce first-run discovery time without changing the
  group run order or calibration identity.
- **The round trip must be lossless for reusable facts.** Exporting from
  `stacks-bench`, ingesting into Postgres, generating a pack, and importing back
  into a fresh app DB must preserve every reusable fact `stacks-bench` needs to
  avoid re-indexing the covered workload.

## Phases

### Phase 1: Export Contract, Job Model, and Provenance Spike

**Goal:** Define the `stacks-bench`-owned export/import contract, the
`chainstate-index` job model, the Postgres ledger schema, finality boundary, and
pack manifest before wiring daemon automation.

**Scope:**

- Inspect real `stacks-bench.db` files from current `feat/stacks-bench` runs.
- Identify reusable block/tx discovery tables and non-reusable app-state tables
  (runs, metrics, calibrations, samples, etc.).
- Specify `stacks-bench chainstate export` and `stacks-bench chainstate import`
  command shapes, JSON result envelopes, and failure modes.
- Design the sbgh Postgres ledger tables for canonical blocks, tx locations,
  source/provenance records, coverage ranges, and conflict quarantine.
- Define an `IndexPackManifest` with network, chainstate identity,
  `stacks-bench` DB/schema version, pack schema version, covered finalized
  height range, source DB metadata, and creation time.
- Decide how chainstate identity is computed for normal and ad-hoc LVM base
  LVs. It must include content-derived finalized-chainstate data, not only a
  human label or LV name.
- Pin the finality boundary used for export/import, including which canonical
  height-to-block mapping is safe to import and how far from tip export must
  stop.
- Define the `chainstate-index` job model, benchmark-group-scoped exclusive
  scheduling semantics, missing-coverage policy (default: enqueue indexing and
  defer benchmark), and indexing-binary selection/version fields.
- Decide the import actor for benchmark pre-seeding:
  benchmark-binary-import vs. indexing-binary-prepopulate-then-benchmark-read.
  Record the compatibility requirements for the chosen path.
- Document the mapping from upstream export facts into Postgres ledger rows and
  from ledger rows back into a workload-scoped import pack.
- Document exact import keys/conflict policy inside `stacks-bench`, even though
  sbgh will not mutate SQLite app rows directly.
- Identify any reusable facts that cannot survive export -> Postgres -> pack ->
  import without extra upstream schema/API support.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] A real completed `stacks-bench.db` can be inspected and classified into
  reusable vs. non-reusable tables.
- [ ] The manifest contains enough provenance to reject a pack from a different
  network, chainstate source, or incompatible DB schema.
- [ ] The `chainstate export/import` command contract is specified enough for
  sbgh to call it without understanding internal SQLite tables.
- [ ] The Postgres ledger schema can represent idempotent canonical block/tx
  facts across compatible chainstates without storing run/calibration/metric
  state.
- [ ] Export -> Postgres ingest -> generated pack -> import is specified as a
  lossless round trip for all reusable facts needed by `stacks-bench`.
- [ ] The `chainstate-index` job model, benchmark-group-scoped exclusivity rule,
  indexing-binary provenance, and missing-coverage policy are specified before
  implementation.
- [ ] The import actor is decided, including how old-ref benchmarks without
  `chainstate import` support can still consume pre-seeded index data if
  possible.
- [ ] Finality and content-derived chainstate identity are defined as
  correctness gates.

**Tests:**

- Upstream SQLite fixture tests for schema classification where practical.
- Postgres schema sketch/fixture tests for idempotent fact ingestion where
  practical.
- Manual inspection notes from at least one current host-produced DB.

**Notes:** Do not proceed with daemon-side table merge code unless the upstream
command path proves impossible. The default design is that sbgh invokes
`stacks-bench`, and `stacks-bench` owns all DB-row semantics.

### Phase 2: Manual Ledger Ingest and Pack Generation

**Goal:** Provide an operator-safe way to ingest exported index facts into
Postgres and generate/import workload-scoped packs outside the runner.

**Scope:**

- Implement `stacks-bench chainstate export` to write an index pack plus
  manifest from a source `stacks-bench.db`.
- Implement `stacks-bench chainstate import` to validate and merge a pack into a
  destination `stacks-bench.db`, preserving all non-index app state.
- Add sbgh code/CLI to ingest an exported pack into the Postgres ledger
  idempotently, with explicit conflict/quarantine behavior.
- Add sbgh code/CLI to generate a workload-scoped import pack from Postgres for
  a requested tx/block/range and target chainstate provenance.
- Add a thin sbgh/operator wrapper only where useful for host ergonomics:
  export from a completed DB, ingest to Postgres, generate a pack, and import
  into a copied DB.
- Keep the first implementation local-filesystem capable; artifact-store/S3
  publication can be added once the format is proven.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] Exporting the same DB twice produces equivalent manifests and idempotent
  ledger rows.
- [ ] Ingesting the same export twice into Postgres is idempotent.
- [ ] Generating a pack for a tx/block/range includes only the needed compatible
  finalized facts.
- [ ] Importing a generated compatible pack into an empty app DB seeds index
  rows without creating run/calibration/metric rows.
- [ ] Export -> Postgres ingest -> generated pack -> import round-trips a
  fixture without forcing `stacks-bench` to rediscover covered blocks/txs.
- [ ] Importing an incompatible generated pack is rejected before any
  destination DB rows are modified.

**Tests:**

- Upstream unit tests over SQLite fixtures for export/import/idempotency/conflicts.
- Postgres ingest/generate fixture tests for idempotency, coverage, and
  conflicts.
- CLI/script smoke on a copied host DB.

### Phase 3: Chainstate-Index Job and Exclusive Scheduling

**Goal:** Make ledger population an explicit, exclusive job type that can run
from cron or operator action.

**Scope:**

- Add a `chainstate-index` task/job kind with source LV/network/provenance,
  finalized coverage target, and configured `stacks-bench` indexing binary.
- Add cron/manual enqueue paths for chainstate-index jobs.
- Extend queue/admission so chainstate-index jobs are exclusive with benchmark
  groups in both directions: no benchmark group starts while indexing is active,
  and no indexing starts until the whole active benchmark group is terminal.
- Run the selected `stacks-bench` binary to index/export finalized facts and
  ingest them into Postgres.
- Record ledger statistics: rows imported, conflicts, covered range, producer
  binary version, and source identity.
- Keep ledger-update failures scoped to the chainstate-index job; benchmark
  jobs should not inherit partial/failed indexing state.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] A chainstate-index job can be enqueued manually for an ad-hoc LV/source.
- [ ] A cron path can enqueue the same job shape without bypassing admission.
- [ ] Queue/admission prevents benchmark groups and chainstate-index jobs from
  running concurrently, including between repeats or variants of one group.
- [ ] The job uses the configured indexing `stacks-bench` binary and records its
  version/commit in ledger provenance.
- [ ] Finalized conflicts are detected and quarantined.

**Tests:**

- Store/queue tests for group-scoped exclusive admission and active-job
  detection.
- Job execution tests with a fake/stub `stacks-bench` export.
- Ingest tests proving failed chainstate-index jobs do not publish partial
  coverage.

### Phase 4: Benchmark Pre-Seed Consumer Integration

**Goal:** Generate and import a compatible index pack into the benchmark run DB
without turning missing coverage into surprise indexing work.

**Scope:**

- Add config for enabling ledger-backed index pre-seeding and selecting its
  local/artifact-store scratch/root.
- Resolve compatible ledger coverage from the job's network, chainstate
  identity, workload target, finalized coverage, and benchmark importer
  compatibility.
- Generate a workload-scoped import pack from Postgres into local scratch space.
- After the results tmpfs is mounted and after any group SQLite seed is copied,
  pre-seed `tmpfs.sqlite_file()` using the Phase 1 import-actor decision.
- If no compatible coverage exists, enqueue a chainstate-index job and defer the
  benchmark by default. Reject-only and live-index fallback remain explicit
  policy choices.
- If a selected candidate fails validation/conflict checks, fail closed and
  report/log enough provenance to debug; do not import partial data.
- Surface ledger hit/miss/skip information in logs and, if useful, in the
  Slack/GitHub progress surface without making it user-noisy.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] A fresh first run can start from a generated compatible import pack rather
  than an empty `stacks-bench.db`.
- [ ] An old-ref benchmark whose binary lacks `chainstate import` either uses
  the indexing-binary pre-population path or fails closed with a clear
  incompatibility reason.
- [ ] A repeated/comparison group with an existing carried DB keeps its group
  state and only receives missing compatible index rows.
- [ ] Missing ledger coverage does not silently run heavy indexing unless
  explicit live-index fallback is enabled.
- [ ] Incompatible selected ledger coverage is rejected without corrupting the
  destination DB.

**Tests:**

- Driver/unit tests around seed order: carried DB first, index import second.
- Integration tests with empty and pre-existing destination DB fixtures.
- Admission/policy tests for missing coverage: default enqueue+defer,
  reject-only, and explicit live-index fallback.

### Phase 5: Ledger Maintenance and Version Compatibility

**Goal:** Keep the ledger queryable, maintainable, and safe across
`stacks-bench` importer versions.

**Scope:**

- Add maintenance hooks for coverage/source/listing so operators can see what
  exists.
- Track producer `stacks-bench` version/commit, pack schema version, DB/app
  schema version, and supported importer version/schema range.
- Ensure pack generation chooses a shape compatible with the benchmark run's
  `stacks-bench` importer, or refuses pre-seeding with a clear reason.
- Add quarantine/diagnostic handling for conflicting finalized data.
- Add removal/quarantine controls for bad source exports or obsolete coverage.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] Operators can list ledger coverage and source/provenance records.
- [ ] Pack generation fails closed when the benchmark importer cannot safely
  import the available pack schema.
- [ ] Quarantined conflicts are visible and are not used for generated packs.
- [ ] Removing/quarantining a source prevents its facts from being selected.

**Tests:**

- Version-compatibility fixture tests for supported and unsupported importers.
- Ledger maintenance tests for listing, quarantine, and source removal.

### Phase 6: Host Smoke, Ad-Hoc Workflow, and Docs

**Goal:** Validate that v23 solves the observed host problem and document the
operator flow.

**Scope:**

- Export and ingest index facts for at least one ad-hoc squashed chainstate.
- Run a chainstate-index job against at least one ad-hoc squashed chainstate and
  confirm no benchmark runs concurrently.
- Run an old txid/range benchmark against a fresh DB with and without a
  generated pack to confirm the first-run indexing reduction.
- Confirm v15/v19/v22 groups still carry the DB correctly after pre-seeding.
- Document how to list ledger coverage, export facts, ingest facts, generate
  packs, and remove/quarantine sources.
- Document safety boundaries: provenance, finality, incompatible schemas, and
  when to fall back to live indexing.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] Host smoke: the observed old-tx ad-hoc workload no longer spends tens of
  minutes walking millions of already-known blocks before measurement.
- [ ] Host smoke: compatible ledger coverage and generated-pack import are
  visible in logs and do not alter benchmark result semantics.
- [ ] Host smoke: a queued benchmark waits or rejects while a chainstate-index
  job is active; no benchmark VM runs concurrently with indexing.
- [ ] Host smoke: missing ledger coverage auto-enqueues indexing and defers the
  benchmark by default instead of silently running long indexing work.
- [ ] Host smoke: an incompatible selected pack is rejected and the job fails
  closed without importing partial data.
- [ ] Docs explain how to seed ad-hoc chainstate test runs.

**Tests:**

- Host smoke checklist.
- Operator docs/help snapshot tests where applicable.

## Final Validation

- [ ] `just build`
- [ ] `just lint`
- [ ] `just test`
- [ ] Manual export/ingest/generate/import smoke against a copied
  `stacks-bench.db`.
- [ ] Chainstate-index job smoke showing exclusive scheduling with benchmarks.
- [ ] Host smoke on an ad-hoc squashed chainstate using an old txid/range.
- [ ] Regression smoke: existing clean-repeat and two-variant comparison groups
  still carry DB/calibration state correctly.

## Follow-Ups

- Richer ledger observability in Slack App Home (`0035`) once coverage
  maintenance is useful to operators.
- Resource/admission integration (`0015`) if export/ingest/pack generation
  becomes large enough to affect host scheduling.
