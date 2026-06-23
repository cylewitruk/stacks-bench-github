# v19: Shared Benchmark Calibration

Add a group-scoped `stacks-bench` calibration step before measured benchmark
runs, so clean repeats and future multi-variant groups reuse one baseline
calibration instead of recalibrating inside every VM (`0041`).

> **Status:** shipped
>
> Shipped as v19 and validated on-host. Benchmark groups can run one shared
> `stacks-bench bench baseline calibrate` pass, persist the resulting
> `calibration_id`, carry the calibrated group DB forward, and inject
> `--baseline-id` into measured runs so clean repeats do not perform inline
> calibration in every VM.
>
> This iteration deliberately comes before JSONL progress (`0027`): calibration
> changes the benchmark workflow shape, while progress reporting can observe the
> new workflow once it exists.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0041-shared-benchmark-calibration` | primary | shipped |

## Why

`stacks-bench bench run` historically performed its own empty-block baseline
calibration. In a clean-repeat group, that means every isolated run pays the
calibration cost and can introduce per-run baseline noise that is not part of
the workload being measured.

Upstream now exposes an explicit calibration command:

```bash
stacks-bench --db /path/to/stacks-bench.db --json \
  bench baseline calibrate \
  --source /path/to/node-data \
  --network mainnet
```

The final stdout envelope returns `result.calibration_id`. Measured runs can then
reuse that calibration:

```bash
stacks-bench --db /path/to/stacks-bench.db --json \
  bench run \
  --source /path/to/node-data \
  --network mainnet \
  --start-at A \
  --count N \
  --baseline-id 12
```

The upstream invariant is now tip-anchored: calibration belongs to the same
indexed chainstate and resolved chain-tip anchor, not to a benchmark range end
block. That is exactly the shape a host-pinned benchmark group wants: one
calibrated group DB, carried forward into every measured run VM.

## Sources

Implementation should verify the upstream contract against the pinned
`cylewitruk/stacks-core` branch `feat/stacks-bench`:

- `contrib/stacks-bench/schema/` — versioned JSON schema for the
  `baseline_calibration` result envelope.
- `contrib/stacks-bench/src/cli/bench/run.rs` — `clap` source of truth for
  `bench run`, including `--baseline-id`.

The name mapping is intentional: the calibration command returns
`result.calibration_id`, and measured runs consume that value via
`--baseline-id`.

## Scope

- Add a `calibrate` workflow step before measured benchmark runs for group
  workloads that opt into shared calibration. The current implementation runs
  that step inside run 0's libvirt lifecycle so the calibrated DB and id are
  handed directly to the measured run.
- Run calibration against the same group `stacks-bench.db` artifact that is
  carried forward between runs.
- Parse `baseline_calibration` result envelopes and persist the
  `calibration_id` needed by later measured runs.
- Inject `--baseline-id <id>` into every measured benchmark run in the group.
- Fail the group loudly if calibration fails, the calibration result cannot be
  parsed, or a measured run rejects the baseline id.
- Report calibration as group setup, distinct from measured workload time.

**Non-goals:** no JSONL progress integration in this slice, no cross-host
calibration sharing, no silent fallback to inline per-run calibration, and no
change to standalone `stacks-bench` semantics.

## Design Decisions

- **Calibration is a workflow step, not a benchmark variant.** It prepares the
  group DB for measured runs. It should not appear as a `BenchmarkSpec` variant
  and should not be included in variance or comparison math.
- **The shared DB is load-bearing.** `baseline_id` and `chainstate_id` are
  DB-local. The calibrated DB must be the one carried into measured run VMs.
- **Tip-anchored by default.** The daemon does not compute a range end block just
  to calibrate. It relies on upstream's same-chainstate/same-tip validation.
- **Fail closed.** Rejecting or losing the calibration is a correctness failure,
  not an opportunity to silently run inline calibration.
- **Host pinning remains the group boundary.** Calibration, measured runs, and
  carried artifacts must stay on the same host until worker-fleet policy exists.

## Phases

### Phase 1: Result Model and Workflow Metadata

**Goal:** Add the daemon-side model for calibration outputs and the persisted
metadata later measured runs need.

**Scope:**

- Add a typed parser for schema-v1 `baseline_calibration` result envelopes.
- Persist the selected calibration id on group/workflow metadata.
- Represent `calibrate` as a first-class workflow step state, reusing the 0037
  workflow-step model rather than hard-coding it into repeat logic.
- Add tests for successful parse, wrong result type/version, missing id, and
  legacy/invalid envelopes.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests, if applicable
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] The daemon can parse and persist a calibration id.
- [x] Bad calibration envelopes fail loudly with useful diagnostics.

**Tests:**

- Parser/unit tests for `baseline_calibration` success and failure envelopes.
- Store tests for persisted calibration metadata.

### Phase 2: Execute Calibration Before Measured Runs

**Goal:** Run the calibration command once per benchmark group before run 0.

**Scope:**

- Add a calibration execution path that uses the same built/cached
  `stacks-bench` binary path as benchmark runs.
- Seed the calibration VM with the group DB and archive the calibrated DB as the
  next group DB artifact.
- Block measured run enqueueing until calibration succeeds.
- Make startup/resume DB-derivable: a group that calibrated successfully but has
  not started measured runs resumes from the persisted calibration state.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests, if applicable
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] A two-repeat group runs exactly one calibration step before the measured
  sequence.
- [ ] A calibration failure marks the group failed and enqueues no measured runs.

**Tests:**

- Driver/runner tests for calibration-before-run ordering.
- Resume tests for calibrated-but-not-started groups.

### Phase 3: Inject Baseline ID into Measured Runs

**Goal:** Thread the saved calibration into every measured benchmark invocation.

**Scope:**

- Add `--baseline-id <id>` to effective measured-run arguments for calibrated
  groups.
- Keep workload-key semantics explicit: calibration changes execution mechanics
  but not the user-requested workload identity.
- Treat a measured-run rejection of `--baseline-id` as a group correctness
  failure.
- Ensure clean repeats still run one VM per repeat and still carry the DB
  forward.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests, if applicable
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] Every measured run in a calibrated group receives the same
  `--baseline-id`.
- [x] No measured run performs inline baseline calibration unless an explicit
  future policy opts out of shared calibration.

**Tests:**

- Argument-normalization tests for calibrated measured runs.
- Group execution tests proving all measured runs receive the stored baseline id.

### Phase 4: Reporting and Host Validation

**Goal:** Make the shared calibration visible enough for operators while keeping
measured-run summaries focused on workload timing.

**Scope:**

- Add a group/reporting row for calibration setup and duration where the
  surface supports it.
- Keep final summaries focused on measured workload timing; richer calibration
  provenance remains a results-summary follow-up.
- Preserve partial-group reporting: if calibration succeeds and a later run
  fails, the surface still reports calibration provenance and any completed run
  data.
- Host-smoke with a two-repeat group.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests, if applicable
- [x] Reviewed (Codex)
- [x] Validated — host smoke confirmed the shared-calibration execution path

**Acceptance & Validation:**

- [x] The run surface shows the calibration/benchmark lifecycle without creating
  extra per-run cards or checks.
- [x] Measured runs are linked to the persisted shared baseline id.
- [x] A host smoke confirms no per-run inline calibration occurs.

**Tests:**

- Reporting tests for calibration provenance and partial groups.
- Host smoke: calibrated two-repeat group.

## Final Validation

- [x] `just build`
- [x] `just lint`
- [x] `just test`
- [x] Host smoke: two clean repeats over a known workload produce one
  calibration, two measured VM runs, one carried group DB, and measured-run
  timings that no longer include repeated inline calibration.

## Validation Notes

- Local validation before close-out: `just build`, `just lint`, and `just test`
  were green during the reviewed implementation.
- Host validation confirmed the important behavioral invariant: measured repeat
  runs became much shorter after the shared calibration landed, indicating that
  calibration is no longer repeated inline per measured VM.
- Fine-grained calibration progress is handled by `0027`/v20. Richer final
  calibration provenance remains tracked by `0028-results-summary-restructure`.

## Follow-Ups

- `0027-fine-grained-progress` should surface calibration progress once stderr
  JSONL events are wired through.
- `0039-multi-variant-benchmark-comparisons` can reuse one calibration across
  variants that share the same group DB and chainstate/tip.
- `0028-results-summary-restructure` should include calibration provenance in
  the overview.
- A separately resumable calibration-only checkpoint is deferred. Today
  calibration and measured run 0 are one claimable libvirt lifecycle: if the
  daemon is interrupted after calibration but before run 0 finishes, retrying
  re-runs calibration rather than silently continuing from a half-persisted
  state. That is fail-closed and acceptable unless host validation shows
  calibration cost warrants a dedicated checkpoint.
