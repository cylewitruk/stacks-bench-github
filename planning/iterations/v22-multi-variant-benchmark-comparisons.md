# v22: Multi-Variant Benchmark Comparisons

Promote explicit ad-hoc comparison benchmarks so a Slack request can benchmark
one workload against two refs and report one noise-aware delta summary (`0039`).

> **Status:** in_progress - Phase 2 implemented and ready for external review.
>
> v21 remains planned but deferred. Comparison benchmarks are the next
> operational need, and they can build on the already-shipped group/run,
> repetition, calibration, and Slack-session work without waiting for the native
> schema-v1 parser cleanup.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0039-multi-variant-benchmark-comparisons` | primary | in_progress |

## Why

Operators need Slack-driven comparisons such as:

```text
@BenchBot bench tx <txid> on sb-integration/3.4.0.0.3 compare sb-integration/3.4.0.0.2
```

Today the daemon can compare PR runs against a configured baseline, but it does
not have a first-class ad-hoc shape for "same workload, two explicit refs, one
summary." The v14/v15/v19 model now gives us the right foundation:

- a benchmark group owns the user-facing surface and artifact prefix;
- each `BenchmarkSpec` represents one concrete workload/ref variant;
- runs remain isolated VMs and are ordered by the group runner;
- the carried group DB and per-variant calibration can cover multiple measured
  executions on the same host.

This iteration turns that foundation into a narrow, useful comparison feature
without taking on matrix scheduling, automatic baseline selection, or open-ended
LLM planning. Natural-language Slack comparison requests are in scope, but the
model may only emit the same bounded two-variant request shape as the
deterministic parser.

## Scope

- Support explicit two-ref benchmark comparisons for the same workload.
- Support natural-language Slack comparison requests through the LLM intent
  resolver, mapped into the same daemon-owned request model as the deterministic
  syntax.
- Create one `BenchmarkGroup` with multiple `BenchmarkSpec` variants.
- Execute variants serially on the same host, with at most one run in flight for
  the group.
- Reuse the same group `stacks-bench.db` carry-forward path across variants.
- Calibrate once per variant by default, then reuse that variant's calibration
  across its clean repeats.
- Build/resolve each variant independently through the existing build cache and
  commit-resolution path.
- Render one comparison summary on the group reporting surface.

**Non-goals:** no variant matrix, no automatic baseline selection, no parallel
variant execution, no worker-fleet scheduling, no portal UI, and no dependency
on the v21 native schema-v1 cleanup.

## Design Decisions

- **First slice is exactly two variants.** The data model can hold more specs,
  but the user-facing request path caps at two variants until admission/resource
  policy is richer.
- **Natural language is the primary Slack UX.** Deterministic flags are useful
  for tests, docs, and operator precision, but v22 must teach the LLM resolver
  to produce the same validated comparison request for prompts like
  "benchmark tx X between ref A and ref B." The LLM still emits structured data
  only; daemon validation owns caps, duplicate-ref rejection, target validation,
  and all values bound into jobs.
- **One request model, one validation path.** Deterministic parsing and LLM
  intent resolution should both converge on the same `ComparisonRequest` shape
  before validation. Do not duplicate cap/ref/workload validation per entry
  point; one validator should own all enqueue-admission checks.
- **The workload is the existing `WorkloadSpec`.** Comparison support must wrap
  the already-validated workload target model, not create a parallel grammar.
  That means txids, block selectors, and block ranges are all supported wherever
  the underlying `WorkloadSpec` supports them.
- **Two variants is a validation cap, not an internal shape.** Author store,
  scheduling, calibration, and comparison-result APIs as N-shaped collections
  (`Vec` of specs and `Vec` of deltas versus spec 0), then enforce
  `max_variants = 2` at request validation for this iteration. That keeps future
  "compare many refs" work to cap/policy/reporting changes rather than a model
  retrofit.
- **Variant identity lives in `BenchmarkSpec`.** The shared SQLite DB remains
  the raw benchmark artifact; daemon rows map run metrics back to variant/ref.
- **Serial group execution.** A comparison group is host-pinned and executes in a
  deterministic order: all runs for `spec_index = 0`, then all runs for
  `spec_index = 1`. That preserves the carried DB/calibration invariants and
  avoids concurrent SQLite writers.
- **Calibration is variant-scoped by default.** v19's shared calibration is
  correct for clean repeats of the same ref, but a branch comparison may change
  empty-block commit behavior. v22 should calibrate each variant once and reuse
  that variant's `baseline_calibration_id` across repeats of that same variant.
  Do not share one calibration across different refs unless a future explicit
  policy opts into that tradeoff.
- **The group DB remains shared.** Per-variant calibration rows live in the same
  carried group SQLite DB. Cross-spec carry-forward is still required so variant
  1 sees variant 0's indexed state and prior artifacts, but variant 1 should
  produce and use its own baseline row before measured runs.
- **Calibration provenance is part of comparison provenance.** The final summary
  should record which calibration id each variant used. If baseline performance
  differs meaningfully between refs, that is signal worth preserving rather than
  hiding behind a group-shared baseline.
- **The primary variant is the comparison baseline.** Initial reporting compares
  variant 1 against variant 0. Labels should say "baseline" and "candidate" (or
  the explicit refs) rather than declaring a winner.
- **Noise-aware verdicts stay conservative.** Sub-noise deltas are
  inconclusive. With repeats, classify against observed per-variant variance;
  without repeats, reuse the existing configured noise-floor discipline and mark
  unknown noise as provisional.
- **v21 is intentionally deferred.** v22 may continue using the compatibility
  parser; it should not block on replacing the result parser unless the
  implementation discovers a hard schema-v1 dependency.
- **Dynamic ref sets are future work.** Requests like "compare all release tags
  in the last 3 months" need a ref-expansion feature that resolves a glob/date
  filter into concrete refs, plus `0015` resource budgets for the resulting
  `variants × clean_repetitions` lifecycle count. v22 should not implement ref
  discovery, but it should avoid baking "exactly two" into internal models.
- **Carry and calibration predicates are related but distinct.** Today
  `uses_shared_calibration` and `job_should_carry_sqlite` are effectively
  per-spec repeat predicates. v22 should generalize DB carry-forward to the
  group sequence, while calibration remains per variant/spec. Update every Rust
  caller and the matching SQL/migration comments together so calibration steps,
  carried DB seeding, and workflow-step rows cannot diverge.

## Phases

### Phase 1: Request Model, Syntax, and Caps

**Goal:** Add a safe daemon request shape for explicit two-ref comparisons.

**Scope:**

- Introduce a comparison request model that wraps one validated workload and two
  variant refs. The workload must be the existing `WorkloadSpec` target model,
  covering txids, block selectors, and block ranges through one path.
- Add an explicit deterministic Slack syntax, for example
  `--rev <baseline> --compare-rev <candidate>`, while keeping existing single
  workload requests unchanged.
- Extend the LLM intent schema/provider parsing to emit the same bounded
  two-variant comparison request. The LLM response must not emit raw CLI flags or
  more variants than the daemon accepts.
- Add `[runner]` or `[slack]` caps for `max_variants` and total measured
  lifecycles (`variants × clean_repetitions`) with conservative defaults.
- Reject duplicate refs, missing refs, mixed workloads, over-cap requests, and
  ambiguous comparisons before enqueue.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests, if applicable
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] Existing single-ref Slack requests resolve exactly as before.
- [x] An explicit two-ref Slack request resolves to one comparison request with
  two ordered variants.
- [x] A natural-language comparison request resolves through the LLM into the
  same comparison request model.
- [x] Txid, block selector, and block-range comparison requests all flow through
  the same request model and validation path.
- [x] Requests exceeding variant or total-lifecycle caps are rejected before any
  job/group rows are created.

**Tests:**

- Workload/comparison parser tests for explicit two-ref syntax.
- LLM intent fixture/schema tests for two-ref comparison requests and invalid
  ambiguous comparisons.
- Deterministic and LLM fixtures for txid, block selector, and block-range
  comparison requests.
- Connector rejection tests for over-cap, duplicate-ref, and ambiguous requests.
- Config tests for the new caps and env overrides.

### Phase 2: Multi-Spec Group Planning

**Goal:** Create comparison groups atomically and make the run chain
DB-resumable across multiple specs.

**Scope:**

- Add store APIs to create one group from a `Vec` of spec requests and the first
  queued run. Phase 1 caps the vec at two variants, but the store API should not
  be a two-tuple.
- Keep `baseline_calibration_id` variant-scoped on `BenchmarkSpec`, and ensure
  the store can persist one calibration id per spec. Do not hoist it to
  `benchmark_group` in this iteration.
- Insert ordered workflow-step rows for both specs, with global `step_index`
  values that reflect the serial lifecycle.
- Extend append/resume logic so completion of the final run for spec K enqueues
  run 0 for spec K+1, and completion of the final spec terminates the group.
- Preserve the invariant: at most one queued/claimed/running job per comparison
  group, independent of daemon `max_concurrent_jobs`.
- Keep in-memory and Postgres stores behaviorally aligned.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests, if applicable
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] Creating a comparison group is atomic: failures leave no orphan specs,
  steps, or jobs.
- [x] The store creation API accepts an ordered spec collection and persists
  `spec_index` from that order, even though request validation currently caps it
  at two variants.
- [x] Each variant has its own explicit persisted calibration id and resume can
  derive the next action without guessing from spec 0.
- [x] Startup resume can continue a partially completed comparison group from
  persisted DB state.
- [x] Concurrent append/resume attempts cannot enqueue two active runs for the
  same group.

**Tests:**

- Postgres and in-memory store tests for two-spec group creation.
- Append/resume tests for spec0-final → spec1-run0 and final-spec terminal.
- Race/backstop tests for duplicate active-run prevention.

### Phase 3: Execution, Carry-Forward, and Per-Variant Calibration

**Goal:** Run both variants through the existing build/cache/libvirt path while
preserving one carried group DB and one calibration per variant.

**Scope:**

- Resolve/build/cache each variant independently.
- Carry the SQLite DB from every completed run into the next run, even when the
  next run belongs to a different spec.
- Fix the carry-seed gate: seed from the prior group DB for every run except the
  group's very first measured run. The old v15/v19 shape (`run_index > 0`) is
  insufficient for two variants with one repeat each, because variant 1's first
  run has `benchmark_run_index = 0` but must still receive the carried DB and
  calibration row.
- Generalize DB carry-forward from "spec has repeats" to "group has a next
  measured execution."
- Run calibration before each variant's first measured execution and persist the
  resulting `baseline_id` on that variant's spec.
- Fix the calibration trigger gate: calibrate a spec when its
  `baseline_calibration_id` is absent and the containing group has more than
  one total measured execution. The old v19 predicate
  (`requested_run_count > 1`) is insufficient for two variants with one repeat
  each, because each spec has `requested_run_count = 1` but still needs its own
  calibration.
- Update the descriptive workflow-step insertion in both Postgres and
  in-memory stores in lockstep with the runtime calibration trigger, so the
  inert `calibrate` step rows accurately reflect per-variant calibration for
  comparison groups.
- Reuse a variant's persisted `baseline_id` across clean repeats of that same
  variant.
- Fail closed if carry-forward or `--baseline-id` validation fails; do not
  silently fall back to inline per-run calibration.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] A two-variant/one-repeat group runs one calibration per variant, then one
  measured VM per variant.
- [ ] Variant 1 run 0 receives the carried DB from variant 0 rather than a fresh
  empty DB.
- [ ] A two-variant/N-repeat group runs each repeat in a fresh VM and carries the
  same group DB through every run.
- [ ] Repeats of the same variant reuse that variant's calibration id; different
  variants do not share a calibration id by default.
- [ ] A two-variant/one-repeat group calibrates both specs even though each spec
  has `requested_run_count = 1`.
- [ ] A carry-forward or baseline-id failure marks the group failed with visible
  partial data rather than continuing with a different DB/calibration.

**Tests:**

- Runner/driver tests for cross-spec carry-forward.
- Per-variant calibration tests for two variants with one repeat each.
- Failure-path tests for missing carried DB and rejected `--baseline-id`.

### Phase 4: Comparison Metrics and Verdicts

**Goal:** Compute a daemon-owned comparison summary from promoted run metrics.

**Scope:**

- Load `job_metric` rows by `benchmark_spec_id` and `benchmark_run_index`.
- Aggregate repeated runs per variant using Execution+Commit as the headline
  metric.
- Model the result as one baseline spec plus a `Vec` of variant deltas against
  that baseline. The first implementation will usually have one delta, but the
  type should not be a `{ baseline, candidate }` pair.
- Compare candidate variant(s) against spec 0 using the existing
  PR-vs-baseline delta semantics where applicable.
- Audit the existing comparability guard: two variants over the same workload
  should compare cleanly even though they are not PR-vs-baseline rows. If the
  guard rejects the comparison because it assumes the older baseline path, adapt
  it explicitly rather than weakening the mismatch checks.
- When repeats exist, use per-variant mean/stddev/CV to classify the delta.
- When repeats do not exist, use the configured noise floor or mark the verdict
  provisional.
- Treat metric absence as partial/incomplete data, not a panic.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] The summary reports baseline/candidate refs, Execution+Commit values,
  signed percentage delta, per-variant calibration provenance, and a
  conservative verdict.
- [ ] The comparison verdict model supports a baseline plus a collection of
  deltas; the two-ref case is represented as one element in that collection.
- [ ] A same-workload two-ref comparison is not incorrectly marked
  incomparable by the existing `measured_blocks`/`warmup_blocks` guard.
- [ ] Sub-noise deltas are rendered as inconclusive/provisional, not as wins.
- [ ] Partial groups render available completed-run data and clearly mark what is
  missing.

**Tests:**

- Pure comparison tests for single-run, repeated-run, sub-noise, and missing
  metric cases.
- Reporter tests for comparison summary row/table construction.

### Phase 5: Reporting Surface and Slack UX

**Goal:** Render comparison progress and results on one group-owned surface.

**Scope:**

- Show the active variant/ref and repeat position while the group runs.
- Avoid per-run or per-variant Slack card/comment/check fan-out.
- Render one final comparison summary with artifact links and refs.
- Keep current PR baseline reporting unchanged.
- Ensure progress from v20 remains scoped to the active run/variant and does not
  leak across specs.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] Slack shows one comparison card for the whole request.
- [ ] The final card identifies both refs and reports the delta summary.
- [ ] Existing PR-vs-baseline comments/checks continue to render as before.

**Tests:**

- Slack timeline/report-surface tests for active variant labels and final
  comparison blocks.
- Regression tests for existing PR-vs-baseline reporting.

### Phase 6: Host Smoke and Documentation

**Goal:** Validate the full operator workflow on the host.

**Scope:**

- Run one Slack comparison between two explicit refs over a small tx/block
  workload.
- Run one natural-language Slack comparison over the same class of workload.
- Confirm the group is serial, host-pinned, uses one shared DB, and calibrates
  each variant separately.
- Capture logs/artifacts needed to debug comparison failures.
- Update docs/help text for the supported comparison syntax and current
  limitations.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] Host smoke: Slack request comparing two refs completes and reports one
  comparison summary.
- [ ] Host smoke: natural-language comparison request reaches the same execution
  path as deterministic syntax.
- [ ] Host smoke: logs show no parallel variant execution, one calibration per
  variant, and no repeated inline calibration inside clean repeats.
- [ ] User-facing help/rejection text names the supported syntax and caps.

**Tests:**

- Host smoke checklist.
- Documentation/help snapshot tests where applicable.

## Final Validation

- [ ] `just build`
- [ ] `just lint`
- [ ] `just test`
- [ ] Slack smoke: two explicit refs over one tx/block workload produce one group
  card, one group DB, one calibration per variant, two measured variant runs,
  and one delta summary.
- [ ] Slack smoke: natural-language comparison request resolves through the LLM
  and produces the same bounded two-variant request.
- [ ] Regression smoke: existing single-ref Slack benchmark and PR-vs-baseline
  comparison paths still work.

## Follow-Ups

- Lift the two-variant cap only after `0015-resource-aware-admission` can budget
  full group lifecycles.
- Add ref-expansion for requests like "all release tags in the last 3 months"
  before accepting dynamic multi-ref comparison requests.
- `0028-results-summary-restructure` should make comparison summaries richer and
  easier to scan, especially for repeated variants.
