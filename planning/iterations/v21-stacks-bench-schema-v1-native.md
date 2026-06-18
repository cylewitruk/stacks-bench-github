# v21: Native `stacks-bench` Schema-v1 JSON

Retire the compatibility-first result parsing path and make the daemon's
`stacks-bench --json` integration speak schema-v1 natively (`0050`).

> **Status:** planned - scheduled after v19/v20 so the native parser covers the
> final workflow/result shapes, including calibration and progress artifacts.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0050-stacks-bench-schema-v1-native` | primary | planned |

## Why

The daemon currently accepts both the legacy `data` envelope and the schema-v1
`result` envelope so deployments can bridge the upstream transition. That was
the right compatibility layer, but it leaves two sources of drift:

- result parsing still treats legacy shapes as the conceptual center;
- UI and metric names still lean on block-centric fields even though schema-v1
  intentionally uses neutral `entries` terminology across range, txid, and block
  selector modes.

Once the integration branches all emit schema-v1, the daemon should dispatch on
the versioned contract directly.

## Scope

- Treat `(schema_version, result_type, result_version)` as the primary result
  dispatch key.
- Make `result_type = "run", result_version = 1` the canonical benchmark-run
  parser.
- Add native DTOs for schema-v1 run results, calibration results introduced by
  v19, and any other consumed result types.
- Prefer `entries`/`warmup_entries`/`measured_entries` naming in new Rust code
  and user-facing text where the workload is not literally a block-height range.
- Use `mode_summary` and `sampled_metric_rows` where they improve summaries and
  validation.
- Keep any legacy parser as an explicit historical fallback only, with tests and
  no silent ambiguity.

**Non-goals:** no database churn unless the storage-name decision requires it,
no new upstream schema design, and no portal work.

## Design Decisions

- **Schema-v1 is canonical for new runs.** Legacy parsing is for old archived
  artifacts, not for normal runtime.
- **Dispatch before deserialize.** The envelope decides which payload parser runs;
  payload inference by field presence should disappear.
- **Storage names need a deliberate decision.** Existing `job_metric` column names
  may remain storage legacy, but the choice should be explicit and documented.
- **Contract tests use upstream fixtures/schema.** The pinned upstream
  `contrib/stacks-bench/schema/v1.json` should be copied or referenced in tests
  so local DTOs cannot drift silently.
- **Final results fail closed; progress degrades.** Unlike v20 progress events,
  unknown final-result schema/type/version combinations are correctness errors
  because the daemon must parse results before persisting/reporting them.

## Phases

### Phase 1: Versioned Result Envelope

**Goal:** Create the schema-v1 result parsing spine.

**Scope:**

- Add a versioned top-level envelope parser.
- Dispatch by `(schema_version, result_type, result_version)`.
- Preserve explicit error-envelope handling.
- Add fixtures for `run`, `baseline_calibration`, and `error`.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] Unknown schema/result versions fail with clear diagnostics.
- [ ] Error envelopes surface the upstream error without being mistaken for a
  successful result.

**Tests:**

- Envelope parser fixtures for `run`, `baseline_calibration`, and `error`.
- Unknown-version/error-path unit tests.

### Phase 2: Native Run Payload and Metrics

**Goal:** Move benchmark result parsing to schema-v1 run payloads.

**Scope:**

- Parse `entries`, `warmup_entries`, `measured_entries`,
  `sampled_metric_rows`, and `mode_summary`.
- Promote metrics and aggregate summaries from the native payload.
- Decide whether persisted metric/summary names stay legacy or get migrated.
- Keep historical legacy-run fixtures if old artifacts must still render.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] Schema-v1 run payloads populate promoted metrics and clean-repeat
  aggregates.
- [ ] Legacy archived run JSON either still renders through an explicit fallback or
  has a documented migration/retention decision.

**Tests:**

- Run-payload parser fixtures covering range, txid, and block selector modes.
- Metric-promotion and clean-repeat aggregate tests.

### Phase 3: Reporting Language and Mode Summary

**Goal:** Align user-facing summaries with the neutral schema-v1 vocabulary.

**Scope:**

- Use "entries" for txid/block selector modes and other non-range workloads.
- Preserve useful block-height wording for range-mode runs.
- Surface `mode_summary` fields that clarify target mode, warmup/measured counts,
  isolation, ordering, and sample unit.
- Ensure Slack/GitHub summaries stay concise.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] Slack and GitHub summaries no longer imply every workload is a block range.
- [ ] Range-mode summaries still read naturally as block ranges.

**Tests:**

- Slack/GitHub rendering tests for range and selector-mode wording.

### Phase 4: Compatibility Retirement and Contract Guard

**Goal:** Make the compatibility boundary small and tested.

**Scope:**

- Remove compatibility shims that are no longer needed for runtime.
- Keep a narrow legacy fallback only if historical artifacts still require it.
- Add a schema/fixture drift guard using upstream
  `contrib/stacks-bench/schema/v1.json`.
- Document the expected upstream schema version in the daemon.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] Runtime parsing is schema-v1-first.
- [ ] Adding or renaming consumed schema-v1 fields requires updating tests.
- [ ] Compatibility behavior is explicit, not accidental.

**Tests:**

- Schema/fixture drift guard against upstream `v1.json`.
- Legacy fallback tests, if retained.

## Final Validation

- [ ] `just build`
- [ ] `just lint`
- [ ] `just test`
- [ ] Host smoke with a schema-v1 `stacks-bench` binary producing range and txid
  runs, plus a calibrated group from v19.

## Follow-Ups

- If storage naming remains legacy, record that as an explicit decision before
  new schema-v2 work starts.
