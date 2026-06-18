# v20: Fine-Grained Bench Progress

Consume upstream `stacks-bench --json` stderr JSONL progress events and feed
them into report surfaces as live sub-phase progress (`0027`).

> **Status:** planned - scoped after v19 because progress should observe the
> post-calibration workflow shape rather than being retrofitted twice.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0027-fine-grained-progress` | primary | planned |

## Why

Today the daemon sees coarse VM phases and the final `run.json`. It cannot show
where a long benchmark is inside indexing, baseline calibration, warmup, replay,
metrics, or cleanup.

Upstream now defines the long-lived command stream shape:

- stdout: one final versioned `CommandResult` envelope.
- stderr: newline-delimited JSON progress events during execution.

Progress events currently dispatch as `(schema_version, event_type,
event_version) = (1, "progress", 1)` with payload:

```json
{
  "phase": "replay",
  "progress": 42,
  "total": 100,
  "message": "Replaying measured entries"
}
```

Older builds may emit no JSONL or may mix ordinary stderr lines. That must behave
exactly like today: coarse phase reporting continues and malformed lines do not
fail the job.

## Scope

- Capture benchmark stderr separately from the serial console by redirecting it
  in the guest script to a dedicated progress file, for example
  `$RESULTS/progress.jsonl`.
- Tail that file live during benchmark execution and archive it beside
  `run.json` and `console.log`.
- Parse progress JSONL defensively: per-line parse, ignore non-JSON, ignore
  unknown event types/versions, and tolerate additive fields.
- Convert recognized progress into daemon worker/reporting events tagged to the
  current workflow step and run.
- Debounce report-surface updates.

**Non-goals:** no upstream protocol changes, no result-envelope cleanup (v21),
no portal UI, and no attempt to reconstruct progress from human console logs.

## Design Decisions

- **Progress is optional.** Missing `progress.jsonl` or an empty file is not an
  error.
- **No stdout/stderr merging.** stdout remains the final `run.json` source;
  stderr JSONL is progress only.
- **Progress is workflow-step aware.** Calibration progress from v19 and measured
  run progress should flow through the same event path with different step/run
  context.
- **Slack renders append-shaped progress.** Streamed Slack plan updates are
  append-friendly, not mutable progress bars. Coalesce progress into readable
  details such as phase headings plus milestone ticks, rather than posting every
  single event verbatim.
- **Other surfaces degrade simply.** GitHub/check surfaces can show throttled
  latest-progress text without trying to mirror Slack's append transcript.

## Phase 1 - Capture, Tail, and Archive Progress JSONL

**Status:** planned

Separate machine progress from the serial console and make it available while
the VM is running.

**Scope:**

- Update guest templates so `stacks-bench` stderr goes to a progress JSONL file.
- Keep stdout redirected to the final result envelope.
- Add a host-side tailer that tracks file offsets and survives file-not-yet-
  created startup gaps.
- Archive raw progress JSONL for forensics.

**Acceptance:**

- Updated builds produce a live progress file during a run.
- Older builds with no JSONL behave like today's coarse phase reporting.

## Phase 2 - Parser and Worker Events

**Status:** planned

Turn raw JSONL lines into typed daemon progress events.

**Scope:**

- Add DTOs for schema-v1 progress events.
- Dispatch by `(schema_version, event_type, event_version)`.
- Ignore unknown or malformed lines without failing the benchmark.
- Add a recipe-neutral worker/reporting event carrying phase, progress, optional
  total, optional message, run index, and workflow step.

**Acceptance:**

- Parser tests cover valid progress, unknown event/version, additive fields,
  missing optional fields, and malformed lines.
- Driver tests prove progress events are emitted without affecting terminal
  success/failure handling.

## Phase 3 - Report Surface Rendering

**Status:** planned

Render progress in Slack and other surfaces without excessive API churn.

**Scope:**

- Add a `ReportSurface` progress hook or equivalent event path.
- Debounce/coalesce updates by time and/or traversed entries.
- For Slack, append compact details to the active task card: phase headings,
  milestone counts, and human messages where useful.
- For GitHub/checks, update a throttled latest-progress summary.

**Acceptance:**

- A long block-range run shows live indexing/warmup/replay progress in Slack.
- Slack updates remain bounded and readable; no event-per-line spam.
- A run with no JSONL still shows the existing coarse phase card.

## Phase 4 - Calibration and Repeat Validation

**Status:** planned

Validate progress across the v19/v15 workflow shape.

**Scope:**

- Confirm calibration progress appears under the calibration workflow step.
- Confirm repeat groups show progress for the active repeat only, on the single
  group surface.
- Confirm progress tailing stops at terminal and archived JSONL is retained.

**Acceptance:**

- Host smoke: a calibrated two-repeat group displays calibration progress, then
  measured-run progress, without duplicate cards or per-repeat fan-out.

## Validation

- `just build`
- `just lint`
- `just test`
- Host smoke with an updated `stacks-bench` binary emitting JSONL.
