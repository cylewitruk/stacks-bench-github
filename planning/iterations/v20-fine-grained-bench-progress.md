# v20: Fine-Grained Bench Progress

Consume upstream `stacks-bench --json` stderr JSONL progress events and feed
them into report surfaces as live sub-phase progress (`0027`).

> **Status:** parked - Phase 2 was implemented and locally validated. The
> remaining reporting work will be revisited after v24 establishes the new
> application and execution boundaries.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0027-fine-grained-progress` | primary | parked |

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

## Sources

Verify the upstream progress contract against the pinned `cylewitruk/stacks-core`
branch `feat/stacks-bench`:

- `contrib/stacks-bench/schema/v1.json` — `cli_events.progress` is the versioned
  event envelope (`schema_version`, `event_type`, `event_version`, and the
  `progress.{phase,progress,total,message}` payload). Treat it as the source of
  truth for the parser DTOs, not the inline example above.

**Open question to resolve before Phase 4:** confirm whether `bench baseline
calibrate` emits `(1, "progress", 1)` events or only `bench run` does. Phase 4
assumes calibration progress exists under the `calibrate` step; if calibrate is
silent, that becomes a no-op fallback rather than a tested live path, and the
host-smoke acceptance should be reworded accordingly.

Also observe total-less progress phases during the host smoke. The Slack
coalescer buckets total-less counters by fixed raw-count intervals; if upstream
emits high-magnitude total-less counters in practice, switch that path to a
scaling bucket or require totals for long-running phases.

## Scope

- Capture benchmark stderr separately from the serial console by redirecting it
  in the guest script to a dedicated, workflow-step-scoped progress file, for
  example `$RESULTS/calibration.progress.jsonl` and `$RESULTS/run.progress.jsonl`.
  Preserve the serial-console stderr stream for forensics; the dedicated file is
  the machine-readable source, not a replacement for `console.log`.
- Tail that file live during benchmark execution and archive it beside the
  command result envelope (`calibration.json` / `run.json`) and `console.log`.
- Parse progress JSONL defensively: per-line parse, ignore non-JSON, ignore
  unknown event types/versions, and tolerate additive fields.
- Convert recognized progress into daemon worker/reporting events tagged to the
  current workflow step and run.
- Debounce report-surface updates.

**Non-goals:** no upstream protocol changes, no result-envelope cleanup (v21),
no portal UI, and no attempt to reconstruct progress from human console logs.

## Design Decisions

- **Progress is optional.** A missing or empty step-scoped progress JSONL file is
  not an error.
- **No stdout/stderr merging.** stdout remains the final `run.json` source;
  stderr JSONL is progress only. The guest may tee stderr to both the progress
  file and the serial console so human failure output remains visible, but the
  daemon never parses progress from `console.log`.
- **Progress is workflow-step aware.** Calibration progress from v19 and measured
  run progress should flow through the same event path with different step/run
  context. The first implementation may render both under the existing Slack
  "Run" task row, but the event model must distinguish `calibrate` from `run`
  so later card/result restructures do not need a parser rewrite.
- **Progress is best-effort.** Phase transitions and terminal outcomes remain
  reliable; JSONL progress is droppable like heartbeats. Parser/tailer/reporting
  errors are logged and ignored unless the underlying `stacks-bench` command
  itself exits non-zero. If progress uses the existing bounded worker-event
  channel, it must be `try_send`-and-drop (or an equivalent separate bounded
  channel) so progress cannot occupy capacity needed by reliable phase/terminal
  delivery.
- **Slack renders append-shaped progress.** Streamed Slack plan updates are
  append-friendly, not mutable progress bars. Coalesce progress into readable
  details such as phase headings plus milestone ticks, rather than posting every
  single event verbatim. The coalescer must send only newly reached milestones
  on the stream path to avoid duplicating the same details on every update; block
  fallback can render the latest compact snapshot.
- **Other surfaces degrade simply.** GitHub/check surfaces can show throttled
  latest-progress text without trying to mirror Slack's append transcript.

## Phases

### Phase 1: Capture, Tail, and Archive Progress JSONL

**Goal:** Separate machine progress from the serial console and make it
available while the VM is running.

**Scope:**

- Update guest templates so `stacks-bench` stderr goes to a progress JSONL file.
- Keep stdout redirected to the final result envelope.
- Add a host-side tailer that tracks file offsets and survives file-not-yet-
  created startup gaps.
- Start the tailer before the relevant VM command can write progress; stop it on
  phase terminal/cancel and drain any final bytes before archiving.
- Run tailers at the `run_phase` boundary: v19 run 0 has one calibration VM
  phase and one measured-benchmark VM phase, so the natural shape is one
  tailer/file per phase rather than a single demuxing task.
- Archive raw progress JSONL for forensics.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests, if applicable
- [x] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] Updated builds produce a live progress file during a run.
- [x] Older builds with no JSONL behave like today's coarse phase reporting
  (missing progress file is optional and archives as absent).
- [x] Human stderr remains available in `console.log` for failure forensics
  (`tee` preserves stderr while writing the progress file).
- [ ] Host smoke confirms the final progress line survives VM poweroff, or logs
  the observed loss as accepted best-effort behavior.

**Tests:**

- Guest-template tests for stdout/stderr separation.
- `libvirt::progress` unit tests for missing files, incremental reads,
  partial-line final drain, and truncation.
- Driver archive test for `run.progress.jsonl`.

**Notes:** Phase 1 intentionally logs raw progress lines at debug only. Parser
DTOs and report-surface events start in Phase 2.

Known limitation: guest scripts use bash process substitution to tee stderr into
the progress file while preserving `console.log`. The script does not wait on
the `tee` child before `sync`/`phase "done"`/poweroff, so the final progress
line can be lost. That is acceptable under v20's best-effort progress policy;
host smoke should confirm whether it matters in practice before we add FIFO/PID
tracking.

### Phase 2: Parser and Worker Events

**Goal:** Turn raw JSONL lines into typed daemon progress events.

**Scope:**

- Add DTOs for schema-v1 progress events.
- Dispatch by `(schema_version, event_type, event_version)`.
- Ignore unknown or malformed lines without failing the benchmark.
- Add a recipe-neutral worker/reporting event carrying phase, progress, optional
  total, optional message, run index, and workflow step.
- Deliver progress over the existing worker-event channel as best-effort,
  bounded traffic; progress backpressure must not block the VM poll loop.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests, if applicable
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] Parser tests cover valid progress, unknown event/version, additive fields,
  missing optional fields, and malformed lines.
- [x] Driver tests prove progress events are emitted without affecting terminal
  success/failure handling.
- [x] Backpressure or parse failures drop progress but do not drop reliable phase
  or terminal events.

**Tests:**

- Parser fixtures for schema-v1 progress events.
- Driver/worker-event tests for recognized and ignored progress lines.

### Phase 3: Report Surface Rendering

**Goal:** Render progress in Slack and other surfaces without excessive API
churn.

**Scope:**

- Add a `ReportSurface` progress hook or equivalent event path.
- Debounce/coalesce updates by time and/or traversed entries.
- For Slack, append compact details to the active task card: phase headings,
  milestone counts, and human messages where useful.
- For GitHub/checks, update a throttled latest-progress summary.
- Keep the Slack coalescer per run/workflow step so repeat run *K+1* cannot
  inherit run *K*'s progress transcript.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] A long block-range run shows live indexing/warmup/replay progress in Slack.
- [x] Slack updates remain bounded and readable; no event-per-line spam.
- [x] A run with no JSONL still shows the existing coarse phase card.

**Tests:**

- Slack card/timeline tests for coalesced progress details.
- GitHub/check-surface tests for throttled latest-progress updates.

### Phase 4: Calibration and Repeat Validation

**Goal:** Validate progress across the v19/v15 workflow shape.

**Scope:**

- Confirm calibration progress appears under the calibration workflow step.
- Confirm repeat groups show progress for the active repeat only, on the single
  group surface.
- Confirm progress tailing stops at terminal and archived JSONL is retained.
- Confirm the v19 calibration command and the measured `bench run` command write
  distinct progress files when both execute in run 0's libvirt lifecycle.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests, if applicable
- [ ] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [ ] Host smoke: a calibrated two-repeat group displays calibration progress, then
  measured-run progress, without duplicate cards or per-repeat fan-out.
- [ ] Host smoke confirms total-less progress phases are low-volume enough for
  the fixed raw-count bucket, or records a follow-up to scale that path.

**Tests:**

- End-to-end host smoke with updated `stacks-bench` emitting JSONL.

## Final Validation

- [ ] `just build`
- [ ] `just lint`
- [ ] `just test`
- [ ] Host smoke with an updated `stacks-bench` binary emitting JSONL.

## Follow-Ups

- `0050-stacks-bench-schema-v1-native` should consolidate schema-v1 DTOs once
  progress and result shapes are both exercised.
- Portal-style progress history remains out of scope for this iteration.
