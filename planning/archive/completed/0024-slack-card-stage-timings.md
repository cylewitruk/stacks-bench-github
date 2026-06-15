# 0024: Slack card stage timings

- **id:** `0024-slack-card-stage-timings`
- **status:** `shipped`
- **date:** 2026-06
- **iteration:** v12 (`v12-slack-streamed-plan`)
- **depends_on:** `0023-slack-card-redesign`
- **relates_to:** `0033-slack-streamed-plan-updates`, `0027-fine-grained-progress`

Added best-effort stage timing to the Slack live plan now that streaming updates
avoid the `chat.update` collapse problem that made heartbeat-driven progress
unpleasant.

## What shipped

- **Observed stage timing** — the timeline tracks stage starts and observed
  transitions for Job / Build / Run / Finalize.
- **Live elapsed text** — active-stage elapsed text is sent through the stream
  path with debounce rather than whole-card edits.
- **Terminal summaries** — completed rows can report short duration summaries for
  stage boundaries the daemon observed.
- **Best-effort reclaim behavior** — timing state is in-memory for this slice;
  after daemon reclaim, the durable stream/card identity survives but prior
  in-memory durations may be absent.

## Validation

- Unit tests covered debounce and stream update behavior.
- Live Slack validation (2026-06): streamed timing/status updates worked without
  collapsing the expanded plan.

## Decisions

1. **Best-effort is enough for v12.** Persisted per-stage timing can be revisited
   if the UI needs exact durations across daemon restart.
2. **Fine-grained counters stay separate.** Per-block/per-phase progress belongs
   to `0027`, where the upstream benchmark process can emit structured progress.

## Follow-Ups

- More precise persisted timings can be added later if live use shows the
  best-effort behavior is too lossy.
- `0027-fine-grained-progress` remains the path for indexing/warmup/measurement
  counters.
