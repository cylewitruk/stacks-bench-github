# 0033: Slack streamed plan updates

- **id:** `0033-slack-streamed-plan-updates`
- **status:** `shipped`
- **date:** 2026-06
- **iteration:** v12 (`v12-slack-streamed-plan`)
- **depends_on:** `0023-slack-card-redesign`
- **relates_to:** `0024-slack-card-stage-timings`

Replaced whole-card Slack `chat.update` progress edits with Slack's streaming
chat APIs. The live Slack card now updates as a semantic plan/task stream
instead of repeatedly replacing the message and collapsing the opened `plan`.

## What shipped

- **Stream Web API client** — the existing reqwest-backed Slack client gained
  typed calls for `chat.startStream`, `chat.appendStream`, and
  `chat.stopStream`.
- **Stable task updates** — the Slack timeline maps Job / Build / Run /
  Finalize to stable task ids and appends `task_update` chunks for state
  changes.
- **Queue integration** — pre-claim queued cards start as streams and queue
  position updates append to the same streamed plan.
- **Fallback path** — stream failures are non-fatal and fall back to the v8
  Block Kit card/update path for that job.
- **Terminal surface** — completion still renders the rich result table and
  download button through `chat.stopStream` blocks.

## Validation

- Unit tests pinned the exact stream request JSON and fallback behavior.
- Live Slack validation (2026-06): a real `@BenchBot bench …` run updated the
  expanded plan in place without the plan collapsing or redrawing wholesale.

## Decisions

1. **Own the outbound stream JSON.** `slack-morphism` remains the inbound Socket
   Mode transport; the outbound stream methods live in our narrow Web API
   client.
2. **Stream first, block fallback.** Slack reporting remains non-fatal. If a
   stream is unavailable, the job continues and the card degrades to the block
   path.
3. **Task updates are compact.** Repeated updates avoid mutable `details` /
   `output` fields that Slack appends visually; human status text goes through
   appended log-style stream text.

## Follow-Ups

- Further card polish is expected after more live use.
- `0027-fine-grained-progress` can feed richer counters into the same stream
  path once `stacks-bench` emits structured progress.
