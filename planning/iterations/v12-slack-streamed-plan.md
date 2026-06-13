# v12: Slack streamed plan updates

Successor to [v8 (`0023`)](../archive/completed/0023-slack-card-redesign.md)
and [v7 (`0022`)](../archive/completed/0022-report-surface-trait.md). Replace
whole-card `chat.update` edits with Slack's native streaming plan/task updates,
so the Slack card behaves like a live plan instead of a repeatedly replaced
Block Kit message.

> **Status:** planned — Phase 0 design drafted for Codex review. No code has
> landed. The live workspace symptom is known: expanding the `plan` card is lost
> on every update because Slack re-renders the whole card.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0033-slack-streamed-plan-updates` | primary | planned |
| `0024-slack-card-stage-timings` | bundled follow-up | planned |

## Why

The v8/v11 Slack card is visually good, but mechanically wrong for the UX we
want. `SlackTimeline` and queue-position updates post a `plan` block and then
replace the entire message through `chat.update`. Slack therefore re-renders the
whole plan and collapses it if the user had opened it.

Slack's current docs expose a more precise primitive:

- [`chat.startStream`](https://docs.slack.dev/reference/methods/chat.startStream/)
  starts a streamed reply under a user request (`thread_ts`) and supports
  `task_display_mode=plan`.
- [`chat.appendStream`](https://docs.slack.dev/reference/methods/chat.appendStream/)
  appends stream chunks, including `task_update`, `plan_update`, and `blocks`.
- [`chat.stopStream`](https://docs.slack.dev/reference/methods/chat.stopStream/)
  finalizes the stream and can render bottom `blocks`.

The `task_update` chunk model matches our four stable rows (`job`, `build`,
`run`, `finalize`) directly. This should preserve the user's expanded/collapsed
state because we update semantic tasks instead of replacing the message. The
final live smoke test must prove that in Slack; docs imply the direction but do
not promise local UI-state preservation.

## Item: `0033-slack-streamed-plan-updates`

- **id:** `0033-slack-streamed-plan-updates`
- **status:** `planned`
- **priority:** `high`
- **depends_on:** `0023-slack-card-redesign`
- **relates_to:** `0024-slack-card-stage-timings`, `0027-fine-grained-progress`
- **source:** Slack UX regression observed after v8/v11 deployment

**Problem:** Slack plan messages are currently replaced wholesale with
`chat.update`; expanded plans collapse on every queue/progress update.

**Scope:** Replace the Slack timeline transport with `chat.*Stream`: start one
threaded stream under the user's request, update stable task IDs with
`task_update` chunks, update the title with `plan_update` when needed, and
finalize with `chat.stopStream` plus result blocks. Keep GitHub reporting
unchanged. Keep the existing `plan_message_ts` persistence as the stream message
identity.

**Acceptance:** A live `@BenchBot bench …` posts one threaded streamed plan; if a
user expands it while queued/running, queue and phase updates do not collapse the
plan; terminal success still shows the results table and download button; a
daemon restart resumes or safely falls back without duplicating the thread.

## Item: `0024-slack-card-stage-timings`

- **id:** `0024-slack-card-stage-timings`
- **status:** `planned`
- **priority:** `medium`
- **depends_on:** `0023-slack-card-redesign`
- **source:** v8 timing follow-up; bundled here because streaming removes the
  `chat.update` spam/collapse problem

**Problem:** The card has staged rows but no live elapsed time or completed
duration per row.

**Scope:** Add per-row start/end timing to the persisted timeline state and
render short live elapsed/details through `task_update` chunks. Terminal rows get
compact outputs ("Built in …", "Ran in …", "Finished in …"). Fine-grained bench
counters from `0027` stay out of scope; v12 only times the stages we already
observe.

**Acceptance:** A live run shows a ticking active-row elapsed value and terminal
stage durations, without re-rendering/collapsing the whole plan.

## Current Code Seams

- `SlackClient` currently exposes `post_blocks_in_thread` and `update_blocks`.
  The real impl (`slack/api_client.rs`) hand-posts JSON to Slack Web API methods
  through `reqwest`.
- `SlackTimeline::upsert` posts or `chat.update`s the whole `blocks` payload.
- `Runner::update_slack_queue_position` also `chat.update`s the queued card.
- `slack-morphism` owns inbound Socket Mode only. The outbound Web API surface
  is intentionally ours.

## SDK / Library Decision

Use a small typed stream layer in our existing `WebApiClient` first:

- Add request/response structs for `chat.startStream`, `chat.appendStream`, and
  `chat.stopStream`.
- Add local `StreamChunk` types for `markdown_text`, `task_update`,
  `plan_update`, and `blocks`.
- Keep `slack-morphism` as the inbound Socket Mode transport. Do **not** vendor
  or patch it just for outbound Web API types.
- Do **not** add a new Slack SDK unless Phase 1 finds a maintained Rust crate
  with complete `chat.*Stream` support and no regression for our Socket Mode
  boundary.

Rationale: local crate inspection of the pinned `slack-morphism 2.22.0` and
`slack-messaging 0.7.4` sources shows no `chat.startStream` / `appendStream` /
`stopStream` or `task_update` / `plan_update` support. Our current outbound
client is already a narrow `reqwest` wrapper, so extending it is lower risk than
forking an inbound transport library.

## Stream Model

Stable tasks:

| ID | Row | Source |
| --- | --- | ------ |
| `job` | Job | queued position, claim/start, supersede/cancel |
| `build` | Build | build/cache phases |
| `run` | Run | benchmark phase |
| `finalize` | Finalize | artifact/report publish + terminal |

Rules:

- `chat.startStream` runs when the connector creates the pre-claim card. It
  persists the returned `ts` via the existing plan-message event.
- Start streams with `task_display_mode=plan` and `thread_ts` equal to the user's
  request `ts`. For channel streams, include `recipient_user_id` and
  `recipient_team_id` from the mention event.
- Queue updates send a `task_update` for `job` only.
- Claimed/running updates send `task_update`s for changed rows only, debounced.
- Completion sends final `task_update`s, then `chat.stopStream` with bottom
  `blocks` for the markdown results table + download button.
- Failure/cancellation sends an error `task_update` and stops the stream without
  result blocks.
- `task_update` / `plan_update` fields must stay under Slack's 256-character
  chunk limit; detailed metrics stay in final blocks.
- `chat.appendStream` documents `markdown_text` as required even when `chunks`
  are present. Phase 1 must verify the least-noisy value (empty string vs. a
  short invisible/no-op string) against Slack before the timeline cutover.

## Failure / Fallback Rules

All Slack calls remain non-fatal to the benchmark.

- If `startStream` fails at enqueue, fall back to the current `postMessage` plan
  path for that job and log a warning.
- If `appendStream` fails with a non-streaming/stopped-message error, fall back
  to one whole-card update or a replacement in-thread card, then keep going.
- If `stopStream` fails, log and best-effort post the terminal blocks in-thread.
- Persisted `plan_message_ts` remains the identity; no schema change unless a
  live test proves Slack needs a separate "stream state" marker.

## Phases

### Phase 1: Stream API Client

**Goal:** typed Slack stream Web API methods behind the existing trait, tested
without a live workspace.

**Scope:**

- Extend `SlackClient` with `start_plan_stream`, `append_stream`, and
  `stop_stream`.
- Add `StreamChunk` / `TaskUpdate` / `PlanUpdate` / `StreamBlocks` structs.
- Interpret Slack envelopes + known stream errors.
- Keep old `post_blocks_in_thread` / `update_blocks` methods for fallback and
  non-timeline callers during the transition.

**Acceptance & Validation:**

- [ ] Unit tests assert exact JSON shape for start/append/stop.
- [ ] Response interpretation handles missing `ts`, `invalid_chunks`, and
  non-streaming/stopped-message errors.
- [ ] No new Slack scopes beyond `chat:write`.

### Phase 2: Streamed Timeline Cutover

**Goal:** `SlackTimeline` and queued-position updates mutate stream tasks, not
whole blocks.

**Scope:**

- Replace `SlackTimeline::upsert` with stream start/append/stop operations.
- Queue-position updater sends a `job` `task_update` instead of `chat.update`.
- Reuse `slack/card.rs`'s stage model, but render stream chunks rather than a
  full `plan` block for live updates.
- Preserve restart behavior through `plan_message_ts`.
- Keep fallback to whole-card update behind the same non-fatal contract.

**Acceptance & Validation:**

- [ ] Existing fake-client timeline tests port to stream calls.
- [ ] One stream `ts` is persisted pre-claim and resumed on claim.
- [ ] Queue → Build → Run → Finalize sends task deltas, not whole-card updates.
- [ ] Terminal success uses `stopStream` with result blocks.

### Phase 3: Stage Timings (`0024`)

**Goal:** live elapsed + completed durations on the four rows.

**Scope:**

- Persist per-stage start/end timestamps in job events or equivalent durable
  state.
- Add debounced heartbeat updates to the active task's `details`.
- Finalize each completed task with a short `output` duration.
- Keep `0027` fine-grained counters out of scope.

**Acceptance & Validation:**

- [ ] Active row shows live elapsed without frequent whole-message edits.
- [ ] Terminal rows show durations after daemon resume.
- [ ] Update debounce prevents noisy Slack API calls.

### Phase 4: Live Slack Verification

**Goal:** prove Slack UI behavior, because CI cannot assert client expansion
state.

**Acceptance & Validation:**

- [ ] In a real Slack workspace, expand the plan while queued/running.
- [ ] Queue/phase/heartbeat updates do **not** collapse the plan.
- [ ] Completion preserves the result table + download button.
- [ ] Restart during a running job resumes or falls back without duplicate spam.

## Final Validation

Run a live `@BenchBot bench …` on the deployed daemon. The streamed plan appears
immediately under the request, updates Job/Build/Run/Finalize in place without
collapsing when expanded, shows stage timings, and finalizes with the same rich
results/download surface v8 shipped.

## Follow-Ups / Relationships

- `0027-fine-grained-progress` remains separate. Once upstream
  `stacks-bench` emits JSONL progress, those counters can feed the same
  `task_update` path.
- `0023` remains the visual/card model ancestor; v12 changes transport and
  timings, not the overall row vocabulary.
- If Slack stream APIs prove unsuitable in live testing, archive this iteration
  as rejected/superseded and retain the fallback `chat.update` path.
