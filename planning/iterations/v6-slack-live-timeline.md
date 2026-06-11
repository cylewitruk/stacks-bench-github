# v6: Slack live timeline

Successor to [v5-slack-adhoc-profiling](../archive/completed/0002-slack-adhoc-profiling.md).
Turn the Slack result card from a **terminal snapshot** into a **live timeline**:
a `plan` block posted when the job starts running and `chat.update`d through
Build → Benchmark → Archive as the run progresses, finalized ✅/❌ at terminal —
surviving a daemon restart by resuming the same card.

*(Iteration `vN` continues the deployment-version lineage; last deployed was v5.
The canonical item identity is `0021-slack-live-timeline`.)*

> **Status:** in_progress
>
> Retroactive — built directly on the shipped v5 Slack surface (`0002`), in two
> slices (both green: 751 workspace tests, lint clean). Remaining before
> `shipped`: Codex review, commit, deploy, and a live `@mention` smoke test
> confirming the card animates in-thread.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0021-slack-live-timeline` | primary | in_progress |

Builds on the v5 plan-block card (`slack-messaging`) and the `ProgressTarget::Slack`
reporting surface (`0002`, shipped).

## Why

The v5 card only appears at the **end** — a long stacks-core build + replay shows
nothing but a static ⏳ for the whole run, and a queued job looks identical to a
running one. A live `plan` timeline tells the user *where* the run is, using
Slack's native task-card status pills (`pending`/`in_progress`/`complete`/
`error`), which map one-to-one onto the benchmark's stages.

## Scope

The card's three rows — **Build → Benchmark → Archive** — advance their status
as the worker emits phase events, the metrics + DB link land on completion, and
the request message's ⏳ swaps to ✅/❌. The posted message `ts` is persisted so
a reclaimed job (daemon restart) resumes the **same** card instead of posting a
duplicate.

## What shipped (the two slices)

### Slice A — persistence foundation

- A `plan_message_sent` `JobEventKind` (+ migration `ALTER TYPE job_event_kind
  ADD VALUE`); the Slack message `ts` (a string) rides the `job_event.detail`
  JSONB — no new column.
- `RunnableJobStore::set_plan_message_ts` + `JobStore::latest_plan_message_ts`
  (Postgres + in-memory), mirroring how `comment_posted` / `check_run_created`
  carry the PR comment / Check Run identity.
- `ProgressTarget::Slack` gained `plan_message_ts: Option<String>`, populated at
  claim time via the read-back. Round-trip tested against real Postgres.

### Slice B — the live timeline

- `SlackClient`: `post_blocks_in_thread` now **returns the message `ts`**, plus a
  new `update_blocks` (`chat.update`). `bench_summary::render_plan_blocks`
  generalized to explicit per-row statuses + outputs (a `PlanTaskStatus` enum).
- `SlackTimeline` (one per Slack job, `Mutex<{plan_ts, stage}>`): `started()`
  posts/resumes the card (persisting its `ts`); `advance(stage)` `chat.update`s
  through the stages (monotonic — heartbeats/out-of-order no-op);
  `completed`/`failed`/`cancelled` finalize + swap the reaction. `stage_for_phase`
  maps the worker's `starting/building → build_done/running → collecting` phases
  onto the three rows.
- Reporter wiring: the `Reporter` creates the timeline once, hands it to the
  `ProgressReporter` (which delegates its Slack branches) and the drain
  `ProgressSink` (per-phase advance). Orphan recovery reconstructs it (resumes →
  cancelled).

## Acceptance

- [x] A run posts the card at start and `chat.update`s it through the stages,
      finalizing ✅/❌ (unit + reporter wiring tests).
- [x] A reclaimed job resumes the persisted card without reposting
      (`resume_updates_the_existing_card_without_reposting`).
- [ ] Live: `@mention bench …` shows the card animate `Build → Benchmark →
      Archive` in-thread, then ✅.

## Follow-ons (not in scope)

- Pre-`running` queue-position state on the card (the queue surface today is
  GitHub-Check-only — see `0014`/`0017`).
- Slack Canvas as a durable "recent benchmarks" history (distinct from per-run
  cards).
