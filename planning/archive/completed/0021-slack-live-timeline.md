# 0021: Slack live timeline

- **id:** `0021-slack-live-timeline`
- **status:** `shipped`
- **date:** 2026-06
- **iteration:** v6 (deployed + live-verified on the Hetzner host)
- **predecessor:** v5 Slack surface ([0002](0002-slack-adhoc-profiling.md))

Turned the v5 terminal result card into a **live `plan` timeline**: a Block Kit
`plan` card posted when the job starts running and `chat.update`d through
**Build → Benchmark → Archive** as the worker emits phases, finalized with the
metrics + the presence-gated `stacks-bench.db` link and a ⏳→✅/❌ reaction swap.
Resumes the **same** card across a daemon restart.

## What shipped

### Persistence (resume across restart)

- A `plan_message_sent` `JobEventKind` (+ `ALTER TYPE … ADD VALUE` migration);
  the Slack message `ts` rides `job_event.detail` JSONB — no new column.
- `RunnableJobStore::set_plan_message_ts` + `JobStore::latest_plan_message_ts`
  (Postgres + in-memory), mirroring how the PR comment / Check Run identity is
  carried. `ProgressTarget::Slack` gained `plan_message_ts`, read back at claim
  time so a reclaimed job resumes the same card instead of reposting.

### The live card

- `SlackClient::post_blocks_in_thread` now returns the message `ts`; new
  `update_blocks` (`chat.update`). `bench_summary::render_plan_blocks` renders
  explicit per-row statuses + outputs (a `PlanTaskStatus` enum).
- `SlackTimeline` (one per job, `Mutex<{plan_ts, stage}>`): `started()`
  posts/resumes the card (persisting its `ts`); `advance(stage)` monotonically
  `chat.update`s the three rows; `completed/failed/cancelled` finalize + swap the
  reaction. `stage_for_phase` maps `starting/building → build_done/running →
  collecting` onto the rows.
- Reporter wiring: the `Reporter` builds the timeline once and hands it to the
  `ProgressReporter` (Slack lifecycle branches) and the drain `ProgressSink`
  (per-phase advance); orphan recovery reconstructs + resumes it.

### Slack-connector hardening (committed alongside)

- A custom Socket Mode **listener error handler** downgrades routine WSS recycles
  (`ResetWithoutClosingHandshake`, matched by the `"Slack WSS error:"` message
  prefix) from slack-morphism's default `error` to `debug`, leaving genuine
  anomalies (e.g. unexpected binary frames) + unexpected errors at `warn`. The
  library auto-reconnects regardless; this only corrects the inverted log
  severity.

## Validation

- Unit + reporter-wiring tests pin post-once → advance → finalize → reaction
  swap, and resume-without-reposting.
- **Live:** an `@BenchBot bench …` run animated the card Build → Benchmark →
  Archive in-thread and finalized ✅ with the metrics + the DB download link.

## Follow-ons

- Reporting-surface refactor — collapse `ProgressReporter` + `ProgressSink` into
  one `ReportSurface` trait — `0022-report-surface-trait` shipped (iteration v7)
  → [archive/completed/0022-report-surface-trait.md](0022-report-surface-trait.md).
- Pre-`running` queue-position state on the card (`0014`/`0017`); Slack Canvas as
  a durable "recent benchmarks" history. Natural-language intent — `0020`.
