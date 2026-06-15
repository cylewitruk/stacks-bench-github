# 0023: Slack card redesign (live queue + rich results)

- **id:** `0023-slack-card-redesign`
- **status:** `shipped`
- **date:** 2026-06
- **iteration:** v8
- **predecessor:** v7 reporting-surface trait ([0022](0022-report-surface-trait.md))

Refined the Slack benchmark card into a **richer live timeline**: a 4-row `plan`
posted **at enqueue** (live queue position), tense-progressing each row
(future → present → past) with italic "what's happening" details that resolve
into output summaries, and a **redesigned completion** view — the timeline plan
above a rich-markdown results table and a primary "Download Profiler Data" button.

> **Status:** shipped — all phases (1 render, 2 timeline, 3 live-queue slices
> A/B1/B2/C) implemented, Codex-signed-off, and **live-verified** end-to-end: a
> queued card animated through Build → Run → Finalize to the results table + a
> green download button on a real `@BenchBot` run. Per-row timing was deferred to
> [`0024`](../../backlog.md). The spec below is retained as the validation recipe.

## What shipped

- **Card model (`slack/card.rs`)** — owns the 4-stage model (`STAGES = 4`,
  `STAGE_TEXT`) + builders `queued` / `running` / `completed` / `failed`. The
  render layer enforces the contract: italic `details` only on non-terminal rows,
  plain `output` only on terminal ones.
- **Timeline (`slack/timeline.rs`)** — `SlackTimeline` slimmed to runtime state +
  Slack mutation, mapping worker phases onto Job/Build/Run/Finalize.
- **Live queue from enqueue** — `JobStore::record_plan_message_ts(job_id, …)` (a
  default method over `insert_event`, no migration); the connector posts the
  queued card pre-claim and persists its `ts`; the runner's
  `update_queue_positions` edits the Slack card's position; the Reporter's
  timeline **resumes** the same card on claim (never reposts).
- **Rich completion** — a `markdown` results table (reusing `metric_table`) + a
  primary "Download Profiler Data" `section` (presence-gated S3 `stacks-bench.db`).

## Post-ship fixes

- **2026-06 — removed the download button's `action_id`** (`slack/card.rs`). The
  primary "Download Profiler Data" URL button carried
  `action_id = download_profiler_db`, which made Slack dispatch a `block_actions`
  interaction over Socket Mode whose **echoed message contained this card's
  `plan` block**. `slack-morphism` can't deserialize a `plan` block, so the
  envelope was never ACKed → Slack redelivered it → listener-error / reconnect
  churn (the `unknown variant 'plan'` WARN + `ResetWithoutClosingHandshake`). The
  download is client-side and we register no interaction handler, so the
  `action_id` was pure liability; removed it (regression test pins "no
  `action_id` on the card"). **Forward constraint:** cards must stay
  **non-interactive** (URL buttons, no `action_id`s) until `slack-morphism`
  learns the `plan` block, else Slack echoes it back and chokes the listener.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0023-slack-card-redesign` | primary | shipped |

Folds the **Slack-card slice** of queue visibility (`0014`) onto the card; the
GitHub-check pre-claim slice of `0014` and the generic phase-event work (`0017`)
stay separate.

## Why

The v6/v7 card only appears when the job **starts running** and shows three
terminal-ish rows. A long queue wait is invisible (a static ⏳), the rows don't
narrate progress, and the metrics are crammed into a task row instead of a proper
table. v8 makes the card a **live timeline** from the moment of enqueue and a
**clean results view** at the end.

## Feasibility (`slack-messaging` 0.7.4 / Block Kit)

| Want | Supported? |
| ---- | ---------- |
| Italic `details`, plain `output` per row | ✅ `RichTextElementText.style.italic` |
| Tense-progressing titles | ✅ plain strings we control per status |
| Rich-markdown results table | ✅ the `markdown` block (GFM tables; 12k-char cap; message surfaces) |
| Primary "Download" button | ✅ `Button.url(...)` + `.primary()` in a `section` accessory |
| **Auto-collapse the plan on completion** | ❌ **no API field** — the `plan` block has only `title`/`block_id`/`tasks`; the chevron is client-only |

→ The plan can't be force-collapsed, so completion keeps the (user-collapsible)
plan **above** the new results blocks (the chosen layout).

## The card

**Four rows**, each a tense-progressing title + italic `details` (while
pending/in-progress) that clears into a plain `output` on complete:

| Row | pending | in-progress | complete — title / output |
| --- | ------- | ----------- | ------------------------- |
| **Job** | "Queued" · *position N/M, waiting Xm* | "Preparing job" · *…* | "Job started" / Started after Xm (waited on K prior jobs) |
| **Build** | "Build benchmark binaries" · *waiting for job to start* | "Building benchmark binaries" · *building release binary [t]* | "Built benchmark binaries" / Built in t (release build) |
| **Run** | "Run benchmark" · *waiting for binaries* | "Running benchmark" · *[t]* | "Benchmark run completed" / Completed in t |
| **Finalize** | "Finalize results" · *waiting for run* | "Publishing artifacts" · *…* | "Benchmark completed" / Completed in T *(or "Benchmark failed" / Failed in T)* |

**On completion**, below the plan: a `divider`, a `markdown` results table (the
metrics — reusing the existing `metric_table` GFM builder), a `divider`, and a
`section` whose accessory is a **primary "Download Profiler Data"** button (the
presigned `stacks-bench.db` URL — S3-only, presence-gated as today). On failure,
the errored row carries the reason and the results blocks are omitted.

> **Timing deferred to `0024`.** The per-row timing in the table above (live
> `[t]` elapsed and completed `in t` outputs) is **not** in v8 — a completed row
> shows its past-tense title + ✓, with the numbers in the results table. Live
> ticking needs heartbeat-driven updates and per-stage durations need to survive
> a resume, so it's its own item
> ([`0024-slack-card-stage-timings`](0024-slack-card-stage-timings.md)).

## Phases

### Phase 1: The new render (`bench_summary`)

**Goal:** the 4-row tense plan + the completion results blocks, as pure render
functions, unit-tested in isolation. No wiring.

**Scope:**

- Generalize `PlanCard` to **4 rows**, each carrying a per-row `title`, an italic
  `details` line, and an `output` (a `PlanRow` struct rather than parallel
  `[_; 3]` arrays).
- `render_results_blocks` → the `markdown` table block + the primary-button
  `section` (presence-gated DB link).
- An italic rich-text helper (`RichTextStyle.italic(true)`).

**Acceptance:** render snapshots match the 4-row tense card + the completion
layout (plan above → markdown table → primary button); the no-metrics and
no-DB-link fallbacks still render; the typed-builder fallback path stays.

### Phase 2: 4-stage tense timeline + completion layout (`SlackTimeline`)

**Goal:** `SlackTimeline` drives 4 stages with tense titles + italic details, and
finalize uses Phase 1's results layout. Still posted at **start** (queue is
Phase 3) — the Job row is born at "Job started".

**Scope:**

- `SlackTimeline`: 3 → 4 stages; `stage_for_phase` maps the worker phases onto
  Job/Build/Run/Finalize; per-stage tense title + italic detail strings.
- `completed` renders **plan + results blocks** (a multi-block message, not just
  the plan); `failed`/`cancelled` mark the row error and keep the plan.
- `SlackReportSurface` (v7) is unchanged in shape — it already wraps the timeline.

**Acceptance:** a run posts the card and advances the 4 rows with tense titles;
completion shows the results table + download button; the suite stays green.

### Phase 3: Live queue from enqueue

**Goal:** the card posts the moment the job is **queued** and shows live position.

**Scope:**

- **New writer — `JobStore::record_plan_message_ts(job_id, &str)`.** The connector
  holds `Arc<dyn JobStore>` and an **unclaimed** core `Job` (no `RunnableJob`
  exists pre-claim), so today's `RunnableJobStore::set_plan_message_ts(&RunnableJob, …)`
  ([job_source.rs](../../../crates/sbgh-daemon/src/job_source.rs)) isn't callable from
  it. Add a by-`job_id` writer on `JobStore` that emits the same
  `plan_message_sent` event; `RunnableJobStore::set_plan_message_ts` delegates to
  it, and `JobStore::latest_plan_message_ts` reads it back **unchanged**.
- **Connector at enqueue, in order:** `create_adhoc_job` → returns `Job`; post the
  queued card in-thread (all rows pending; the Job row reads "Queued"); `record_plan_message_ts(job.id, ts)`.
  Recording is **non-fatal** — on failure, log loudly and let the claim-time
  reporter post a replacement card (today's comment/check identity-persistence
  semantics). The ⏳ reaction stays the at-a-glance status.
- The runner's queue-position updater updates **Slack** cards (the Job row's
  details → "position N/M, waiting Xm") as position changes — extending today's
  GitHub-check-only `update_queue_positions` (the `0014` Slack slice).
- On claim, the Reporter's `SlackTimeline` **resumes** the card (it already reads
  `plan_message_ts` back) and advances Job → "Job started".
- At enqueue the commit isn't resolved yet, so the title shows the **rev** until
  claim, then updates to the short SHA.

**Ownership boundary:** the connector may **only** create the pre-claim card +
persist its identity. All claimed/running/terminal mutation stays owned by
`SlackTimeline` via the v7 `SlackReportSurface` — Phase 3 steps outside the
surface model **only** because there is no reporter before claim.

**Acceptance:**

- An `@mention bench …` posts a "Queued" card immediately, the position updates
  while it waits, and it animates through to the completed results view.
- The queued card's `ts` is **persisted for an unclaimed Slack job** and
  `list_queued` / claim assembly reads it back — so the reporter resumes the same
  card and never reposts.

## Decisions

1. **Plan stays above the results** on completion — auto-collapse isn't
   API-settable, so the timeline stays (user-collapsible) and the markdown table
   plus the download button render beneath it.
2. **Metrics move to a `markdown` table block**, out of the Run row's `output` —
   the row outputs become one-line summaries; the detailed numbers get a proper
   table (the same `metric_table` GFM builder PR comments use).
3. **Post at enqueue, resume on claim** — the connector posts the card; the
   reporter's timeline resumes it via the persisted `ts` (the v6 resume path).
   **One** card across queue → run → done.
4. **Reuse, don't fork** — the v7 `SlackReportSurface` wraps the richer timeline
   unchanged; the connector + runner reuse the same render + plan-message
   persistence seam, not a parallel one.
5. **Pre-claim card persists by `job_id`** *(from the Codex Phase-3 review)* — a
   new `JobStore::record_plan_message_ts(job_id, …)`, since the connector has no
   `RunnableJob` yet; `RunnableJobStore::set_plan_message_ts` delegates to it and
   `latest_plan_message_ts` reads it back. Non-fatal, mirroring comment/check
   identity persistence. **Ownership boundary:** the connector owns *only* the
   pre-claim card; `SlackTimeline` / `SlackReportSurface` owns everything from
   claim onward.

## Final Validation

A live `@mention bench …`: the card appears immediately as "Queued · position
N/M", updates position while waiting, animates Job → Build → Run → Finalize with
tense titles + italic details, and finalizes as the plan + a markdown results
table + a green "Download Profiler Data" button.

## Follow-Ups / relationships

- `0014` (pre-claim placeholder / queue visibility) — v8 ships its **Slack-card**
  slice; the GitHub-check pre-claim placeholder stays in `0014`.
- `0017` (generic phase events) — finer sub-states ("Preparing benchmark") would
  ride new phase labels; v8 approximates them from today's phases.
- `0024` (Slack card stage timings) — per-row live elapsed + completed durations,
  deferred from v8 (needs persisted per-stage timing across a restart). Filed
  separate from Phase 3 to keep the pre-claim seam uncluttered.
