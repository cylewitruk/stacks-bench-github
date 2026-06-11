# 0022: Reporting-surface trait

- **id:** `0022-report-surface-trait`
- **status:** `shipped`
- **date:** 2026-06
- **iteration:** v7
- **predecessor:** v6 Slack live timeline ([0021](0021-slack-live-timeline.md))

Collapsed the two reporting objects — `ProgressReporter` (lifecycle) and
`ProgressSink` (worker-event drain), which each re-interpreted `ProgressTarget`
separately — into **one `ReportSurface` lifecycle trait** with one impl per
surface, built once per job behind a shared factory. A pure internal refactor:
no behavior change, no schema (the full suite passed unchanged).

## What shipped

- `crate::report`: a `ReportSurface` trait
  (`started/phase/heartbeat/completed/failed/cancelled`, all non-fatal `()`),
  three impls, and `build_report_surface(gh, jobs, store, slack, job)` selecting
  one by `(ProgressTarget, slack)`:
  - **`GitHubReportSurface`** — PR comment + Check Run together (the check URL
    feeds the comment; terminal-check ownership stays split to avoid flicker),
    absorbing the old `ProgressReporter` GitHub branches + `ProgressSink`'s
    debounced phase path.
  - **`SlackReportSurface`** — a thin adapter over the unchanged `SlackTimeline`
    (+ the artifact store for the completed metrics / DB link).
  - **`NoopReportSurface`** — a Slack target with no client wired (preserves the
    pre-existing silent degrade, explicitly).
- `Reporter::run` builds **one** surface (after `ensure_reporting` +
  `start_running`, so it snapshots the resolved commit + comment/check ids) and
  shares it across `started`, the drain loop, and `finish`;
  `runner.rs::conclude_orphan_report` builds via the **same** factory.
  `ProgressReporter` + `ProgressSink` (and `progress.rs`) deleted.

## Decisions

1. Split by **surface ownership**, not call-site timing.
2. **Two real surfaces, not three** — GitHub comment + check stay coupled
   (splitting them is fake decoupling).
3. **Typed trait methods**, not a `ReportEvent` enum / `broadcast` — each surface
   owns its durable identity (`comment_id` / `check_run_id` / `plan_message_ts`).
4. **`SlackTimeline` wrapped, not rewritten** — the v6 card mechanics untouched.
5. The surface **holds the artifact store** (built once at assembly).
6. A Slack target with **no client** → explicit `NoopReportSurface`, not the
   GitHub catch-all (added from the v7-plan review).

## Validation

Behavior-preserving: the full `just test` suite passed with **no assertion
changes** — the old reporter/sink lifecycle + phase tests were ported verbatim
onto the surfaces. Codex signed off the plan, Phase 1 (additive surfaces), and
the Phase-2 cutover.

## Follow-ons

- Events / `broadcast` spine — only when a dynamic observer arrives; the results
  portal (`0003`) is the first, landing as **+1 surface**, not a bus.
- Dedupe `build_store_or_local` between `Reporter::run`'s surface and
  `pr_metric()` (pre-existing; minor, no item filed).
