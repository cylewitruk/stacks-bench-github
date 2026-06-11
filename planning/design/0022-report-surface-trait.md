# Design 0022: Reporting-surface trait

- **id:** `0022-report-surface-trait`
- **status:** `backlog`
- **depends_on:** `0021-slack-live-timeline` (ship + verify first)
- **unblocks:** `0003-results-portal` (the portal slots in as a third surface)
- **review:** Codex signed off (design)
- **source:** v6 scoping — the reporter/progress "smell" thread (2026-06)

Collapse the two reporting objects — [`ProgressReporter`](../../crates/sbgh-daemon/src/progress.rs)
(lifecycle) and [`ProgressSink`](../../crates/sbgh-daemon/src/reporter.rs)
(worker stream) — into **one `ReportSurface` lifecycle trait** with **one
impl per surface**. A pure internal refactor: no behavior change, no schema.
*(Finishes the Phase-3 `ReportSurface` the `0002` design called for, which was
sidestepped when the Slack timeline was bolted on via `if let Slack … else`
branches.)*

## Why

The smell is **not** "match on `ProgressTarget` instead of polymorphism." It's
that reporting is split by **call-site timing** (`ProgressReporter` fires on the
lifecycle, `ProgressSink` on the worker event stream) instead of by **surface
ownership**. Consequences, in today's code:

- The same `ProgressTarget` interpretation is repeated across ~6 methods × 2
  types — `started/completed/failed/cancelled` each carry an `if Slack { tl…;
  return } else { github… }`; the sink re-derives the surface again via
  `comment_id()` / `check_run_id()`.
- The surface **construction** is duplicated: the `SlackTimeline::new(...) +
  with_timeline(...)` block exists once in the reporter's `run` and again, near
  identically, in the runner's orphan-recovery path.

## Target shape

```rust
#[async_trait]
trait ReportSurface {
    async fn started(&self);
    async fn phase(&self, label: &PhaseLabel, elapsed: Duration);
    async fn heartbeat(&self, label: &PhaseLabel, elapsed: Duration);
    async fn completed(&self, summary: &Value, comparison: Option<&BaselineComparison>);
    async fn failed(&self, error: &str);
    async fn cancelled(&self, reason: &str);
}
```

- `GitHubReportSurface` — PR comment **+** check run **together** (holds
  `comment_id` / `check_run_id`).
- `SlackReportSurface` — the live timeline / thread / reactions (holds
  `plan_message_ts`).

Build **one** surface per job at assembly, behind a single factory used by both
the reporter's `run` and the runner's orphan recovery. Each surface owns its own
durable identity, so nobody re-matches the enum.

## Decisions

1. **Split by surface ownership, not call-site timing.** One trait spanning the
   whole lifecycle; the lifecycle/drain distinction becomes an internal detail
   of the caller, not a type boundary.
2. **Two surfaces, not three** (vs. the initial comment/check/Slack sketch).
   GitHub's comment and check are **coupled by design**: the check is created
   first so its URL feeds the comment, and the terminal check update is
   deliberately owned by the lifecycle path (the sink skips the check on
   terminal to avoid flicker). Splitting them into two fanout subscribers would
   re-introduce ordering/flicker coordination — fake decoupling.
3. **Typed trait methods, not a `ReportEvent` enum or `broadcast`** — each
   surface owns durable identity (`comment_id` / `check_run_id` /
   `plan_message_ts`) read back on reclaim; a broadcast spine would force every
   subscriber to re-derive identity from events, which is strictly worse. The
   per-method signatures (`completed(summary, comparison)`) carry intent a
   uniform `on_event` would flatten.
4. **Sequenced after `0021` ships.** Behavior-preserving, so it's safest and
   most reviewable against a green, *deployed* baseline; the existing
   reporter/sink/timeline tests pin the behavior it must preserve. Don't stack
   it on unreviewed/undeployed code.

## Out of scope

- **Events / broadcast.** Revisit only when a genuinely dynamic observer arrives
  — the portal (`0003`) is the first; even then it's **+1 surface**, not a bus
  spine.

## Acceptance

- One `ReportSurface` per job; `ProgressReporter` + `ProgressSink` are gone.
- Surface construction is DRY — one factory, used by the reporter's `run` and
  the runner's orphan recovery (no duplicated `SlackTimeline::new` block).
- Every existing reporter/sink/timeline test passes **unchanged** (behavior
  preserved).
