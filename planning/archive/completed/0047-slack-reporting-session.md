# v18: Slack Reporting Session (group-scoped surface ownership)

Move Slack stream/card liveness from the **per-run** reporting surface to a
**group-scoped session**, so the card's lifetime matches the trigger/request
lifetime instead of a single run's (`0047`).

> **Status:** shipped — implemented, tested (919 green, lint clean),
> Codex-reviewed, and host-validated. Slack keepalives were observed every ~10s
> in production logs, with no Slack stream idle-timeout recurrence after release.
>
> Corrects the ownership model exposed by v17's per-run keepalive (the
> repeat-group inter-run gap): the per-run `SlackReportSurface` is now a delegate
> into a group-scoped `SlackSession` that owns the card, stream keepalive, and
> reactions for the group's lifetime, reaped on a group-terminal event (with a
> conservative abandonment sweep). The v17 per-run keepalive is superseded.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0047-slack-reporting-session` | primary | shipped |

## Why

The reporting surface is built **per run** ([`build_report_surface`]), and v17's
stream keepalive is owned by that per-run `SlackReportSurface`. But a repeat
group shares **one** Slack card/stream across all its runs. The two lifetimes
diverge:

- The runner processes each run as a separate claim → fresh `Reporter` → fresh
  surface, and enqueues run *N+1* only **after** run *N*'s reporter terminates.
- A non-final `completed()` calls `repeat_completed()`, which marks that run's
  timeline `stage = STAGES`; the surface is then dropped, aborting its keepalive.
- So between run *N*'s terminal and run *N+1*'s `started()` — carry-forward DB
  promotion, re-claim, prepare, VM provisioning, potentially minutes — the shared
  stream is **undriven** and Slack lapses it (`message_not_in_streaming_state`).

This is not patchable at the per-run layer: the keepalive can't outlive a surface
whose timeline is per-run and already terminal. The fix is to make the **session**
(card + stream + reactions + keepalive) a first-class, group-scoped owner, with
per-run surfaces as thin delegates.

The intended semantic model — deliberately plural at both seams, so a future
job/group can fan out to several destinations:

```text
trigger/request ─▶ reporting session(s) ─▶ per-run surface delegate(s)
```

not today's `run ─▶ surface ─▶ Slack liveness`.

## Scope

- A `SlackSession` owning the `SlackTimeline` (card, `plan_ts`, reactions) and the
  keepalive task, keyed by `(benchmark_group_id, Slack target identity)` in a
  daemon-held registry.
- `SlackTimeline` becomes group-scoped with a notion of the **current run**
  (`begin_run`), instead of embedding one run for its whole life.
- `SlackReportSurface` becomes a per-run delegate to the session; it no longer
  owns the timeline, the keepalive, or a `Drop` abort.
- Session lifetime = group lifetime: created on the first run, reaped on a
  **group-terminal** event (final-repeat success, or any run's failure/cancel),
  plus a conservative abandonment sweep.
- Update the `report.rs` reporting-seam docs: Slack is now a per-run **delegate**
  into a group-scoped session, not a self-owning per-run surface.

**Non-goals:** the cross-worker/fleet story (`0004`) — an in-memory session can't
span workers; the persisted `plan_ts` remains the resume fallback there. The
pre-claim queued-stream window stays `0040`'s concern. No change to the card
rendering API or the reaction emoji set. **No multi-destination fan-out** in
v18 (a `CompositeReportSurface(Vec<Box<dyn ReportSurface>>)` or typed combinator)
— but the seam is preserved so it's a small addition, not a retrofit (see the
composability guardrail below).

## Design Decisions

- **Registry owned by the daemon, threaded like `slack`.** A
  `SlackSessionRegistry` (`Mutex<HashMap<Uuid, Arc<SlackSession>>>`, or a sharded
  map) is held by the runner and passed through `Reporter::new` →
  `build_report_surface`, exactly as `self.slack`/`self.jobs` are today. Only
  Slack jobs touch it; non-Slack/`Silent` jobs are unchanged.
- **Key by `(benchmark_group_id, Slack target identity)`.** Even though the first
  implementation has one Slack target per group, key on the target identity
  (channel + thread `ts`) too, so the registry never bakes in "one group = one
  destination forever." `build_report_surface` for a Slack job get-or-creates the
  session. Concurrent groups coexist under distinct keys; runs **within** a group
  are sequential (the runner enqueues the next only post-terminal), so a session is
  never driven concurrently — the registry mutex only guards create/reap.
- **Start the keepalive after `started()`, via idempotent `ensure_keepalive()`.**
  `touch_stream()` no-ops at `stage == 0`, so a task spawned at session *creation*
  could exit before the first card update. The session exposes
  `ensure_keepalive()` (idempotent), called by the per-run surface **after**
  `begin_run + started`, when there's a live in-progress row to keep warm.
- **`begin_run(&job)` resets ALL run-specific state.** Today `SlackTimeline` stores
  immutable run fields (`job_id`, `rev`, `commit`, `commit_url`) and `OnceLock`s
  for the cached-build labels — fine for a per-run object, wrong for a reused
  group-scoped one. Those move into resettable run state (the `OnceLock`s become
  resettable fields) and `begin_run` refreshes them + `benchmark_run_index` +
  stage/timing, so run *N+1* can't inherit run-0 metadata or stale cache labels.
  The card identity, `plan_ts`, and reaction surface stay put. Reaction logic
  (run 0 swaps ⏳→🚀) already keys off `benchmark_run_index`, so it carries over.
- **Per-run surface is a thin delegate.** `SlackReportSurface { session, job, jobs,
  store }`. `started/phase/heartbeat` delegate to `session.timeline`; the keepalive
  and its `Drop` abort move out of the surface entirely. Single-run jobs are a
  group of size 1 — same path, marginally more ceremony, uniformly correct.
- **Reap on an explicit group-terminal predicate.** This is the core ownership
  boundary, so name and test it: a run is **group-terminal** iff it is a
  final-repeat **success** (`job_is_final_repeat`), or **any** run's failure /
  cancel (a failed/cancelled run stops the group). A non-final success is **not**
  group-terminal — it leaves the session alive, covering the inter-run gap. The
  surface reaps (abort keepalive + remove from registry) only on group-terminal.
- **Conservative abandonment sweep.** "No queued/active runs" is briefly true
  during the inter-run carry-forward gap — a naive sweep would reap exactly the
  session it's meant to protect. The sweep is **grace/TTL-gated** (only reap a
  session untouched for longer than the gap could plausibly take) and
  **DB-progress-aware** (don't reap while the group still has runs making
  progress). Modeled on `sweep_stuck_claims`; ships after the core.
- **Composability guardrail — don't harden a new accidental constraint.** Solving
  this Slack bug must not bake in single-destination reporting. Keep `ReportSurface`
  fan-out friendly (non-fatal, `()` return, no single-owner assumptions); keep the
  registry **Slack-specific** (a `SlackSessionRegistry`, not a generic "the group
  session"); and phrase the model as session(s)/delegate(s) plural. A future
  `CompositeReportSurface(Vec<Box<dyn ReportSurface>>)` (PR check + Slack DM +
  channel receipt …) should then be a small addition, not a retrofit — but it is
  **out of scope** for v18.

## Phases

### Phase 1: Group-scoped timeline + session + registry (`0047`)

**Goal:** A `SlackSession` owns the timeline + keepalive; the timeline supports a
current-run handoff.

**Scope:**

- Move `SlackTimeline`'s immutable run fields (`job_id`, `rev`, `commit`,
  `commit_url`) and the cached-build `OnceLock`s into **resettable** run state.
- Add `SlackTimeline::begin_run(&RunnableJob)` — refresh all run-specific state
  (the fields above + `benchmark_run_index`), reset stage/timing/cache labels for
  the new run; leave `plan_ts`/`streaming`/reactions intact.
- New `slack/session.rs`: `SlackSession` (owns `Arc<SlackTimeline>` + the keepalive
  `JoinHandle`; `ensure_keepalive()` spawns it idempotently, `reap`/`Drop` aborts)
  and `SlackSessionRegistry` (get-or-create + reap, keyed by
  `(benchmark_group_id, Slack target identity)`).
- Keep `SlackTimeline::spawn_keepalive`/`touch_stream` as-is — the session is the
  new owner of the handle, started after the first `started()`.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests
- [x] Reviewed
- [x] Validated

> Landed as a self-contained slice (914 green, lint clean). `SlackTimeline`
> run-state moved into resettable `State` (group-constant fields incl. a stable
> `group_id` for log correlation stay `&self`; `run_index` is an `AtomicI32` for
> the lock-free predicates); `ctx()` borrows from `State`. New `slack/session.rs`.
> The new API carries transitional `#[allow(dead_code)]` until Phase 2 wires it.

**Acceptance:**

- [x] `begin_run` refreshes the run index + run metadata + cache labels and resets
  the stage, without disturbing the card identity / `plan_ts` / live reaction; a
  second `begin_run` shows no run-0 metadata or stale cached-build label leakage.
- [x] `ensure_keepalive()` is idempotent — N calls spawn exactly one task.
- [x] Registry get-or-create returns the same session for one group across runs;
  `reap` aborts the keepalive and removes it.

### Phase 2: Per-run surface delegates; lifetime = group (`0047`)

**Goal:** The surface no longer owns Slack liveness; the session spans the group.

**Scope:**

- `SlackReportSurface` holds `Arc<SlackSession>` + `job`; `started()` calls
  `begin_run` + `started`, then `ensure_keepalive()` — **in that order**. This
  ordering is load-bearing: `begin_run` resets `stage` to 0 and `touch_stream`
  exits at `stage == 0`, so a keepalive spawned before the new run's `started`
  (which sets `stage = 1`) would immediately exit. `ensure_keepalive` is
  idempotent, so across a non-final repeat it re-arms rather than double-spawning.
  Remove the surface's keepalive field and `Drop` abort.
- Thread `SlackSessionRegistry` runner → `Reporter::new` → `build_report_surface`;
  `build_report_surface` get-or-creates the session for a Slack job.
- Add an explicit `is_group_terminal(&job, outcome)` predicate; terminal methods
  reap the session **only** when it holds (final-repeat success, or any
  failure/cancel) — non-final success leaves it alive.
- Update the `report.rs` module/`ReportSurface` docs: Slack is a per-run delegate
  into a group-scoped session; the "one surface per job owns the lifecycle" line
  no longer holds for Slack.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests
- [x] Reviewed
- [x] Validated

> Landed (918 green, lint clean). Registry threaded `Runner.JobDeps →
> Reporter::new → build_report_surface`; `SlackReportSurface` is now a delegate
> (`begin_run → started → ensure_keepalive`; no keepalive field / `Drop`). The
> keepalive task was made **group-lifetime** (touch returns `Keepalive::Alive`
> while idle, `Dead` only when the stream is permanently gone) — this is the
> robust answer to Codex's re-arm note: the task never exits between runs, so the
> `stage→0` reset can't strand it. `report.rs` seam docs updated.

**Acceptance:**

- [x] `is_group_terminal` is unit-tested across the matrix: non-final success →
  false; final-repeat success → true; failure/cancel (any index) → true.
- [x] A 2–3-run group keeps **one** keepalive task alive continuously across run
  boundaries (no abort at non-final terminal).
- [x] A non-final repeat does **not** permanently lose the keepalive across the
  `begin_run → started → ensure_keepalive` handoff — covered by
  `repeat_group_shares_one_session_reaped_only_on_final` (keepalive asserted
  running after the handoff) and `touch_stream_stays_alive_but_quiet_when_idle`.
- [x] The session is reaped exactly once, on a group-terminal event; the registry
  is empty afterward.
- [x] Single-run and non-Slack jobs behave exactly as before (existing suites +
  `single_run_slack_job_reaps_on_completion`).

### Phase 3: Abandonment sweep (`0047`)

**Goal:** No session leaks for stuck/abandoned groups — without reaping a healthy
group mid carry-forward.

**Scope:**

- A periodic sweep (alongside the existing claim sweep) reaps a session only when
  it is **both** untouched for longer than a grace TTL (chosen to exceed a
  plausible inter-run carry-forward + provisioning gap) **and** DB-progress-aware:
  its group has no active/queued runs and none making progress. "No runs right now"
  alone is never sufficient — that's the gap the session exists to cover.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests
- [x] Reviewed
- [x] Validated

> Landed (919 green, lint clean). `SlackSession` gained `last_touched` (bumped on
> get-or-create and successful run completion — failure/cancel reap immediately,
> so they don't touch); `SlackSessionRegistry::
> sweep_abandoned(grace, active_groups)` reaps idle-past-`SESSION_ABANDON_GRACE`
> (10 min) sessions whose group isn't active. The coordinator computes
> `active_groups` from the DB (`list_queued` ∪ running-via-`load_runnable`) each
> tick and skips the sweep on any read error (never reaps on uncertainty); a long
> *running* job stays protected (its group is in `running`), so the grace only
> bridges the brief carry-forward gap. `len`/`is_empty` promoted from test-only.

**Acceptance:**

- [x] A group abandoned mid-flight has its session reaped after the grace TTL; a
  healthy group in its inter-run carry-forward gap is **never** reaped early —
  `sweep_reaps_only_idle_and_inactive_sessions` covers the grace gate, the
  active-group (DB-progress) gate, and the abandoned-reap (+ keepalive abort).

## Final Validation

- [x] `just build`
- [x] `just lint`
- [x] `just test` (919 passed, 1 skipped)
- [x] Host smoke: Slack streams have not shown the prior idle-timeout /
  `message_not_in_streaming_state` recurrence after release; repeat/group
  sessions remain driven through the observed host runs.
- [x] Extended trace smoke: a Slack 2–3 repeat request streams continuously across run
  boundaries — `slack: stream keepalive` lines every ~10s through the
  carry-forward / provisioning gaps, and **no** `message_not_in_streaming_state`
  mid-group; the final run reaps the session.

## Follow-Ups

- The v17 per-run keepalive (item `0044`/the keepalive on `SlackReportSurface`) is
  **superseded** by the session and is removed in Phase 2.
- `0004-worker-fleet`: a cross-worker next-run can't share the in-memory session;
  revisit session ownership (a reporting service / persisted keepalive lease) when
  the fleet lands. Until then the persisted `plan_ts` is the fallback.
- `0040-slack-queue-receipt-before-stream`: the pre-claim queued-stream window is
  still its concern; a claim-time stream start composes cleanly with the session.
- `0048-slack-stream-error-classification`: Phase 2 kept the conservative
  "any append error → block mode" fallback. A transient Slack/API blip therefore
  permanently abandons streaming for that card — worth distinguishing transient
  from permanent (`not_in_streaming_state`/missing) errors. Logged as backlog.
- **Multi-destination fan-out** (deferred): with the seam preserved, a
  `CompositeReportSurface(Vec<Box<dyn ReportSurface>>)` or typed combinator can
  later drive several destinations for one job/group (PR check + Slack DM +
  channel receipt). A small addition on top of v18, not a retrofit — file as its
  own item when a second concurrent destination is actually needed.
