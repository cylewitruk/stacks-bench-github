# Roadmap 5 — Coordinator / worker execution architecture

Successor to [roadmap-v4.md](./roadmap-v4.md). Goal: split the daemon's single
serial `execute()` into three cooperating roles — a **coordinator**, per-job
**workers**, and per-job **reporters** — communicating over channels, so that:

1. **Multiple benchmarks can run in separate VMs concurrently** (admission gated
   by a resource-aware slot limit), and
2. benchmark **execution is decoupled from reporting** — a worker only runs the
   VM and emits status events; a dedicated reporter owns every GitHub + DB
   side-effect, and
3. the daemon **shuts down cleanly** — a `systemctl stop` (or a double `ctrl-c`)
   aborts in-flight runs, tears down their VMs, and fails their jobs; a single
   `ctrl-c` drains gracefully (stop claiming, let in-flight finish).

Process unchanged: Opus implements, Codex reviews, Opus fixes.

> **Status: Phase 1 shipped** (the worker/reporter split, concurrency = 1) —
> implemented + Codex-reviewed in two slices. Phases 2–5 are planned. Phases are
> ordered so each is independently shippable with tests green; **Phase 1
> delivered the separation-of-concerns win** at concurrency = 1, and concurrency
> is only switched on in Phase 3.

## Why

Today [runner.rs](../crates/sbgh-daemon/src/runner.rs)'s `execute()` does
*everything* for one job, serially, one job at a time:

```text
claim_next → preflight (resolve commit + create check + post comment + start_running)
          → run libvirt driver to terminal  (the ~30-min part)
          → complete/fail (DB) + conclude check + final comment
```

Two problems follow:

- **No concurrency.** The `run()` loop claims one job and runs it to completion
  before claiming the next ([runner.rs:148-156](../crates/sbgh-daemon/src/runner.rs#L148-L156)).
  A second `/benchmark` waits ~30 min behind the first even when the host could
  run both. As hardware grows this is pure idle capacity.
- **The runner does too much.** It owns VM lifecycle *and* GitHub *and* DB
  writes. The reporting concern is smeared across `execute()`,
  [`ProgressReporter`](../crates/sbgh-daemon/src/progress.rs), and
  `ProgressPhaseListener` ([runner.rs:563](../crates/sbgh-daemon/src/runner.rs#L563)),
  coupled to execution by direct calls and a callback trait.

The good news: **the driver is already built for concurrency.** Every per-run
resource is namespaced by `job.id` —
[the VM domain `sbgh-{job_id}`, job dir, LVM chainstate snapshot, and results
tmpfs](../crates/sbgh-daemon/src/libvirt/driver.rs#L139-L145), and even the git
mirror fetch lands on a per-job ref
[`refs/sbgh/{job_id}`](../crates/sbgh-daemon/src/libvirt/git_mirror.rs#L62). So
this is an **orchestration** change, not a driver rewrite.

---

## Target architecture

Three roles. The coordinator is the only long-lived loop; workers and reporters
are spawned as a pair per in-flight job and live for that job only.

| Role | Owns | Touches GitHub / DB? |
| ---- | ---- | ---- |
| **Coordinator** | claim policy, the concurrency slots (a resource-aware semaphore), the stuck-claim sweep, spawning the worker+reporter pair, queue-position knowledge, graceful shutdown | **queue state only** — `claim_next` + `sweep` (the scheduler's own transitions); no GitHub, no `prepare`/reporting/lifecycle writes |
| **Worker** (one per running job) | *pure execution* — run the libvirt driver to a terminal outcome; emit phase/heartbeat/terminal events | no |
| **Reporter** (one per running job) | the *only* GitHub + job-lifecycle side-effect owner — **`prepare`** (resolve commit → ensure check → post comment → `start_running`) + per-phase surface updates + terminal (`complete`/`fail`/`set_check_run`/`set_comment_id`), the per-job debounce, the reconcile, `app_id`, the non-fatal policy | yes |

> **Boundary, precisely:** the *only* DB writes the coordinator makes are the
> queue-state transitions inherent to being the scheduler (`claim_next` →
> `claimed`, the stuck-claim `sweep`). **Every** GitHub call and every
> reporting/lifecycle write — including `prepare` and `start_running` — is the
> reporter's. So `prepare` is the reporter's **first stage**, not a coordinator
> step (resolving Codex's side-effect-ownership finding).

### Data flow

```text
              ┌─────────────── Coordinator (single loop) ───────────────┐
              │  sweep_stuck_claims (periodic)                           │
              │  while slot free:  claim_next ──┐  (queue state only)    │
              │  on pair join:     free slot    │                        │
              └─────────────────────────────────┼──────────────────────-─┘
                                                 │ spawn pair per job
                          ┌──────────────────────┴───────────────────────┐
                          │                                               │
              ┌──────── Reporter ─────────┐               ┌───── Worker ──────┐
              │ 1. prepare():             │  resolved job │ run libvirt driver│
              │    resolve commit,        ├──────────────▶│ emit Phase/Heart/ │
              │    ensure check, comment, │     (kick)    │      Finished     │
              │    start_running          │               └─────────┬─────────┘
              │ 2. consume events ◀───────┼─── mpsc events ──────────┘
              │ 3. terminal: complete/fail│
              └───────────────────────────┘
```

`prepare()` is the **reporter's first stage** (resolve commit → ensure check →
post comment → `start_running`), so all GitHub + lifecycle writes stay in the
reporter. There is one data dependency: the worker needs the **resolved commit**
to fetch/run, so the reporter completes `prepare` and hands the resolved job to
the worker before the worker starts; per-phase + terminal reporting then flows
back over the channel. The worker itself never touches GitHub or the DB. The
event enum:

```rust
// Recipe-neutral: `Phase` carries an opaque label (forward-looking
// constraint 2), and the terminal payload is the recipe's associated
// outcome type — `BenchRecipe::Outcome` today, not a hardcoded bench struct.
enum WorkerEvent<O> {
    Phase { label: PhaseLabel, elapsed: Duration },
    Heartbeat { label: PhaseLabel, elapsed: Duration },
    Finished(O), // O = <R as Recipe>::Outcome
}
```

This replaces the `PhaseListener` *callback* ([runner.rs:674](../crates/sbgh-daemon/src/runner.rs#L674))
with a channel send, and folds the inline `ProgressReporter` terminal calls
([runner.rs:264](../crates/sbgh-daemon/src/runner.rs#L264),
[runner.rs:284](../crates/sbgh-daemon/src/runner.rs#L284)) into the reporter's
`Finished` handler. The driver's push-based phase model is what makes this a
natural swap rather than a rewrite.

---

## Design considerations

### 1. One reporter per job, not one global reporter

A single global reporter consuming one shared channel is simpler, but a hung
GitHub call (the 15 s client timeout) for job A would **head-of-line block** job
B's terminal report. Per-job reporters isolate latency and ordering, and scope
the debounce state naturally (no `HashMap<job_id, …>`). Cost is one extra
lightweight task per in-flight job — negligible. **Recommended: per-job
reporter.** (A global reporter remains a valid concurrency = 1 starting point if
we want to stage the split, but going per-job from Phase 1 avoids rework.)

### 2. Channel discipline — heartbeats are droppable, transitions are not

Use a small **bounded** mpsc. `Heartbeat` is a "still alive" signal — drop it
(`try_send`, ignore `Full`) under backpressure. `Phase` transitions and
`Finished` must never be lost — `send().await` them. Losing a `Finished` would
strand a job's terminal report. Document the asymmetry at the send site.

### 3. `prepare` is the reporter's first stage (not a coordinator step)

`prepare` (resolve commit + ensure check + post comment + `start_running`) is a
bundle of GitHub + lifecycle side effects, so it belongs to the **reporter**, not
the coordinator (consistent with the side-effect boundary above). Each claimed
job spawns its reporter; the reporter runs `prepare` first, then kicks the worker
with the resolved job, then consumes its events. Because each job's `prepare`
runs in its own per-job task, multiple jobs prepare **concurrently** — admission
isn't serialized behind anyone's `resolve_commit`/`pr_head_sha` GitHub round-trip.
The slot/permit is reserved at claim (before `prepare`) so the concurrency limit
still holds across the prepare + run window.

### 4. Concurrency is resource-aware, not CPU-bound

A slot is a **whole VM**: build/bench vCPUs + GiB + an LVM thin snapshot +
tmpfs. The limit is a configurable `[vm].max_concurrent_jobs` (**default 1** →
behaviourally identical to today), **not** `cpus - 2`. A later refinement can
admit by a resource budget (sum of per-job vCPU/memory ≤ host capacity) rather
than a flat count; flat count first.

### 5. Shared mutable resources to audit (the only non-job-keyed state)

Everything per-run is `job_id`-namespaced *except*:

- **The git mirror bare repo** ([git_mirror.rs](../crates/sbgh-daemon/src/libvirt/git_mirror.rs)).
  Refs are already namespaced (`refs/sbgh/{job_id}`), so refs won't collide, but
  concurrent `git fetch` processes mutate one repo's object store / `packed-refs`
  / `FETCH_HEAD` and can lock-contend. Mitigation: a per-mirror async `Mutex`
  around the fetch (simple, serializes only the seconds-long fetch, not the run),
  or a per-job fetch into an isolated `GIT_DIR`. **Audit + decide in Phase 2.**
- **sccache** — shared by design; concurrent compiles against one cache is its
  normal, supported mode. Expected safe; confirm no `--stop-server`/dir-wipe in
  our teardown races a peer.
- **libvirt network / MAC / DHCP** — all VMs on the `default` network; confirm
  MACs/hostnames are unique per domain (they should derive from the unique
  domain name/UUID) so two concurrent VMs don't collide on a lease.

### 6. Failure isolation + two-mode shutdown

Workers run in a `JoinSet` under a `CancellationToken`. A panicking worker fails
**that** job only (the coordinator catches the join error and routes a synthetic
`Finished(Failed)` to its reporter).

Shutdown has **two modes**, driven by signals (full design in Phase 4):

- **Drain** (1× `SIGINT` / terminal `ctrl-c`): stop claiming; let in-flight runs
  finish naturally; exit when idle. Queued jobs wait for the next boot.
- **Abort** (`SIGTERM` from `systemctl stop`, or a 2nd `SIGINT`): cancel
  in-flight workers, **tear down their VMs**, and mark their jobs **failed**
  (`finish_reason = aborted`), then exit.

The hard part is **async cancel-safety**: dropping a worker future at an
`await` does **not** destroy its VM — tokio cancellation just stops polling. So
abort must be *cooperative* — the worker `select!`s on the cancel token and, on
cancellation, explicitly runs cleanup before returning. To make that robust and
reusable, introduce an idempotent **`cleanup_by_job_id(job_id)`** on the driver
that reconstructs every resource name from the job id (destroy `sbgh-{job_id}`,
drop the LVM snapshot, unmount the tmpfs, `rm -rf` the job dir) — usable both for
live abort **and** for startup recovery of VMs orphaned by a hard kill, which
strengthens the existing **stuck-claim sweep**
([runner.rs:117](../crates/sbgh-daemon/src/runner.rs#L117)) into a true
crash-recovery path (reclaim the row *and* clean the orphaned domain).

### 7. The DB claim path is already concurrency-safe

`claim_next_queued` hands out a claim token and every `running`/terminal write
is guarded on `(id, claim_token)` ([job_source.rs:268-277](../crates/sbgh-daemon/src/job_source.rs#L268-L277)).
Concurrent claims from one coordinator are serialized in the loop anyway; the
guards + sweep mean even a future multi-claimer stays correct.

**Migration scope (resolving Codex's migration finding):** the *execution*
refactor — Phases 1 (split), 3 (coordinator), 4 (shutdown) — needs **no schema
change**. The one piece that does is **forward-looking constraint 2** (generic
phase events): today `job_event_kind` is an enum with bench-specific
`PhaseBuild*`/`PhaseBench*` values ([models.rs:485](../crates/sbgh-core/src/models.rs#L485)).
Generalizing to `PhaseStarted`/`PhaseFinished` (label in `detail`) is a **small,
additive** migration — `ALTER TYPE job_event_kind ADD VALUE IF NOT EXISTS
'phase_started'`/`'phase_finished'` (transaction-safe on PG12+, the same pattern
v4 used), keeping the old values readable for back-compat rather than rewriting
history. So: one additive enum migration, owned by whichever phase lands
constraint 2 — not "none".

### 8. Wiring

`runner.run()` is one arm of the daemon's top-level `tokio::try_join!`
([main.rs:174](../crates/sbgh-daemon/src/main.rs#L174)). The coordinator replaces
it in place; worker/reporter pairs are spawned *inside* the coordinator, so the
top-level join arity is unchanged.

---

## Forward-looking: keep the engine task-agnostic

This app may grow from `stacks-bench-github` into a `stacks-github` platform
hosting several **long-running task kinds** ill-suited to GitHub runners
(benchmarking first; **block validation** next). ~80% of the codebase is already
a generic "long-running GitHub-task" platform — the bench-specific parts cluster
into three pluggable concerns: *trigger/command*, *run recipe* (what runs in the
VM, its phases, its resource shape), and *result extraction + render*. The full
platformization (crate split, rename, the second task kind) is its own effort —
see [roadmap-v6.md](./roadmap-v6.md).

The one piece that is **expensive to retrofit** is the worker/reporter boundary
this roadmap is about to draw. So v5 builds it **generic over a task kind from
day one** (one impl — `BenchRecipe` — for now), rather than wiring `stacks-bench`
straight through. Concretely, these are **constraints on the phases below**, not
separate work:

1. **Worker/reporter are defined over a `Recipe` trait, not "benchmark."** The
   Phase 1 worker runs `recipe.execute(ctx, events)`; the reporter renders via
   `recipe.render(outcome)`. Bench is `BenchRecipe: Recipe`. (The `WorkerEvent`
   stream is already recipe-neutral.)
2. **Generic phase events.** Collapse the bench-specific `PhaseBuild*`/
   `PhaseBench*` ([models.rs:485](../crates/sbgh-core/src/models.rs#L485)) into
   `PhaseStarted{label}` / `PhaseFinished{label}` carrying the label in `detail`,
   so a kind with different phases doesn't grow the enum.
3. **Typed results stay per-kind.** `job_metric` remains bench's result table;
   future kinds get their own (subject-vs-provenance: each subdomain owns its
   result schema). `job` / `job_event` stay generic.
4. **Command dispatch is a small registry.** `/<command>` → task kind, so adding
   `/validate-blocks` later is a registration, not a new hardcoded handler.
5. **No new bench names in generic code.** A job carries an opaque `task_kind` +
   payload; `bench_args` lives *inside* `BenchRecipe`.

These cost ~nothing while building v5 and avoid a bench-coupled engine that v6
would have to unpick. Everything else (renaming `sbgh-*`, splitting crates,
writing the block-validation recipe) is deferred to v6 — mechanical once this
boundary holds.

---

## Phase 1: Event bus + worker/reporter split (concurrency = 1)

**Goal:** Decouple execution from reporting with **no** behaviour change — still
one job at a time. This is the separation-of-concerns win and the foundation for
everything after.

**What:**

- Introduce `WorkerEvent` + a bounded mpsc per job.
- Extract a **`Worker`** that owns the `LibvirtDriver` run and emits events
  (replacing `ProgressPhaseListener`'s callback with channel sends).
- Extract a **`Reporter`** task that owns `prepare` (resolve-commit +
  `ensure_reporting` + `start_running`, today's preflight logic, unchanged) as
  its first stage, then consumes `WorkerEvent` — folding in the moves of
  `ProgressReporter` (`started`/`completed`/`failed`), the per-phase surface
  updates, the debounce, and the DB terminal writes.
- The runner's `execute()` becomes: claim → spawn the per-job `(reporter, worker)`
  task → await it. The reporter runs `prepare`, kicks the worker with the
  resolved job (consideration 3), pipes events, and writes the terminal. The
  coordinator's only DB touch stays the claim/sweep.

**Design notes:**

- Keep `prepare` as the reporter's first stage (consideration 3); the permit is
  reserved at claim, before `prepare`, so per-job prepares run concurrently
  without breaking the limit. Keep the `app_id` `OnceCell` + reconcile in the
  reporter/prepare path verbatim — just relocated.
- The non-fatal reporting contract is preserved exactly; only its owner moves.
- **Task-agnostic boundary (forward-looking constraint 1):** define the worker
  over `recipe.execute(ctx, events)` and the reporter over `recipe.render(...)`,
  with `BenchRecipe` as the sole impl. This is the load-bearing seam for v6 — get
  it right here, not later.
- **Channel discipline — the abort *action* is Phase 4 (re-scoped):** the
  `EventSink` splits reliable `phase` (returns `SinkResult`) from best-effort
  `heartbeat`, and the channel-backed sink now genuinely returns `SinkClosed`
  when the reporter is gone. **Surfacing** it is done in the channel slice (the
  worker logs it loudly). **Acting** on it — aborting an in-flight run — is
  deliberately *not* done here: a clean abort must interrupt the driver mid-run
  and tear the VM down (`cleanup_by_job_id`), which is exactly the **Phase 4**
  cancellation path; a dirty `select!`-drop now would leak the VM (worse than
  letting the run finish + tear down). So the worker carries a loud `TODO`
  pointing at Phase 4. The *symmetric* gap — a reporter that dies mid-run
  leaving the job stuck in `running` — is partly closed in the channel slice
  (the reporter terminal-fails on an abnormal channel close, so a *dead worker*
  can't strand the job); a *dead reporter* still needs Phase 4's stuck-`running`
  recovery (today's sweep only reclaims `claimed`).

**Status:** complete (landed in two Codex-reviewed slices).

- [x] Initial implementation completed
- [x] Integration coverage added (worker emits ordered events; reporter drives surfaces + DB to terminal; non-fatal preserved; abnormal channel-close terminal-fails)
- [x] Reviewed — Codex signed off
- [x] Complete

**Notes:**

- **Slice A (recipe seam):** `Recipe`/`TaskContext`/`TaskOutcome` + `BenchRecipe`,
  recipe-neutral `WorkerEvent`/`PhaseLabel`, the libvirt driver decoupled from
  `RunnableJob`. **Slice B (channel + reporter):** bounded `mpsc` + `oneshot`
  hand-off, the spawned per-job `Reporter` owning `prepare` + all GitHub/DB
  side-effects (forward-looking constraint 1 fully realized — `prepare` moved
  into the reporter, not deferred).
- **Carried to Phase 4:** the `SinkClosed` → in-flight **abort action** (needs
  the driver cancellation path + `cleanup_by_job_id`; a dirty drop would leak
  the VM) and the **dead-reporter / stuck-`running` recovery**. A dead *worker*
  is already handled in-slice (the reporter terminal-fails on abnormal close).

---

## Phase 2: Driver concurrency-safety audit

**Goal:** Prove (and fix where needed) that two runs can execute simultaneously
without corrupting shared state. De-risks Phase 3 before any parallelism is
switched on.

**What:**

- **Git mirror:** add a per-mirror async `Mutex` around `fetch_sha` (or a
  per-job isolated `GIT_DIR`); add a test that two concurrent fetches into one
  mirror don't corrupt refs/objects.
- **sccache / LVM / libvirt:** confirm concurrent snapshots off the shared
  origin, concurrent sccache use, and unique MAC/DHCP per domain. Document
  findings; fix any shared fixed name discovered.
- Add a focused concurrency test at the driver seam (two `RecordingShell` runs
  interleaved) asserting no shared-path collisions.

**Design notes:**

- This phase is mostly **audit + a small lock**, not new architecture. If the
  audit finds everything already safe, it collapses to the git-mirror lock + a
  regression test.

**Status:** complete (Codex signed off).

- [x] Initial implementation completed
- [x] Integration coverage added — `concurrent_mirror_fetches_are_serialized`
- [x] Reviewed — Codex signed off
- [x] Complete

> **Noted assumption (Codex):** `MIRROR_LOCK` is **process-local** — it protects
> the mirror only within one daemon, which matches today's one-daemon-per-host
> model. Two daemons sharing a `paths.git_mirror` would need an OS file lock
> (`flock`); out of scope.

**Audit findings:** as predicted, the audit collapsed to **one** hazard.

- **git mirror** — the only shared mutable state on the per-job path. Fixed with
  a process-global `MIRROR_LOCK` (`tokio::Mutex`, one mirror per daemon)
  serializing every mirror mutation (`ensure` clone, `fetch_sha`, `prune`). This
  closes the fresh-host double-`clone --mirror` TOCTOU and `FETCH_HEAD`/
  packed-refs/object-store races. Per-job refs `refs/sbgh/<job_id>` already
  avoided ref-name collisions. Cost is negligible (the guarded ops are
  seconds-long vs a ~30-min run).
- **Safe by construction (no change needed):** VM domain (`sbgh-{job_id}` name +
  per-job UUID + libvirt **auto-generated MAC** — no `<mac>` is emitted), LVM
  chainstate snapshot (`sbgh-{job_id}-chainstate` off a **read-only** origin;
  `lvcreate` is VG-locked by LVM), results tmpfs + job dir (keyed by `job_id`),
  and sccache (shared by design — atomic content-addressed writes, no teardown
  wipe).

**Design notes:**

- The driver-seam concurrency test is realized as the git-mirror serialization
  test (a probe `Shell` proving peak in-flight = 1); a separate driver-level
  interleave test would only re-prove per-job path isolation already covered by
  the audit. Real multi-VM execution lands behind Phase 3's default-1 limit.

---

## Phase 3: Coordinator + slot pool (real concurrency)

**Goal:** Run up to `max_concurrent_jobs` benchmarks at once.

**What:**

- Replace the single-job `run()` loop with a **coordinator**: a `Semaphore`
  (size `[vm].max_concurrent_jobs`, default 1) + a `JoinSet` of worker/reporter
  pairs. Claim while permits are free; spawn a pair per claim; free the permit on
  join.
- Add `[vm].max_concurrent_jobs` config (default 1) + example + docs.
- Graceful shutdown via `CancellationToken`; panic isolation via `JoinSet` join
  errors → synthetic `Finished(Failed)` (consideration 6).
- Keep the stuck-claim sweep on its existing cadence.

**Design notes:**

- Default 1 means **deploying this phase changes nothing** until the operator
  raises the limit — a safe rollout.
- Claim stays serial inside the coordinator loop; only execution parallelizes.

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (N concurrent jobs reach terminal independently; a panicking worker fails only its job; sweep still recovers)
- [ ] Reviewed — Codex signed off
- [ ] Complete

---

## Phase 4: Signal handling & lifecycle shutdown

**Goal:** Clean, predictable shutdown — `systemctl stop` and `ctrl-c` abort or
drain in-flight runs deterministically instead of leaking VMs or stranding jobs
mid-flight.

**Signal → mode mapping:**

| Signal | Source | Mode |
| ---- | ---- | ---- |
| `SIGTERM` | `systemctl stop` | **Abort** — cancel + cleanup + fail in-flight, exit |
| `SIGINT` ×1 | terminal `ctrl-c` | **Drain** — stop claiming, finish in-flight, exit when idle |
| `SIGINT` ×2 | terminal `ctrl-c` again | **Abort** (escalate from Drain) |

**What:**

- A signal task (`tokio::signal::unix` for `SIGTERM` + `SIGINT`) that drives a
  shared shutdown state (a `watch` channel of `Running | Draining | Aborting`)
  and the coordinator's `CancellationToken`. `SIGINT` while `Running` → `Draining`;
  `SIGINT` while `Draining`, or any `SIGTERM` → `Aborting` (cancel fires).
- **Coordinator:** stop claiming once not `Running`; on `Aborting` propagate
  cancellation to all workers; exit once the `JoinSet` is empty.
- **Worker cancel-safety:** wrap the driver run in `select!` against the cancel
  token; on cancel call `driver.cleanup_by_job_id` (consideration 6) and emit
  `Finished(Aborted)`. Optionally thread the token into the driver's poll loops
  so abort is *prompt* rather than waiting for the current phase await to yield.
- **Reporter:** `Finished(Aborted)` → `fail(job, "aborted by shutdown")` (DB) +
  conclude the check `failure` with an "aborted" note. The run shows a clean ✗,
  not a spinner.
- **Top-level wiring + precise exit condition (resolving Codex's
  drain-completion finding):** a single `CancellationToken` (the "shutdown"
  token) is shared by all `tokio::try_join!` arms
  ([main.rs:174](../crates/sbgh-daemon/src/main.rs#L174)). The sequencing:
  1. Signal → state becomes `Draining` (or `Aborting`). The **coordinator** stops
     claiming; on `Aborting` it also cancels in-flight workers.
  2. The coordinator runs until its `JoinSet` is empty (drain: natural
     completion; abort: cleanup-then-finish), and **only then triggers the shared
     shutdown token** and returns. The token firing is the single signal that
     "all in-flight work is done".
  3. The other arms **observe that same token and return**: the API server via
     `axum::serve(...).with_graceful_shutdown(token.cancelled())`; the webhook
     processor via a `select!` on `token.cancelled()` in its loop. (On `Draining`
     they keep serving *until step 2 fires the token* — so the daemon doesn't
     accept-but-never-exit.)
  4. All three arms returned → `try_join!` completes → process exits.

  The key invariant Codex flagged: **the coordinator owns drain completion and is
  the sole trigger of the shutdown token**, so there is always a definite party
  that ends the join. (On `Aborting` the token can fire immediately *after*
  cleanup; the same path.)
- **systemd unit:** `KillMode=mixed` (so systemd `SIGTERM`s only the daemon and
  lets *it* tear down the VMs, instead of `control-group` killing qemu children
  out from under cleanup) + a generous `TimeoutStopSec` (cleanup of N concurrent
  VMs must finish before systemd escalates to `SIGKILL`). Document in
  [host-bringup.md](./host-bringup.md).

**Design notes:**

- The worker-level cancel-safety primitive (`cleanup_by_job_id` + cooperative
  cancel) is independently valuable and could be pulled forward to Phase 1/2 to
  give clean single-job shutdown before concurrency lands; the SIGINT 1×/2×
  escalation needs the coordinator (Phase 3), so the full phase sits here.
- **Fail vs re-queue on abort** is a real choice — see open questions. This phase
  assumes **fail** (the user's stated intent: "abort/cleanup/fail"), with a clear
  remark so an operator can re-trigger.

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (SIGTERM aborts+cleans+fails; SIGINT drains; 2×SIGINT escalates; cancel actually tears down the VM)
- [ ] Reviewed — Codex signed off
- [ ] Complete

---

## Phase 5: Resource-aware admission + queue-position reporting

**Goal:** Admit by host capacity rather than a flat count, and surface a job's
queue position — for which this architecture removes the *concurrent-updater*
cost, with one caveat about pre-claim jobs.

**What:**

- **Queue position — two regimes (resolving Codex's reporter-lifetime finding):**
  reporters exist **only for claimed/in-flight jobs**, so a still-DB-queued job
  has *no* reporter to push to. Therefore:
  - *Post-admission* (claimed, slot reserved, but maybe waiting on `prepare`/a
    peer): the job's reporter is live, and the coordinator emits its position to
    it — free, a coordinator→reporter event, no updater.
  - *Pre-claim* (sitting in the DB queue, no reporter): position visibility is
    **exactly roadmap-v4 Phase 3's placeholder check** — a check persisted
    independently of any reporter, refreshed by a small **queued-position
    updater** (a coordinator-side periodic task over the queued rows). This is
    the piece that genuinely needs building; it's not free.
- So the honest promise is: **in-flight position is free; pre-claim position
  reuses v4 Phase 3's placeholder + a lightweight queued updater.** (If we want
  to scope smaller first, ship "position visible from admission onward" and defer
  pre-claim visibility to the v4 Phase 3 landing.)
- **Resource budget admission** (optional): admit by Σ(per-job vCPU/memory) ≤
  host capacity instead of a flat slot count, so heterogeneous build/bench
  shapes pack the host without oversubscribing.

**Design notes:**

- This phase removes v4 Phase 3's *concurrent-updater-per-running-job* cost (those
  jobs now have reporters); the **pre-claim** placeholder + queued updater is the
  residual work, shared with v4 Phase 3 — the two roadmaps converge here. The
  reporter is also the natural home for v4 Phase 3's placeholder/`skipped` checks.

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (or N/A justified)
- [ ] Reviewed — Codex signed off
- [ ] Complete

---

## Dependencies & related work

- **roadmap-v4 Phase 3 (placeholder / `queued (N before)` checks)** — Phase 5
  removes the *per-running-job* concurrent-updater cost (those jobs now have
  reporters), but **pre-claim** queue visibility still needs v4 Phase 3's
  placeholder check + a small queued-position updater (see Phase 5). Sequence v5
  Phases 1–3 first; then v4 Phase 3 + v5 Phase 5 land together.
- **DB migrations:** none for the execution refactor (Phases 1/3/4); **one
  additive enum migration** for generic phase events (forward-looking constraint
  2 — see consideration 7). A resource-budget admission table, if ever wanted,
  would also be additive.
- **`[vm].max_concurrent_jobs`** is the one new config knob (Phase 3); the
  reporting `[reporting]` config from v4 is unaffected.
- **The systemd unit** (`KillMode`/`TimeoutStopSec`) gains documented settings in
  Phase 4 — a [host-bringup.md](./host-bringup.md) change, no code.

## Open questions (for Codex)

1. **Reporter granularity** — per-job (recommended, consideration 1) vs a single
   global reporter. Agree per-job?
2. **Git mirror isolation** — a per-mirror fetch `Mutex` (serialize the
   seconds-long fetch) vs per-job isolated `GIT_DIR` (more disk, full
   parallelism). Which trade-off?
3. **Prepare placement** — serial-in-coordinator first, promote to a per-job
   stage only if admission latency proves to matter? Or build it as a stage from
   the start?
4. **Shutdown policy** — drain in-flight runs on `SIGTERM`, or stop-claiming +
   let the stuck-claim sweep reclaim on reboot? (The latter is less code and
   already correct; draining is friendlier to a long run mid-flight.)
5. **Admission model** — is a flat `max_concurrent_jobs` enough for the
   foreseeable host, or should Phase 5's resource budget be in scope sooner?
6. **Abort outcome — fail vs re-queue.** On abort the plan **fails** in-flight
   jobs (stated intent), giving a clean ✗ "aborted". The alternative is to
   **re-queue** them (operator-initiated abort isn't a real benchmark failure)
   so they re-run on next boot — which is also exactly what the stuck-claim sweep
   would do if we simply exited without marking them. Fail (visible, explicit) or
   re-queue (resumes work)?
7. **Drain scope.** Does `Draining` only stop the coordinator claiming (queued
   jobs simply wait — simplest, matches "processed on next startup"), or should
   it also pause the webhook processor so the DB queue stops growing? Stopping
   the claim loop alone is sufficient; pausing the processor is optional polish.
