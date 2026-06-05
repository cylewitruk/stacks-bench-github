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

> **Status: Phases 1–4 shipped + signed off** (Phase 4 also **smoke-tested
> live** on the host: SIGTERM mid-run → gray "cancelled" check; `kill -9` +
> restart → orphan VM cleaned + row `cancelled` + check concluded). Phase 1
> (worker/reporter split), Phase 2 (mirror-lock concurrency audit), Phase 3 (the
> coordinator + slot pool, behind a default-1 limit), and Phase 4 (signal
> handling / graceful shutdown / orphan recovery) are done. Phase 5
> (resource-aware admission + queue-position) planned. Phases are ordered so each
> is independently shippable with tests green; concurrency is only *usable* once
> an operator raises `[runner].max_concurrent_jobs`.

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

Job tasks run in a `JoinSet`, each with a child of the daemon abort
`CancellationToken`. A panicking task fails **that** job only: dropping it closes
its `events` channel, so the (separate) reporter task hits its channel-close
`abandon` path and terminal-fails the job; the coordinator just logs the
`JoinError` with job context. (No synthetic `Finished(Failed)` — the reporter
owns the terminal write.)

Shutdown has **two modes**, driven by signals (full design in Phase 4):

- **Drain** (1× `SIGINT` / terminal `ctrl-c`): stop claiming; let in-flight runs
  finish naturally; exit when idle. Queued jobs wait for the next boot.
- **Abort** (`SIGTERM` from `systemctl stop`, or a 2nd `SIGINT`): cancel
  in-flight workers, **tear down their VMs**, and mark their jobs **failed**
  (`finish_reason = aborted`), then exit.

The hard part is **async cancel-safety**: dropping a worker future at an
`await` does **not** destroy its VM — and worse, dropping it *mid-provision*
orphans the source **loop device** (`/dev/loopN`, dynamically named, attached
then detached *inside* `SourceDisk::provision`). So abort is **not** a
worker-side `select!`-drop. Instead (as built in 4A): **the cancel token is
threaded into the driver** and honored at a **cancellation-safe point — the poll
loop, never mid-provision**. Provision runs atomically; once the VM is running,
a cancel makes the poll loop stop and the driver's **normal teardown** (with its
artifact handles, including the loop detach) runs. The worker just awaits and
maps `token.is_cancelled()` → aborted.

A separate, idempotent **`cleanup_by_job_id(job_id)`** (4B) covers the
*handle-less* case — orphans from a dead reporter or hard-killed daemon, where
no live driver exists. It reconstructs resources from the job id (destroy
`sbgh-{job_id}`, drop the LVM snapshot, unmount the tmpfs + `source.mnt`, detach
the loop device via `losetup -j` on the job-id-named backing file, prune the git
ref, `rm -rf` the job dir), strengthening the **stuck-claim sweep**
([runner.rs:117](../crates/sbgh-daemon/src/runner.rs#L117)) into a true
crash-recovery path (reclaim the row *and* clean the orphan).

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
  deliberately *not* done here: a clean abort needs the **Phase 4** driver
  poll-loop cancellation path (so the driver's normal teardown runs with
  handles); a dirty `select!`-drop now would leak the VM (and, mid-provision,
  the source loop device). So the worker carries a loud `TODO` pointing at Phase
  4. The *symmetric* gap — a reporter that dies mid-run
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
- **Carried to Phase 4:** the `SinkClosed` → in-flight **abort action** (the
  driver poll-loop cancellation path; a dirty drop would leak the VM) and the
  **dead-reporter / stuck-`running` recovery** (the handle-less
  `cleanup_by_job_id`). A dead *worker* is already handled in-slice (the reporter
  terminal-fails on abnormal close).

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

- Replace the single-job `run()` loop with a **coordinator**: an `Arc<Semaphore>`
  (size `[runner].max_concurrent_jobs`, default 1) + a `JoinSet` of per-job
  tasks. Claim while permits are free; spawn a task per claim with the permit
  **moved into the task** (frees the slot on completion — no coordinator
  bookkeeping); free + top up on `join_next`.
- Add `[runner].max_concurrent_jobs` config (default 1) + example + host-bringup
  note. **Under `[runner]`, not `[vm]`** (Codex): the limit is on daemon
  execution *slots*, not VM capacity — task kinds go non-VM in v6.
- **Panic isolation falls out of Phase 1 for free:** a job-task panic drops its
  `events_tx` → the reporter's channel-close path terminal-fails the job; the
  coordinator logs the `JoinError` with job context (a task-id→job-id map). No
  synthetic `Finished(Failed)` needed — the reporter owns the terminal write.
- Keep the stuck-claim sweep each loop iteration (the lease keeps it off
  actively-`running` jobs).

**Design notes:**

- Default 1 means **deploying this phase changes nothing** until the operator
  raises the limit — a safe rollout. (Verified: the whole prior suite passes
  through the new `JobDeps::run` path unchanged.)
- Claim stays serial inside the coordinator loop; only execution parallelizes.
- **Graceful/abort shutdown is Phase 4**, not here — this phase has no
  `CancellationToken`; the coordinator loops forever and relies on the existing
  sweep + the reporter's terminal handling for recovery.

**Status:** implementation complete (pending Codex review).

- [x] Initial implementation completed
- [x] Integration coverage added — `coordinator_enforces_limit_and_tops_up`
      drives the extracted `Coordinator` fill/reap seam with a blocking source,
      asserting the slot limit is enforced (spawns exactly the limit, no
      over-claim while full, tops up on completion);
      `concurrent_jobs_reach_terminal_independently` covers per-job isolation.
      The panicking-task path is logging-only here (the reporter's abnormal-close
      terminal-fail is covered by `abnormal_channel_close_terminal_fails_the_job`)
- [x] Reviewed — Codex signed off
- [x] Complete

> **Noted (Codex):** `in_flight()` (`JoinSet::len`) is an *upper bound* — a task
> can free its semaphore permit (allowing a top-up) before the next `join_next`
> reaps it. The **semaphore** is the real concurrency cap, not the task count.

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
- **Worker cancel-safety (done in 4A):** the cancel token is **threaded into
  the driver** (`run_benchmark`→`run_phase`→`poll_to_completion`) and honored at
  the **poll loop only** — never mid-provision (dropping the run there would
  orphan the source loop device). The worker **awaits** the recipe (no
  `select!`-drop) and maps `token.is_cancelled()` → `Finished(Aborted)`; the
  driver's normal teardown runs with handles. **`cleanup_by_job_id` is not used
  here** — it's the handle-less orphan-recovery path below.
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

- Two distinct mechanisms, don't conflate them: **live in-process abort** is
  driver-token / poll-loop cancellation (4A, done) — there's a running driver
  that does its own teardown; **`cleanup_by_job_id`** is the *handle-less*
  orphan/stuck-`running` recovery (4B) for a dead reporter or hard-killed daemon
  where no driver exists. The cancel-safety primitive (4A) was buildable before
  the signal wiring; the SIGINT 1×/2× escalation needs the coordinator (Phase 3),
  so the orchestration sits here.
- **Fail vs re-queue on abort** is a real choice — see open questions. This phase
  assumes **fail** (the user's stated intent: "abort/cleanup/fail"), with a clear
  remark so an operator can re-trigger.

**Status:** complete + **live-validated** — 4A + 4B-1 + 4B-2 + 4C all
Codex-signed-off and smoke-tested on the host: `systemctl stop` mid-run produced
a gray "cancelled" check; `kill -9` + restart cleaned the orphaned VM (destroy/
undefine/umount/`losetup -j`/`lvremove`/dir-removal, all reconstructed from the
job id), cancelled the row, and concluded the stuck check.

- **4A — cancel-safety primitive (done; revised after Codex review):**
  cancellation is **threaded into the driver** and honored at the **poll loop
  only** (`run_benchmark`→`run_phase`→`poll_to_completion` take a
  `CancellationToken`; a `FinishReason::Cancelled`). The worker **awaits**
  `recipe.execute(ctx, sink, &token)` (it does *not* drop the future) and maps
  `token.is_cancelled()` → `Terminal::Aborted`; the reporter records `Aborted`
  as `fail("aborted by shutdown")`, **propagating a fail-write error** so a job
  that may still be `running` isn't reported as cleanly handled. The coordinator
  hands each job a **child** token off one daemon `shutdown` token (not fired in
  4A).
  - **Why poll-loop-only (the Codex High fix):** dropping the `execute` future
    mid-`SourceDisk::provision` would orphan its **loop device** (attached then
    detached *within* provision; `/dev/loopN` is dynamically named and not
    reconstructable by id). So provision runs **atomically**; cancel is observed
    once the VM is running, and the driver's **normal teardown** (with handles)
    tears it down. No partial-provision cleanup needed → **`cleanup_by_job_id`
    and `Recipe::cleanup` are deferred to 4B** (cross-boot/dead-reporter orphan
    recovery, where it'll also handle `source.mnt` + the loop device via
    `losetup -j` on the job-id-named backing file).
  - Tests: `cancellation_breaks_at_poll_loop_and_tears_down` (driver: pre-cancel
    → poll breaks → teardown destroys the domain) and
    `a_cancelled_run_is_reported_aborted` (worker: cancelled token → `Aborted`).
- **4B-1 — signals + graceful shutdown (done):** [shutdown.rs](../crates/sbgh-daemon/src/shutdown.rs)
  defines three `CancellationToken`s — **`abort`** (parent of the per-job child
  tokens; fired on Abort), **`draining`** (stop-claiming; set on Drain or Abort),
  **`exit`** (the coordinator fires it when drained + idle). `watch_signals` maps
  `SIGINT`×1 → Drain, `SIGINT`×2 / `SIGTERM` → Abort. `Runner::run(shutdown)`
  stops claiming when draining and returns once idle, firing `exit`; the API
  (`with_graceful_shutdown`) + processor (`select!`) observe `exit` so
  `try_join!` completes and the process exits — the coordinator is the **sole**
  trigger. systemd unit gains `KillMode=mixed` + `TimeoutStopSec=120s` (so the
  daemon drives teardown instead of systemd SIGKILLing its `virsh` children).
  Test: `drain_stops_claiming_and_fires_exit_when_idle`.
- **4B-2 — orphan/stuck-`running` recovery (done):**
  [`LibvirtDriver::cleanup_by_job_id`](../crates/sbgh-daemon/src/libvirt/driver.rs)
  is now the **complete**, handle-less, idempotent teardown — destroy/undefine
  the `sbgh-<id>` domain, unmount the results tmpfs **and** `source.mnt`, find +
  detach the dynamically-named loop device via `losetup -j <source.raw>` (the
  piece 4A couldn't reconstruct from the id), `lvremove` the
  `sbgh-<id>-chainstate` snapshot, prune the git ref, `rm -rf` the job dir; every
  step logged-and-continued. Two new `JobStore` methods back the recovery:
  `running_job_ids` (after a crash, every `running` row is necessarily an orphan)
  and `fail_orphan` (unconditional `running → failed` + a `failed` event, **no
  claim-token guard** — the claimer is dead and recovery runs before any fresh
  claim; idempotent on the guard). *(Superseded by **4C-1**: `fail_orphan` →
  `cancel_orphan`, `failed` → `cancelled` — a crash-orphan is re-triggerable, not
  a failure.)* The coordinator runs `recover_orphans`
  **once at startup, before the loop**: for each orphan, `cleanup_by_job_id`
  **then** `fail_orphan` — that order is crash-safe (a crash mid-recovery
  re-lists the still-`running` row next boot, so cleanup re-runs idempotently and
  no VM leaks behind a `failed` row). Orphans are **failed**, not re-queued
  (consistent with abort; a crash mid-run may recur) — PR jobs are re-triggered
  with `/benchmark`, baselines by the next push. v5 is bench-only so recovery
  dispatches straight to the libvirt driver; v6's task-kind split would pick the
  cleanup by the orphan's stored kind.
  - **Codex review hardening (two Mediums):** (1) `cleanup_by_job_id` returns a
    **source-loop-clear** signal — narrowly "the source loop is verified gone,
    so deleting `source.raw` + failing the row are safe," *not* "every artifact
    cleaned." The loop is singled out because it's the only artifact whose
    recovery needs the backing file; the rest (domain, tmpfs, LVM, git ref) stay
    best-effort and a transient failure there does NOT hold the row `running`
    (id-addressable without `source.raw`; don't wedge the lifecycle on a flaky
    `lvremove`). If the loop can't be verified-detached (`losetup -j`/`-d`
    failed), it **preserves the job dir** (so `source.raw` — the only handle to
    re-find the loop — survives) and returns `false`; `recover_orphans` then
    leaves the row **`running`** (skips `fail_orphan`) so the next boot retries,
    rather than failing it and stranding the leak. (2)
    `recover_orphans` now returns `Result` and **propagates** a
    `running_job_ids` enumeration failure — startup-critical, since we can't
    rule out live orphan VMs; the process exits and systemd
    `Restart=on-failure` retries rather than claiming blind. **Re-review
    Medium:** a non-zero `losetup -j` exit (a genuine query failure, vs a
    missing file which exits 0/empty on util-linux) is now also treated as
    incomplete — empty stdout after a failed query can't be read as "all clear",
    so `source.raw` is preserved and the row left `running`.
  - **Out of 4B-2 scope (follow-up):** concluding an orphaned PR job's
    stuck-spinning Check Run — needs the reporting context (check id + repo +
    installation) reconstructed at startup; a re-triggered `/benchmark` posts a
    fresh check meanwhile. The dead-reporter-while-alive case is recovered at the
    **next** restart, not in-process (a reporter panic is a rare bug).
  - Tests: `cleanup_by_job_id_reconstructs_full_teardown_from_id` (exact command
    order + `losetup -j`→`-d` of the surfaced device + snapshot/job-dir removal),
    `cleanup_by_job_id_skips_loop_detach_when_none_attached` (no spurious
    `losetup -d`), `startup_recovers_orphaned_running_job` (recovery runs
    `cleanup_by_job_id` + `cancel_orphan` before claiming),
    `running_job_ids_lists_only_running_jobs`,
    `cancel_orphan_terminalizes_running_without_a_claim_token_and_is_idempotent`,
    `cancel_orphan_ignores_a_claimed_not_running_job` (4C-renamed). Review hardening adds
    `cleanup_by_job_id_preserves_backing_file_when_loop_detach_fails`,
    `startup_leaves_orphan_running_when_cleanup_incomplete`, and
    `startup_recovery_aborts_when_listing_running_jobs_fails`.

  - [x] 4A implementation completed
  - [x] 4A coverage (cancel breaks at the poll loop, `losetup -d` ran before
        cancel, normal teardown destroys the domain; worker maps token → aborted)
  - [x] 4A reviewed — Codex signed off
  - [x] 4B-1 implemented (SIGTERM aborts; SIGINT drains; 2×SIGINT escalates; graceful exit)
  - [x] 4B-1 reviewed — Codex signed off
  - [x] 4B-2 implemented (complete `cleanup_by_job_id` + `running_job_ids`/`fail_orphan` + startup `recover_orphans`)
  - [x] 4B-2 review round 1 — Codex 2 Mediums (loop-leak preservation; list-failure startup-critical) addressed
  - [x] 4B-2 review round 2 — Codex Medium (non-zero `losetup -j` → incomplete, preserve) addressed
  - [x] 4B-2 review round 3 — Codex Low (return-contract is *source-loop-clear*, not "fully complete"); doc + `source_loop_clear` rename
  - [x] 4B-2 coverage added (10 tests across driver, store, coordinator)
  - [x] 4B-2 reviewed — Codex signed off (3 rounds; no findings on the last pass)
  - [x] Complete + live-validated (kill-9 orphan recovery on the host)

### Phase 4C — Cancelled terminal status (abort/orphan ≠ failure)

**Status:** complete + **live-validated** — 4C-1 + 4C-2 both Codex-signed-off.
A `systemctl stop` mid-run produced the gray "Cancelled" check + "re-run with
`/benchmark`" comment; a `kill -9` + restart cleaned the orphan, cancelled the
row, and concluded its stuck check as gray "Cancelled" with the "daemon
restarted while this run was in progress" reason — both on real PRs.

**Why:** before this, an operator-initiated abort *and* a crash-orphan both
landed as `failed` — a red ✗ check that reads as "the benchmark broke" and
counts against failure metrics, when the run was simply *stopped*. The
`cancelled` status existed in both PG enums (`job_status`, `job_event_kind`) and
the Rust models since the first migration but was **never written** — 4C starts
using it. No migration needed.

- **4C-1 — Cancelled end-to-end (done):**
  - `CheckRunConclusion::Cancelled` → GitHub's native `cancelled` conclusion
    (`status_strings` maps it to `("completed", "cancelled")`), which renders
    **neutral-gray**, not a red ✗.
  - Store: `cancel_job(job_id, claim_token, remark)` — claim-guarded
    `claimed|running → cancelled` + a `cancelled` event, mirroring `fail_job`
    minus the forensics result (a cancelled run produced none). `fail_orphan`
    (added in 4B-2, only called by `recover_orphans`) is **replaced** by
    `cancel_orphan` (unguarded `running → cancelled`) — a crash-orphan is
    re-triggerable, not a failure (the operator's chosen classification).
  - Reporter: `Terminal::Aborted` now routes to `jobs.cancel(...)` +
    `ProgressReporter::cancelled(...)` (gray check + a "cancelled, re-run…"
    comment) instead of `fail(...)`/`failed(...)`. The check's re-trigger hint
    branches by surface (Codex review): PR jobs say "re-run with `/benchmark`",
    baseline (`branch_push`/`tag_created`) checks say "re-run by pushing the
    branch/tag again" — the comment's `/benchmark` copy is PR-only by
    construction (`update_comment` no-ops for baselines).
  - Runner: `recover_orphans` → `cancel_orphan` (orphan rows become `cancelled`).
  - **Metrics:** cancelled runs are excluded from baselines / change-impact
    deltas *by construction* — `cancel_job`/`cancel_orphan` write no `job_metric`
    (only `complete_job` does), and baselines select only metric-bearing rows.
  - **`event_status` note:** the cancelled timeline event uses
    `JobEventStatus::Fail` (the `job_event_status` PG enum has no neutral value,
    and it's an audit-only field) — the load-bearing fields are
    `job.status=cancelled` + `event_kind=cancelled`. A dedicated `cancelled`
    event_status would need a migration for negligible gain; deferred.
  - Tests (5 new): `cancel_job_transitions_running_to_cancelled_with_event`,
    `cancel_job_rejects_a_stale_claim_token`,
    `cancel_job_terminalizes_a_claimed_job_not_yet_running`,
    `cancelled_concludes_check_cancelled` (progress),
    `aborted_terminal_cancels_the_job_and_check` (reporter); plus the 4B-2
    orphan tests retargeted to `cancel_orphan` + `Cancelled`.
- **4C-2 — conclude the orphaned check (done):** at startup recovery, after
  `cancel_orphan` succeeds, the coordinator concludes the orphan's
  stuck-`in_progress` Check Run (and updates its stale comment) as `cancelled` —
  closing the spinner the 4B-2 follow-up flagged. Rather than duplicate the
  conclusion logic, a new read-only `RunnableJobStore::load_runnable(job_id)`
  reconstructs the orphan's `RunnableJob` view (sharing `claim_next`'s assembly
  via an extracted `assemble_runnable`, but taking **no claim** —
  `claim_token = None`, status untouched), and the existing
  `ProgressReporter::cancelled` does the rest (gray check + correct re-trigger
  hint, exactly matching a live abort). Best-effort: the row is already
  terminal, so a GitHub blip just leaves the check spinning (no worse than
  pre-4C-2) and isn't retried. Tests: `startup_concludes_orphan_check_as_cancelled`
  (runner) + `v2_source_load_runnable_assembles_view_without_claiming` (read-only,
  no status change) + `v2_source_load_runnable_surfaces_existing_check_for_conclusion`
  (the existing check id + url are surfaced — the data the conclusion needs).

- [x] 4C-1 implemented (Cancelled status + gray check; abort + orphan → cancelled)
- [x] 4C-1 coverage added (6 new tests, incl. the baseline re-trigger-hint fix)
- [x] 4C-1 reviewed — Codex signed off (Low: baseline check hint) + live-validated via `systemctl stop`
- [x] 4C-2 implemented (orphan check-conclusion via `load_runnable` + reused `ProgressReporter::cancelled`)
- [x] 4C-2 coverage added (3 new tests; 531 daemon+core green)
- [x] 4C-2 reviewed — Codex signed off (added the existing-check-surfacing test)
- [x] Complete + live-validated (kill-9 orphan → row cancelled + check concluded gray)

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

**Status:** sub-sliced by value-at-`max=1`. **5.1 (queued "#N ahead")
Codex-signed-off**; **CPU pinning (5.5) implemented** (review pending) so
concurrent benchmarks can run on dedicated cores. 5.2 (in-flight position) and 5.4 (resource-budget
admission) are **deferred** — they only pay off at `max_concurrent > 1`, which
isn't deployed; revisit when the knob is raised. 5.3 (PR-sync discoverability
placeholder, the v4-Phase-3 convergence) is a separate, migration-bearing
follow-up.

- **5.1 — queued "#N ahead" position (done):** a coordinator-side updater
  (`update_queue_positions`, run each loop iteration after `fill_slots`) reports
  each still-queued job its place. Key facts that made it migration-free:
  `pr_comment`/`branch_push` jobs already carry their head SHA at enqueue, and
  the existing `check_run_created` job-event already persists a check id for the
  reporter to adopt. Mechanics:
  - `JobStore::queued_jobs_ordered` (FIFO `created_at, id`, matching
    `claim_next_queued`) + `RunnableJobStore::list_queued` (read-only assembly via
    the shared `assemble_runnable`, `claim_token = None`).
  - For queued job at index `i`, `ahead = in_flight + i` (runs that finish/claim
    before it). `ensure_position_check` creates **or** updates a check in the
    existing `in_progress` state with a "queued — N ahead" body — *not* a new
    GitHub `queued` state, so the signed-off reporter is untouched and there's no
    queued→in_progress transition gap; on claim the reporter adopts the same
    check (id persisted) and phase updates replace the text.
  - Restart-safe + dedup: mirrors the reporter's reconcile-or-create-and-persist
    (`find_check_run_by_external_id` → reuse, else create + `set_check_run`). A
    **failed persist returns `false`** (Codex review) so the position isn't
    debounced — the next tick re-reconciles + retries recording the
    `check_run_created` event the claim-time reporter reads back to adopt.
  - An in-memory `last_positions` map suppresses redundant GitHub edits (only on
    a position change) and is pruned to the live queue.
  - **Eligibility:** only jobs whose `[reporting]` wants a check *and* that
    already carry a head SHA (so a `tag_created` job, unresolved until claim, and
    a comment-only/no-report job, get no pre-claim position check).
  - Tests: `coordinator_reports_queue_positions_and_debounces`,
    `coordinator_updates_existing_position_check_without_duplicating`,
    `coordinator_skips_position_for_no_check_and_unresolved_jobs`,
    `v2_source_list_queued_returns_queued_in_claim_order_readonly`.
  - **Known caveat (for review):** an unbounded queue means one position check per
    queued job per change; fine for realistic queues, but a `top-N` cap (with a
    logged "+M more") is a natural follow-up if queues ever get deep.
  - **Same-SHA dedup (5.1 follow-up, from the live smoke-test):** the smoke-test
    surfaced that GitHub shows only the **latest-updated** check run per
    `(app, name, head_sha)`, so two jobs on the *same commit* (e.g. a double
    `/benchmark` on one PR) fight over the single `stacks-bench` check — the
    running job's phases and the queued job's position alternate. This is a
    *pre-existing* consequence of per-job checks on a shared SHA (two running
    jobs would collide too); 5.1 just made it visible during the wait. **Fix
    (operator's choice — dedup at enqueue):** `JobStore::find_active_job(repo,
    commit, trigger_kind)` + the `/benchmark` accept path skips enqueuing when an
    active (`queued`/`claimed`/`running`) `pr_comment` job already covers that
    exact `(repo, head_sha)` — silently (consistent with how the processor
    handles denials; the existing job's check is the implicit feedback), mapped
    to the `EnqueuedJob` outcome like the per-webhook `AlreadyEnqueued`. A
    re-`/benchmark` after the job finishes still runs (terminal jobs don't
    block). Cross-kind collisions (a `branch_push` baseline + a PR `/benchmark`
    on the same SHA) are a noted, rarer limitation — they report to different
    surfaces, so they're not deduped. Tests:
    `find_active_job_matches_repo_commit_kind_and_excludes_terminal` (store) +
    `benchmark_for_a_commit_already_being_benchmarked_is_deduped` (processor).
  - **Guarantee — best-effort, not hard (Codex review):** the dedup is a
    check-then-insert *outside* the atomic job-creation boundary, so it isn't a
    structural guarantee. Two windows remain: (1) two **concurrent processors**
    (the queue is `FOR UPDATE SKIP LOCKED`) could both pass the check then both
    insert — unreachable on the current **single daemon**, real only if the
    processor is scaled out; (2) a **crash** between the dedup decision and the
    inbox marking the webhook done could, on retry-after-the-original-finished,
    enqueue a fresh job. Crucially, the worst case of *both* is a redundant
    **re-run** (the original's check has already concluded), **not** the
    concurrent check collision the dedup prevents — so the live UX problem
    doesn't recur. **Structural hardening (if/when a hard guarantee or
    multi-processor is wanted):** a partial unique index
    `UNIQUE (github_repo_id, git_commit_hash) WHERE trigger_kind = 'pr_comment'
    AND status IN ('queued','claimed','running')`, with `create_job_with_links`
    catching the unique violation → `AlreadyEnqueued`. Deferred as premature for
    a single-processor deployment.

- [x] 5.1 implemented (queued "#N ahead" position; no migration)
- [x] 5.1 review round 1 — Codex Medium (failed persist must un-debounce / retry) addressed
- [x] 5.1 review round 2 — Codex Low (reconcile path: failed `update_check_run` returns `false`, mirroring the existing-check path)
- [x] 5.1 coverage added (5 new tests; 536 daemon+core green)
- [x] 5.1 reviewed — Codex signed off (2 review rounds; Medium + Low addressed)
- [x] 5.1 smoke-tested live — queue-behind-running showed "queued — 1 run ahead"; surfaced the same-SHA check collision
- [x] 5.1 follow-up: same-SHA `/benchmark` dedup at enqueue (find_active_job) + tests
- [x] dedup reviewed — Codex signed off as a documented **best-effort** UX guard (not a DB-enforced invariant); structural partial-unique-index hardening specced + deferred (single-processor; crash-retry worst case is a redundant re-run, not a collision)
- [x] 5.5 — CPU pinning implemented (per-slot `[runner].cpu_sets` + `host_cpus` → `<vcpu cpuset>` + `<emulatorpin>`; coordinator slot-index pool; host-bringup docs for `nosmt`/`isolcpus`/IRQ + `scripts/irq-affinity.sh` helper)
- [x] 5.5 review round 1 — Codex Low (example/docs: match per-phase vCPUs to slot size; build-oversubscribe-is-contained-but-pointless, bench-must-not) addressed
- [x] 5.5 reviewed — Codex signed off (round 2: copy-paste TOML polish on the example comment)
- [x] 5.5 complete (code; pending deploy + the pinned serial-vs-concurrent A/B)
- [ ] 5.2 / 5.3 / 5.4 — deferred (see status)

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
