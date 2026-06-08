# Roadmap 9 — Distributed worker fleet (`remote-daemon`, capability-scheduled)

Turns the single-host daemon into an **orchestrator + a fleet of remote worker
daemons**, each declaring **capabilities** (`benchmark`, `block-validation`, …)
and pulling compatible work — the GitHub self-hosted-runner model applied to
benchmark/validation jobs. Dedicated bare-metal boxes (pinned bench hosts,
big-local-NVMe block-validation hosts) and eventual cloud-ephemeral instances all
become **instances of one fleet model**.

> **Goal:** scale concurrency and heterogeneous hardware by **adding workers**,
> not by sharing one host. A worker dials *out*, registers its capabilities,
> long-polls for compatible jobs, runs them via its **local `Driver`**
> ([roadmap-v8.md](./roadmap-v8.md) Phase 1), and streams events + artifacts
> back. The orchestrator stays the **sole DB client** and owns all GitHub side
> effects.

Process unchanged: Opus implements, Codex reviews, Opus fixes.

> **Sibling docs:** execution architecture (coordinator/worker/reporter split,
> orphan recovery) is [roadmap-v5.md](./roadmap-v5.md); the **task axis**
> (`Recipe` kinds, block-validation) is [roadmap-v6.md](./roadmap-v6.md);
> change-impact reporting is [roadmap-v7.md](./roadmap-v7.md); the **local
> `Driver` seam** (task ⟂ backend, on one host) is [roadmap-v8.md](./roadmap-v8.md).
> This doc owns the **distribution layer** — getting jobs *to* capable workers.

## Two orthogonal seams

This is **not** another `Driver` kind. There are two seams, and v9 is the upper one:

1. **`Driver` seam (v8 Phase 1)** — *task ⟂ backend*, **local to a worker**: "run
   this `TaskSpec` here." A worker holds one or more local Drivers (a bench worker
   → a pinned/libvirt driver; a block-val worker → a local reflink-fan-out
   driver). v9 does not touch it.
2. **Distribution seam (this doc)** — *orchestrator ⟂ fleet*: "get this job to a
   capable, free worker; stream results back." Sits **above** Drivers.

So a **worker = { transport client + advertised capabilities + one-or-more local
Drivers }**, and the orchestrator becomes a **capability-matched scheduler**.

## The GitHub self-hosted runner mapping

| GitHub self-hosted runners | stacks-github fleet |
| ---- | ---- |
| labels (`runs-on: [self-hosted, gpu]`) | worker **capabilities** + resource facts (cores, RAM, has-local-nvme, reflink-fs) |
| runner **polls** control plane (pull) | worker **long-polls** the orchestrator, capability-filtered |
| one job/runner; logs streamed back | worker runs one job via its local Driver; streams `WorkerEvent`s back |
| registration token; online/offline/busy | worker register + **heartbeat/lease**; drain/deregister |
| ephemeral runners (register→run→deregister) | **cloud-ephemeral workers** later (a provisioner spawns one; it registers, runs, dies) |

The last row is how **cloud stays in scope without committing**: a cloud instance
is just a worker on an autoscaled VM. The parked v8 AWS work
([roadmap-v8.md](./roadmap-v8.md) Phases 3–6) returns here as a **worker
provisioner** — *who* spawns the worker host — orthogonal to this protocol.

## The worker lifecycle

```text
worker daemon starts
  → registers / heartbeats with { capabilities, resources, version }
  → long-polls: "give me compatible work"
  → receives TaskSpec + lease token
  → runs local Driver (v8 Phase 1)
  → streams WorkerEvents back (sequence-numbered)
  → uploads artifacts / summary
orchestrator-side reporter owns DB + GitHub side effects (unchanged)
```

## What's reused vs. genuinely new

**Reused** — v9 is largely "make the Worker a *remote* process":

- The **coordinator → worker/reporter split** (v5) — the Worker just moves across
  a process/network boundary; the **reporter stays orchestrator-side, unchanged**.
- The **claim/lease queue** (`RunnableJobStore`) + atomic claim semantics — now
  brokered *through* the orchestrator (workers don't touch the DB; see Transport).
- **Orphan recovery** (`recover_orphans` / `cleanup_by_job_id`) — generalizes from
  "the in-process worker died" to "a worker's lease expired."
- The **`EventSink` / `WorkerEvent`** contract and the **`summary` blob** — they
  now arrive over the wire instead of an mpsc channel; consumers are untouched.

**Genuinely new** — the honest cost of the direction:

- A **thin worker↔orchestrator API** (register / heartbeat / long-poll-claim /
  event-ingest / artifact-upload / complete).
- **Remote event streaming** back to the reporter (sequence-numbered, idempotent).
- **Remote artifact transfer** (`archive_dir` is local today) — tarball upload or
  an object-store pointer the reporter fetches.
- **Worker lifecycle + liveness** — register/heartbeat/lease/drain/deregister; the
  worker registry on the orchestrator.
- **AuthN/Z** — registration tokens (à la GitHub); per-job secrets (GitHub
  installation tokens) delivered short-lived over the authenticated channel.
- **Capability + resource matching & fairness** — so a flood of multi-hour
  block-validations can't starve bench (this answers v6 Open-Q#4).

## The split: **move** logic out, don't duplicate it

v9 is a **split of today's `sbgh-daemon` monolith**, not a new parallel codebase.
The worker-execution logic is **extracted into a shared crate** — it is *not*
copied, and the two binaries do *not* carry forked copies of it. The orchestrator
stops executing jobs itself; execution lives in one place (the exec crate) and is
run by a worker.

**Where each of today's `sbgh-daemon` modules goes:**

| Module(s) | Destination |
| --------- | ----------- |
| `api/`, `webhook_processor`, `reporter`, `progress`, `comparison`, `bench_summary`, `job_source` | **orchestrator** (keeps DB ownership + all GitHub side-effects) |
| `runner` coordinator (claim/lease/slots/`recover_orphans`) | **orchestrator** → grows into the scheduler + worker registry + the worker-API server |
| `runner::run_worker` + `JobDeps::run` inline worker loop | **moves to the worker** (this is the worker logic that must leave the orchestrator) |
| `driver`, `libvirt/`, `recipe`, `bench_recipe` | **moves to the shared exec crate** (the execution substrate) |
| `events` | **splits at the existing producer/consumer seam** — see below |

**Proposed crate structure (one impl each, no duplication):**

- **`sbgh-exec` (new, shared library)** — the execution substrate: `driver`
  (`Driver`/`TaskSpec`/`DriverOutcome`/`Placement`), `libvirt/`, `recipe`,
  `bench_recipe`, and the worker run loop. Depends only on the wire-contract
  types, never on the DB or GitHub.
- **Wire contract** (in `sbgh-core`, or a small `sbgh-proto`) — `TaskContext`,
  `TaskSpec`, `WorkerEvent`/`Terminal`/`PhaseLabel`, the registration/capability/
  claim/lease messages, and the `summary` shape. The only thing both sides share.
- **`sbgh-worker` (new binary)** — thin: register → long-poll → run `sbgh-exec`
  against the claimed `TaskSpec` → stream events + upload artifacts. Its
  `EventSink` is a **network sink** (today's `ChannelSink` over mpsc becomes an
  HTTP client).
- **`sbgh-daemon` / orchestrator (existing binary)** — `api`, `webhook_processor`,
  the scheduler/registry, the reporter, and the worker-API server. Sole DB client.
  **Never executes a job itself** — execution only happens in an `sbgh-worker`.

**The `events.rs` seam is where the split runs.** `EventSink` (the *producer*) is
worker-side; the **reporter** (the *consumer*) is orchestrator-side; today's
per-job mpsc channel simply **becomes the network**. The roadmap-v5 reliable/
best-effort channel discipline carries straight over to the wire.

**Single-box deployments — a co-located, but always *separate*, `sbgh-worker`
(decided).** Even on one host, the worker is a **separate `sbgh-worker` process**
talking the same API over **loopback** — never an in-process worker inside the
orchestrator. The orchestrator does **only** orchestration/reporting and
**never executes a job itself**. This keeps exactly one execution model (local is
just `localhost`, byte-identical to remote), gives crash isolation (a worker
panic on a long or fan-out job can't take down the orchestrator), and avoids a
special in-process path that would re-fork the very thing the split consolidates.
A one-box install therefore runs two processes — orchestrator + `sbgh-worker` —
both built from this repo, sharing the single `sbgh-exec` implementation.

The **loopback worker** in Phase 1 below is therefore the *real* `sbgh-worker`
binary pointed at localhost — not ad-hoc worker logic re-embedded in the
orchestrator.

## Transport: thin worker API, **not** shared-Postgres claiming (Codex)

The cheap option — workers claim capability-filtered straight from the shared
Postgres queue — was rejected. It reverses a real architectural win (**the daemon
is the sole DB client**, owning policy/lifecycle state) for a saving that mostly
evaporates: workers would need DB credentials + network reachability, schema
changes become worker-deployment concerns, a worker bug could corrupt lifecycle
state directly — and auth, leases, drain, event streaming, and artifact upload
**need a protocol anyway**. So the orchestrator brokers everything over a thin
**pull-based API**; the DB stays behind it. This also matches the GitHub-runner
model and works across NAT/firewalls because workers dial *out*.

## Phase 1: Worker protocol + registry (control plane)

**Goal:** a worker can register, advertise capabilities, long-poll, and be handed
a job — with a **stub task** (no real Driver yet). Prove the protocol on a single
**loopback worker** (worker process on the same host as the orchestrator) before
any network/firewall concerns.

> **Transition note (Codex).** Phase 1 is **control-plane-only / non-production**:
> it proves the protocol with *stub* work. The orchestrator's existing **in-process
> execution stays for production** through Phase 1 — the "orchestrator never
> executes" end state (above) is reached only in **Phase 2**, when `run_worker`
> and the execution substrate move into `sbgh-exec`/`sbgh-worker` and the
> in-process path is removed.

**Scope:**

- A new `sbgh-worker` binary (or daemon mode): config = `{ accepted_tasks,
  resource facts, orchestrator URL, auth token }`; registers + long-polls.
- Orchestrator-side **worker registry** (a `worker` table — orchestrator-owned, so
  the sole-DB-client rule holds) tracking `{ id, capabilities, resources, version,
  last_seen, status }`.
- **Capability-matched offer**: a queued job is offered to a long-polling worker
  whose `capabilities ⊇ job.required_capabilities`. Reuses the atomic claim +
  lease, but the orchestrator performs the DB mutation, not the worker.
- The worker↔orchestrator API surface: `register`, `heartbeat`, `poll` (long),
  `complete` / `fail`. (Event + artifact endpoints land in Phase 2.)

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (loopback worker claims a stub job)
- [ ] Reviewed — Codex signed off
- [ ] Complete

## Phase 2: Remote execution (data plane)

**Goal:** a claimed job actually **runs on the worker** via its local `Driver`,
streams events, and uploads results — bench end-to-end on a remote worker.

**Scope:**

- **Extract the execution substrate into `sbgh-exec` (the move, not a copy; see
  "The split" above).** `driver`, `libvirt/`, `recipe`, `bench_recipe`, and the
  `run_worker` loop **leave `sbgh-daemon`** for the shared crate; the orchestrator
  loses its inline worker. This is a pure code move — v8 Phase 1 already made the
  recipe/driver seam clean, so the coordinator/reporter plumbing is untouched.
- Worker invokes its configured **local `Driver::run_task(TaskSpec)`** (v8 Phase 1,
  now in `sbgh-exec`) on the claimed spec; the worker's local config picks the
  Driver (pinned/libvirt for bench, reflink-fan-out for block-val).
- **Event ingest**: worker POSTs `WorkerEvent`s with **sequence numbers**;
  orchestrator ingest is idempotent + ordered, then fans into the **existing
  reporter** exactly as the in-process `EventSink` does today.
- **Artifact upload**: the worker ships `archive_dir` (tarball) or an object-store
  pointer + the `summary` blob; the orchestrator lands it where the reporter +
  `extract_outcome` already read. **Reporter/DB/vs-baseline path unchanged.**
- A loopback worker first; then a **second physical host** (the real test).
- **Baseline-safety rule (Codex) — `measurement_profile` cannot wait for Phase 4.**
  v7 comparisons are *live*, so the moment a job can run on a remote worker it
  could otherwise reuse the local host's baselines. Phase 2 must therefore do one
  of, explicitly:
  - **(preferred)** stamp each job's `measurement_profile` **at worker-assignment
    time, in the same DB transition that hands out the lease** (Codex) — the
    profile is a property of the **worker that runs the job**, so a still-queued
    job has none yet; it's resolved from the assigned worker's declared profile
    when the orchestrator records `{ worker_id, measurement_profile }` atomically
    with the lease, before execution starts. Even a single profile per worker with
    no sharing/forking UI is enough; `find_baseline_for` filters on it from day
    one. Phase 4 adds only the *operator ergonomics* (shared profiles, per-profile
    noise floor, fork guidance), not the column.
  - **(fallback)** if the column slips, remote-worker jobs render **absolute-only
    / comparison-disabled** until Phase 4 — never compared against a baseline from
    a different worker.

  A remote bench run must **never** silently match a local-host baseline.

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (remote bench run drives the unchanged reporter)
- [ ] Reviewed — Codex signed off
- [ ] Complete

## Phase 3: Liveness, leases, drain & remote orphan recovery

**Goal:** never lose or double-run a job when a worker dies, restarts, or drains.

**Scope:**

- **Heartbeat + lease TTL**; missed heartbeats → lease expiry.
- **Distributed crash-recovery split (Codex).** Local resources (VMs, reflink
  workspaces) exist *only on the worker host*, so the orchestrator cannot reap
  them directly — it must not promise central cleanup. Split it v5-style, across
  the boundary:
  - **Orchestrator on lease expiry** — terminal-cancel the DB row, record a
    **cleanup obligation** `{worker_id, job_id}`, and mark the worker
    `stale`/`offline` if heartbeats stopped.
  - **Worker on startup/reconnect** — run **local** orphan recovery for any leases
    it owned (or poll its outstanding cleanup obligations) and
    `cleanup_by_job_id` locally. Idempotent + best-effort.
  - **If the worker never returns** — the orchestrator can only cancel the row and
    surface the worker as stale; the remote-local resources are unreachable until
    (and if) the host comes back. No false promise of central reaping.
- **Graceful drain**: a worker stops claiming, finishes its current job,
  deregisters (for planned host maintenance / ephemeral-worker teardown).
- **Event resume**: sequence numbers let a reconnecting worker resume its stream
  without duplicating side effects.

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (kill a mid-job worker → job recovers cleanly)
- [ ] Reviewed — Codex signed off
- [ ] Complete

## Phase 4: Baseline trust — `measurement_profile` (migrated from v8 Phase 2)

**Goal:** keep v7 baseline comparisons valid across a heterogeneous fleet —
**without fragmenting baselines per box.** The unit is *"these runs are
comparable,"* and it's **operator-declared**, not auto-fingerprinted. (Per the
Phase 2 safety rule the *column* likely already exists as a single stamped value;
Phase 4 adds the **operator ergonomics** — sharing, per-profile noise floor, fork
guidance — on top.)

**Why operator-declared, not worker-id / hardware-class (Codex + design intent):**
worker id is too granular (redeploying a daemon on the same host would destroy
baseline continuity); raw hardware class is too coarse. The deliberate strategy
of running **small, resource-constrained VMs on large physical hosts** exists
precisely to make different boxes *comparable* — so the operator must be able to
declare that, and **several physical hosts may legitimately share one profile.**

**Scope:**

- A **`measurement_profile`** (a.k.a. `substrate_key`) — an operator-controlled
  label attached to a worker via config. **Multiple workers/hosts MAY share a
  profile** when the operator judges them equalized (the anti-fragmentation
  mechanism). Stable across worker-daemon replacement on the same host.
- v7 `find_baseline_for` + the `job_baseline_*` indexes filter on
  `measurement_profile` (not worker id), so baselines flow across **all** workers
  in a profile. The successor to v8's `execution_backend` trust column.
- **Per-profile noise floor.** `[reporting].noise_cv_pct` becomes
  per-profile: a profile spanning several hosts carries a slightly **higher** CV
  to absorb cross-host variance — the deliberate "raise tolerance rather than
  throw out baselines" lever. Principled, because the v7 comparison already
  consumes `noise_cv_pct` (σ_diff = √2·CV); a multi-host profile just sets a more
  conservative value.
- **Intentional fork** when comparability *genuinely* breaks — **disk/storage
  class is the headline axis** (block replay + bench warmup are I/O-sensitive;
  see [block-validation-taskspec.md](./block-validation-taskspec.md)), alongside
  CPU-pinning scheme, VM shape, or backend. Forking is an explicit operator act,
  not automatic.
- Profile is a string key, app-validated; adding/forking one is a config change,
  not a migration.

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added
- [ ] Reviewed — Codex signed off
- [ ] Complete

## Phase 5 (optional / later): fairness, admission & cloud provisioner

**Goal:** schedule fairly across kinds, and make workers themselves elastic.

**Scope:**

- **Resource-aware + fair scheduling** (v6 Open-Q#4): capability *and* resource
  matching; a multi-hour block-validation flood can't starve bench (per-kind
  quotas / weighted fairness).
- **Cloud-ephemeral worker provisioner** — the reborn v8 AWS / Hetzner-Cloud work
  ([roadmap-v8.md](./roadmap-v8.md) Phases 3–6): a provisioner spawns ephemeral
  workers that register, run, and deregister. Gated on the cost/variance/hydration
  data that parked it. Bench-only at first (block-val wants local NVMe → bare
  metal; see [block-validation-taskspec.md](./block-validation-taskspec.md)).

**Status:**

- [ ] Design pinned
- [ ] Implementation
- [ ] Reviewed — Codex signed off
- [ ] Complete

## Security

- **Worker auth**: registration tokens (à la GitHub runner registration), then a
  per-worker bearer/mTLS identity on every call.
- **Per-job secrets**: GitHub installation tokens are minted **per job**,
  short-lived, and delivered with the `TaskSpec` over the authenticated channel —
  never long-lived on a worker.
- **Least privilege**: a worker can only act on jobs it was handed (lease-scoped);
  it cannot enumerate or mutate the queue.

## Decisions

1. **`remote-daemon` is a distribution layer, not a `Driver` kind.** It sits above
   the v8 Driver seam; a worker runs the *real* local Driver. (Codex-confirmed.)
2. **v9 is a *split* of `sbgh-daemon`, not parallel logic.** The worker-execution
   substrate (`driver`/`libvirt`/`recipe`/`run_worker`) **moves once** into a
   shared `sbgh-exec` crate; it is never duplicated, and none of it is left behind
   executing in the orchestrator. The orchestrator **never executes a job
   itself**; even single-box installs run a **separate co-located `sbgh-worker`
   process** over loopback (never in-process), keeping one execution model. (User-
   directed.)
3. **Thin pull-based worker API, not shared-Postgres claiming.** The orchestrator
   stays the sole DB client; workers dial out and never touch the DB. (Codex.)
4. **The orchestrator-side reporter is unchanged.** Events + summary arrive over
   the wire instead of mpsc; DB + GitHub side effects stay central.
5. **Capability matching supersedes static per-task-kind backend config** (v8
   Phase 2's `[backend.<kind>]`). Workers advertise what they can run; the
   scheduler routes. Per-job routing — deferred in v8 Decision #6 — is delivered
   here.
6. **Baselines are per `measurement_profile`** (Phase 4) — an **operator-declared**
   comparability label, *not* a per-box fingerprint. Several equalized hosts may
   share a profile (the small-VM-on-big-host strategy is *built* for this);
   comparability is bought with a **per-profile noise floor**, and forked
   intentionally when something that breaks it changes (disk class, pinning, VM
   shape). Prefer a wider profile + higher tolerance over fragmenting baselines.
7. **Cloud is a worker provisioner, not an execution path.** AWS / Hetzner-Cloud
   return as *who spawns a worker* (Phase 5), gated on cost/variance/hydration.

## Sequencing

- **v8 Phase 1 (the `Driver` seam) comes first** — it's the foundation every
  worker runs jobs through. v9 is a no-op without it.
- **Then v9 Phases 1 → 2** are the MVP: a remote worker runs bench end-to-end.
  Phase 3 (liveness) is required before any real multi-host use.
- **Phase 4** (baseline-trust / `measurement_profile`) lands before mixing
  heterogeneous hardware into one installation's baseline timeline.
- **Phase 5** (fairness + cloud provisioner) is later, demand-driven.
- **v9 ⟂ v6.** The task axis (v6) and the distribution axis (v9) are orthogonal;
  block-validation gets the fleet for free once both its `Recipe` (v6) and the
  worker model (v9) exist.
- **The upper stack is reused unchanged** — job engine, reporting, the v7
  comparison; the DB gains the worker registry + the `measurement_profile` column
  only.
