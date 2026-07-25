# Design 0004: Distributed worker fleet (`remote-daemon`, capability-scheduled)

- **id:** `0004-worker-fleet`
- **status:** `planned` (`v25-worker-fleet-block-validation`)
- **depends_on:** `0010-driver-seam` (shipped),
  `0055-execution-boundary-preparation` (v24),
  `0056-compiler-enforced-execution-boundaries` (v24.1)
- **iteration:**
  [`v25-worker-fleet-block-validation`](../iterations/v25-worker-fleet-block-validation.md)
- **review:** Codex signed off (design)
- **source:** roadmap-v9 + v25 dedicated-worker activation

Turns the single-host daemon into an **orchestrator + a fleet of remote worker
daemons**, each declaring **capabilities** (`benchmark`, `block-validation`, …)
and pulling compatible work — the GitHub self-hosted-runner model applied to
benchmark/validation jobs. Dedicated bare-metal boxes (pinned bench hosts,
big-local-NVMe block-validation hosts) and eventual cloud-ephemeral instances all
become **instances of one fleet model**.

**Concrete v25 deployment:** a dedicated Hetzner host is available for the
first remote worker: 64 CPU cores, 256 GB RAM, and four 4 TB NVMe drives. It is
the `block_validation` worker. Existing benchmark execution moves to a separate
co-located loopback worker so the orchestrator has no production inline
execution path. The host's filesystem, NUMA, NVMe layout, safe shard count, and
dataset capacity remain measured Phase 1 inputs—not assumptions derived from
the advertised specifications.

**Goal:** scale concurrency and heterogeneous hardware by **adding workers**, not
by sharing one host. A worker dials *out*, registers its capabilities, long-polls
for compatible jobs, runs them via its **local `Driver`** (the shipped seam,
`0010`), and streams events + artifacts back. The orchestrator stays the **sole DB
client** and owns all GitHub/Slack side effects, including report rendering,
debounce, rate limiting, retries, and reporting-session state. This doc owns the
**distribution layer** — getting jobs *to* capable workers; the task axis is
`0005`/`0019`, the local execution substrate is `0010`, change-impact reporting
is `0009`.

*(Below, "v8/v9/…" are historical roadmap shorthands — resolve via
[index.md](../index.md): v5→`0008`, v6→`0005`/`0019`, v7→`0009`, v8 seam→`0010`,
v8 cloud→`0006`, "v9"=this fleet.)*

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
([`0006`](../backlog.md) Phases 3–6) returns here as a **worker
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
orchestrator-side reporter owns DB + GitHub/Slack side effects (unchanged)
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

## The split: change process placement, don't duplicate logic

v24.1 moves execution ownership out of `sbgh-daemon` before networking: the
driver API lives in `sbgh-driver`, the concrete VM adapter lives in
`sbgh-libvirt`, and dispatch/recipes/run-loop logic lives in the in-process
`sbgh-worker` library. v25 adds the worker binary and transport, then removes
the daemon's transitional `sbgh-worker` and `sbgh-driver` dependencies. No
execution implementation is copied or retained in the orchestrator.

**Where each of today's `sbgh-daemon` modules goes:**

| Module(s) | Destination |
| --------- | ----------- |
| `api/`, `webhook_processor`, `reporter`, `progress`, `comparison`, `bench_summary`, `job_source` | **orchestrator** (keeps DB ownership + all GitHub/Slack side effects) |
| `runner` coordinator (claim/lease/slots/`recover_orphans`) | **orchestrator** → grows into the scheduler + worker registry + the worker-API server |
| execution request, recipes, dispatch, and run loop | **`sbgh-worker`** |
| `Driver`, task/context/outcome, and event ports | **`sbgh-driver`** |
| `libvirt/` and backend-owned configuration | **`sbgh-libvirt`** |
| `events` | **splits at the existing producer/consumer seam** — see below |

**Proposed crate structure (one impl each, no duplication):**

- **`sbgh-driver` (added in v24.1)** — dependency-light internal
  driver/task/event API. It has no concrete backend, daemon model, wire DTO, or
  infrastructure client.
- **`sbgh-libvirt` (added in v24.1)** — the concrete pinned/libvirt benchmark
  adapter and its backend-owned configuration.
- **`sbgh-worker` (library added in v24.1; binary added in v25)** — execution
  dispatch, recipes, run loop, cache/artifact service composition, then the
  register/long-poll/event/upload transport shell.
- **`sbgh-proto` (new, dependency-light library)** — owned, versioned worker
  wire DTOs for execution/task context, events/terminal outcomes, registration,
  capabilities, claim/lease, and artifact results. It does not expose core/DB
  structs or depend on the HTTP client implementation.
- **Wire contract payload** — versioned equivalents of task context,
  task/event/terminal outcomes, registration/capability/claim/lease messages,
  and artifact results. Each side validates and converts these DTOs at its
  boundary; internal driver types are not serialized implicitly.
- **`sbgh-daemon` / orchestrator (existing binary)** — `api`, `webhook_processor`,
  the scheduler/registry, the reporter, and the worker-API server. Sole DB
  client and GitHub/Slack side-effect owner. It alone holds Slack credentials
  and reporting clients. **Never executes a job itself** — execution only
  happens in an `sbgh-worker`.

**The `events.rs` seam is where the split runs.** `EventSink` (the *producer*) is
worker-side; the **reporter** (the *consumer*) is orchestrator-side. The network
does not merely replace mpsc: reliable task-neutral events are first committed
to the durable attempt ledger from
[`0017`](0017-generic-phase-events.md), then projected/replayed by the reporter.
The roadmap-v5 reliable/best-effort discipline carries across the wire without
pretending every heartbeat/progress sample is durable.

**Single-box deployments — a co-located, but always *separate*, `sbgh-worker`
(decided).** Even on one host, the worker is a **separate `sbgh-worker` process**
talking the same API over **loopback** — never an in-process worker inside the
orchestrator. The orchestrator does **only** orchestration/reporting and
**never executes a job itself**. This keeps exactly one execution model (local is
just `localhost`, byte-identical to remote), gives crash isolation (a worker
panic on a long or fan-out job can't take down the orchestrator), and avoids a
special in-process path that would re-fork the very thing the split consolidates.
A one-box install therefore runs two processes — orchestrator + `sbgh-worker` —
both built from this repo, with execution implemented once in the
`sbgh-worker` library and its selected adapter crates.

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

**v25 protocol upgrades are coordinated, not rolling.** Worker and orchestrator
must report the same protocol version. Operators drain/stop workers, upgrade
both sides, and restart them as one compatibility set. Supporting an explicit
version-skew window is a later design change, not implied by versioned DTOs.

## Phase 1: Worker protocol + registry (control plane)

**Goal:** a worker can register, advertise capabilities, long-poll, and be handed
a job — with a **stub task** (no real Driver yet). Prove the protocol on a single
**loopback worker** (worker process on the same host as the orchestrator) before
any network/firewall concerns.

> **Transition note (Codex).** Phase 1 is **control-plane-only / non-production**:
> it proves the protocol with *stub* work. The orchestrator's existing **in-process
> execution stays for production** through Phase 1 — the "orchestrator never
> executes" end state (above) is reached only in **Phase 2**, when the existing
> v24.1 worker library is hosted by the `sbgh-worker` binary and the daemon's
> in-process library edge is removed.

**Scope:**

- Add the `sbgh-worker` binary around the existing worker library: config =
  `{ accepted_tasks,
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

- Host the existing v24.1 `sbgh-worker` library in the worker binary and remove
  `sbgh-daemon`'s in-process worker and direct driver-API dependencies after
  loopback parity. The transport/artifact/event adapters are new data-plane
  work; the driver API, recipes, and libvirt are reused unchanged.
- Worker invokes its configured local
  **`Driver::run_task(TaskSpec)`** implementation from `sbgh-libvirt` or the
  block-validation adapter selected by its advertised capability.
- **Event ingest**: worker POSTs reliable task-neutral events with
  attempt-scoped sequence numbers; orchestrator ingest commits them durably
  before acknowledgement, then the reporter projects/replays them per
  [`0017`](0017-generic-phase-events.md). Best-effort progress cannot create a
  reliable-sequence gap.
- **Artifact upload**: the worker stages `archive_dir` (tarball) or an
  object-store pointer + the `summary` blob under its attempt. Only an accepted,
  fenced terminal promotes/attaches those store keys; rejected/stale attempts
  are invisible to consumers and GC-reclaimable. **Reporter/DB/vs-baseline path
  remains orchestrator-owned.**
- A loopback benchmark worker first; then the dedicated **Hetzner
  block-validation worker** as the real second-host test.
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
- **Single-capability hold:** if no other online worker can satisfy the task, a
  fenced attempt is held with an operator alert rather than silently requeued in
  a loop. Cleanup/recovery or an explicit operator abandonment decision gates a
  successor attempt.
- **Graceful drain**: a worker stops claiming, finishes its current job,
  deregisters (for planned host maintenance / ephemeral-worker teardown).
- **Event resume**: sequence numbers let a reconnecting worker resume its stream
  without duplicating side effects.
- **Attempt artifact GC:** staged artifacts from fenced/expired attempts without
  an accepted terminal are reclaimed after an auditable grace period; attached
  result artifacts are never swept by this path.

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
  see [`0019`](0019-block-validation-recipe.md)), alongside
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
  ([`0006`](../backlog.md) Phases 3–6): a provisioner spawns ephemeral
  workers that register, run, and deregister. Gated on the cost/variance/hydration
  data that parked it. Bench-only at first (block-val wants local NVMe → bare
  metal; see [`0019`](0019-block-validation-recipe.md)).

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
  it cannot enumerate or mutate the queue. It has no Slack credential or
  client, and any GitHub token is limited to repository access rather than
  reporting.

## Decisions

1. **`remote-daemon` is a distribution layer, not a `Driver` kind.** It sits above
   the v8 Driver seam; a worker runs the *real* local Driver. (Codex-confirmed.)
2. **v9 changes process placement, not execution ownership.** v24.1 already
   establishes `sbgh-driver`, `sbgh-libvirt`, and the `sbgh-worker` library.
   v25 hosts that library in a separate process and removes the daemon's
   transitional worker and direct driver-API edges; it does not create a
   parallel executor. Even single-box installs use a co-located `sbgh-worker`
   process over loopback. (User-directed.)
3. **Thin pull-based worker API, not shared-Postgres claiming.** The orchestrator
   stays the sole DB client; workers dial out and never touch the DB. (Codex.)
4. **The orchestrator-side reporter is unchanged.** Events + summary arrive over
   the wire instead of mpsc; DB + GitHub/Slack side effects, credentials,
   debounce, rate limiting, retries, and reporting-session state stay central.
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
