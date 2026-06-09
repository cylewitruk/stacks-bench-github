# Roadmap 10 — Slack interactions & ad-hoc profiling benchmarks

Adds **Slack** as a first-class way to trigger and read benchmarks, for the
**ad-hoc, no-commit** case: *"profile this tx / block from yesterday that was
slow."* No PR, no commit under test — the code is a constant (a configured
default rev), the **workload** is the variable (`--txid` / `--block`), and the
deliverable is a **flamegraph**, not a vs-baseline delta.

> **Goal:** let someone in Slack type `/bench --block 184231 --repetitions 5` (or
> `--txid 0xabc…`) and get back a flamegraph for that workload — reusing the
> existing bench execution path end-to-end. Slack is a **new trigger source** and
> a **new reporting surface**; the job is the **existing bench Recipe** with
> ad-hoc workload args. No new task kind.

Process unchanged: Opus implements, Codex reviews, Opus fixes.

> **Sibling docs:** the **local `Driver` seam** this rides is
> [roadmap-v8.md](./roadmap-v8.md) (Phase 1, landed); the **distributed worker
> fleet** is [roadmap-v9.md](./roadmap-v9.md); the **multi-task platform** is
> [roadmap-v6.md](./roadmap-v6.md); change-impact reporting is
> [roadmap-v7.md](./roadmap-v7.md). This doc owns the **Slack surface + ad-hoc
> profiling job**.

## Why

- **The team lives in Slack**, and slow-tx/slow-block investigations are ad-hoc —
  they don't start from a PR. Forcing a commit-shaped flow (open a branch, push,
  `/benchmark`) just to profile a known-slow tx is friction.
- **`stacks-bench` already supports it.** It accepts `--txid` and `--block`
  (each repeatable; the two are mutually exclusive) plus `--repetitions N` (how
  many times to execute each, atop `--warmup` — replacing `--count`). So the
  "what to bench" is a **solved capability**: it's a bench-args parameterization,
  not new task work.
- **It validates the surface abstractions.** Slack is a second trigger *source*
  and a second reporting *surface*; building it generalizes the
  GitHub-coupled reporter behind a surface seam — reusable for anything later.

## What's new vs. reused

| Concern | This roadmap |
| ------- | ------------ |
| Execution | **Reused** — the bench `Recipe` + v8 `Driver` seam, unchanged |
| Workload | **Reused** — `bench_args` / `workload_key`; the ad-hoc workload is `--txid`/`--block`/`--repetitions` |
| Code under test | **New** — default rev (config), *decoupled* from the workload |
| Trigger source | **New** — Slack (Socket Mode), alongside GitHub webhooks |
| Reporting surface | **New** — `ProgressTarget::Slack`; the reporter generalized behind a surface trait |
| Result shape | **New** — a **flamegraph artifact**, absolute (no vs-baseline delta) |

## The decoupling: code-under-test ≠ workload

sbgh's job is **commit-anchored** today (repo + rev → build → replay the default
smoke workload). The ad-hoc case inverts it:

- **Code = a constant.** `[slack].default_repository` + `[slack].default_rev`
  (a branch/tag/sha, resolved to a commit by the *existing* claim-time
  resolution path), optionally overridable via a `--rev` flag.
- **Workload = the variable.** `--txid` / `--block` (repeatable, mutually
  exclusive) + `--repetitions` — passed straight through as the job's
  `bench_args`.
- **No vs-baseline.** An ad-hoc workload gets its own `workload_key`, so it
  simply won't match a baseline — which is *correct*: the user wants absolute
  "how slow is this," delivered as a flamegraph, not a delta vs `develop`.

So a Slack job is an ordinary `RunnableJob` — default repo/rev (→ commit),
ad-hoc `bench_args`, a `ProgressTarget::Slack`, baseline comparison off.

## Transport: Socket Mode

A **Socket Mode** app (an outbound WebSocket the orchestrator opens to Slack,
authenticated by an app-level `xapp-` token) — not a public Request URL.
Rationale: internal/single-workspace, **no new public inbound surface**, and it
dials *out* (firewall/NAT-friendly — same philosophy as the v9 worker model).

- **Auth:** the app-level token authenticates the *connection*; a bot `xoxb-`
  token authorizes Web API calls (post/update/upload). In Socket Mode there's
  **no per-message HMAC** — the socket itself is authenticated, so the
  signing-secret verification (the analog of the GitHub webhook HMAC) is only
  needed *if* we ever switch to an HTTP Request URL.
- **The WebSocket token is *not* the authorization story (Codex).** Every command
  must still validate **`team_id` / enterprise / workspace** *and* the Slack user
  against the config allowlist (Phase 1) before acting — the connection being
  authenticated says nothing about *who* sent a given command. Tie the per-command
  check here so it isn't treated as an afterthought.
- **Ack discipline:** each socket envelope must be **acked within 3 s**; the
  actual work (enqueue + reporting) happens async, exactly like the
  ack-then-update pattern the slash-command HTTP flow would use.

## Phase 0: Slack app + Socket Mode connectivity spike

**Goal:** de-risk the Slack plumbing before any sbgh wiring.

**Scope:**

- Register the Slack app; scopes: `commands`, `chat:write`, `files:write` (for
  the flamegraph), optionally `app_mentions:read`. Mint the app-level token
  (`connections:write`) + bot token.
- Prove end-to-end by hand: open a Socket Mode connection, receive a `/bench`
  slash-command envelope, **ack within 3 s**, and `chat.postMessage` a reply.
- Decide the v1 entry surface: **slash command** (recommended) vs. a modal vs.
  `@sbgh` mention.
- **`stacks-bench` artifact spike (Codex) — the other half of the de-risk.** Run
  the intended ad-hoc command (`--txid`/`--block` + `--repetitions`) locally / in
  the VM and confirm: which **profiler flags** produce a flamegraph, **what
  file(s)** are emitted, and **how they surface** in `run.json` / the archive.
  The whole goal rests on this producing the artifact we expect — a tiny spike,
  big de-risk.

**Status:**

- [ ] Spike complete — connection + slash command + reply proven
- [ ] `stacks-bench` ad-hoc command produces the expected flamegraph artifact
- [ ] Reviewed — Codex signed off

## Phase 1: Slack connector + ad-hoc job enqueue

**Goal:** `/bench …` in Slack enqueues a job and acks with a "queued" message.

> **Not deployable alone (Codex).** Phase 1 is an internal slice — Slack enqueue
> stays **disabled/behind config** until Phase 2 adds terminal reporting, so a
> job is never shipped to users without a result path. The MVP is Phases 1 **+**
> 2 together.

**Scope:**

- A `slack` connector (orchestrator-side — a new concurrent task in `main`'s
  `try_join!`, alongside the runner + webhook processor + api): the Socket Mode
  client loop, with reconnect/backoff.
- Parse the `/bench` command text → workload: `--txid` / `--block` (repeatable,
  mutually exclusive), `--repetitions`, `--warmup`, optional `--rev`.
- **Authz:** a Slack allowlist in config (workspace + user ids) → permission to
  enqueue against the default repo. (Distinct from the GitHub multi-tenant
  model — Slack identities are their own allowlist.)
- Enqueue a `RunnableJob`: repo/rev from `[slack]` config, `bench_args` = the
  parsed workload, `ProgressTarget::Slack { channel, message_ts }`, baseline off.
- Ack: post a "⏳ queued" `chat.postMessage` and capture its `ts` for later
  `chat.update`s.

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (a fake Slack client → command parses + enqueues)
- [ ] Reviewed — Codex signed off
- [ ] Complete

## Phase 2: The ad-hoc / no-commit job shape

**Goal:** the decoupling above, as a real job kind, reusing the bench path.

**Scope:**

- New `trigger_kind = slack_adhoc`; `[slack].default_repository` /
  `default_rev` config; the workload args flow through as `bench_args`.
- Commit resolution reuses the existing claim-time branch/tag→commit path.
- Baseline comparison (roadmap-v7) is **disabled for this kind** — absolute
  results only, no `find_baseline` call.
- Execution is the **unchanged** bench `Recipe` + `Driver` (v8) — a Slack job
  runs exactly like a PR bench, only its trigger + report surface differ.
- **Minimal terminal Slack reporting — so the MVP actually finishes a job
  (Codex).** A Slack job needs *someone* to post its result, so this phase adds a
  small **Slack branch at terminal** in the reporter: for `ProgressTarget::Slack`
  it `chat.update`s the queued ack into a result message (absolute timing text).
  This is a thin inline post, **not** the generalized surface trait — that, plus
  *live* phase progress, is Phase 3. So Phases 1+2 deliver a working text bench
  (queued → result); no job is ever enqueued without a reporting path.

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added
- [ ] Reviewed — Codex signed off
- [ ] Complete

## Phase 3: Reporting-surface generalization + live Slack progress

**Goal:** replace Phase 2's minimal Slack branch with a clean surface seam, and
add **live** phase progress.

**Scope:**

- Extract a **`ReportSurface`** trait (post → update-progress → finalize) from
  the reporter ([reporter.rs](../crates/sbgh-daemon/src/reporter.rs) /
  [progress.rs](../crates/sbgh-daemon/src/progress.rs)). **GitHub impl preserves
  today's behavior** (check runs + comments); the **Slack impl** subsumes Phase
  2's inline terminal post and adds `chat.update` progress.
- `ProgressTarget::Slack` routes to the Slack surface; phases **stream**
  (queued → building → running → done), debounced like the PR comment — the
  upgrade over Phase 2's terminal-only post.
- This is the one structural change; it's reusable for any future surface and is
  orchestrator-side (composes with v9, which keeps the reporter central).

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added (the existing GitHub reporter tests stay green)
- [ ] Reviewed — Codex signed off
- [ ] Complete

## Phase 4: Flamegraph artifact pipeline (the payoff)

**Goal:** deliver the flamegraph for the profiled workload into the Slack thread.

**Scope:**

- Capture `stacks-bench`'s profiler/flamegraph output in the VM; add it to the
  **artifact manifest** so the driver pulls it into the run bundle next to
  `run.json` (the v8 artifact path, unchanged in shape).
- **Depends on the artifact store ([roadmap-v12.md](./roadmap-v12.md)).** The
  bundle ships to object storage; Slack delivery is then either a **signed-URL
  link** (v12 issues it — simplest) or a file **upload** via Slack's current
  external-upload flow (`files.getUploadURLExternal` →
  `files.completeUploadExternal`; the old `files.upload` is sunset). **(Decision:
  upload vs. signed link — see below.)**
- Render the result: absolute per-tx/per-block timing + the flamegraph; **no**
  vs-baseline delta.

**Status:**

- [ ] Initial implementation completed
- [ ] Integration coverage added
- [ ] Reviewed — Codex signed off
- [ ] Complete

## Phase 5 (optional): richer UX

**Goal:** lower the syntax barrier + add affordances.

**Scope:**

- A **modal** (`views.open` off the slash command's `trigger_id`) with a form
  for tx/block/repetitions/rev, instead of memorized flags.
- **Buttons** on a result: "Re-run", "Profile again with more repetitions".
- `@sbgh` **mention** invocation + threaded results.

**Status:**

- [ ] Design pinned
- [ ] Implementation
- [ ] Reviewed — Codex signed off
- [ ] Complete

## Decisions

1. **Slack is a trigger source + reporting surface, not a new task kind.** It's a
   new **`trigger_kind`** (`slack_adhoc`) but the **same bench task/`Recipe`** —
   `stacks-bench`'s `--txid`/`--block`/`--repetitions` makes the ad-hoc workload
   a `bench_args` parameterization on the existing `Recipe`, so execution is
   reused wholesale. (New trigger kind ≠ new task kind.)
2. **Socket Mode, not a public Request URL.** Outbound WS, app-level token, no
   new inbound surface. The HMAC signing-secret path applies only if we later
   move to HTTP.
3. **Ad-hoc jobs are absolute profiling, not vs-baseline.** The code is a
   constant (default rev); the variable is the workload; the deliverable is a
   flamegraph. The ad-hoc `workload_key` won't match a baseline anyway.
4. **Orchestrator-side only.** The Slack connector + reporting never touch
   execution — they compose with v9 (execution rides workers; Slack I/O stays in
   the orchestrator).
5. **Reporting is generalized behind a `ReportSurface` trait.** GitHub + Slack
   are two impls selected by `ProgressTarget`; existing GitHub behavior preserved.

## Sequencing & relationship to the other roadmaps

- **Rides v8 Phase 1 (done); independent of v6 and v9.** It runs on the current
  single-host bench path now, and inherits the v9 fleet later for free (a Slack
  job is just a bench job a worker runs).
- **Phase 4 depends on the artifact store ([roadmap-v12.md](./roadmap-v12.md))** —
  build that first; the flamegraph ships to object storage and Slack links it.
- **MVP = Phases 1 + 2** — a working text bench end-to-end (queued ack →
  **terminal result**, via Phase 2's minimal Slack reporting). **Phase 3** adds
  live phase progress + the generalized `ReportSurface`; **Phase 4** (flamegraph)
  is the real payoff.
- **Phase 3 is orthogonally useful** — any future report surface (a status page,
  email, …) reuses the `ReportSurface` seam.

## Open questions (for Codex / decisions to pin)

1. **Flamegraph delivery:** Slack file upload (renders inline, but needs
   `files:write` + the upload dance) vs. a **signed-URL link** to the artifact in
   object storage ([roadmap-v12.md](./roadmap-v12.md), simpler). Lean: link first,
   upload as a Phase-4 polish.
2. **Authz + cost control.** A profiling run is expensive (full VM + replay).
   What's the allowlist granularity (workspace / channel / user), and do we need
   per-user rate limits or a confirm step?
3. **Queue/host sharing.** Ad-hoc Slack profiles share the bench host + queue
   with PR benches — contention/fairness (this is the v6 Open-Q#4 / v9 fairness
   thread surfacing again). Same queue, or a separate lane?
4. **Retention.** How long are archived flamegraphs kept, and where are they
   served from for the Slack link?
