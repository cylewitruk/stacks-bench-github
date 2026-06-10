# v5: Slack ad-hoc profiling

Successor to [v4-artifact-store](v4-artifact-store.md). Add **Slack** as a
first-class way to trigger and read benchmarks for the **ad-hoc, no-commit**
case — *"profile this tx/block from yesterday that was slow"* — reusing the
existing bench execution path. Slack is a new **trigger source** + new
**reporting surface**; the job is the same bench `Recipe` with ad-hoc workload
args (`--txid`/`--block`/`--repetitions`), and the payoff is a **flamegraph**,
not a vs-baseline delta.

> **Status:** planned
>
> Promoted from backlog 2026-06; full design (Codex-signed-off) in
> [design/0002-slack-adhoc-profiling.md](../design/0002-slack-adhoc-profiling.md).
> Rides the shipped `Driver` seam (`0010`); its flamegraph delivery consumes the
> v4 artifact store (`0001`, code-complete). Independent of the worker fleet
> (`0004`) and task-kind platform (`0005`).

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0002-slack-adhoc-profiling` | primary | planned |

## Why

The team lives in Slack and slow-tx/slow-block investigations are **ad-hoc** —
they don't start from a PR, so forcing a commit-shaped flow (branch, push,
`/benchmark`) just to profile a known-slow tx is friction. `stacks-bench` already
accepts `--txid`/`--block` (repeatable, mutually exclusive) + `--repetitions`, so
"what to bench" is a solved capability — this is a `bench_args` parameterization,
not new task work. Building it also **generalizes the GitHub-coupled reporter**
behind a surface seam, reusable for any later surface.

## Scope

A Socket-Mode Slack connector with an **`@sbgh` mention** entry surface, a
`slack_adhoc` trigger (default repo/rev from config, ad-hoc workload), a
generalized `ReportSurface`, and a flamegraph artifact delivered into Slack. The
**code under test is a constant** (configured default rev) and the **workload is
the variable** — so an ad-hoc job is an ordinary `RunnableJob` with
`ProgressTarget::Slack`, baseline comparison off. Details + transport rationale
(Socket Mode, ack discipline, authz) live in the design doc.

**Entry surface = `@sbgh` mention** (an `app_mention` event), *not* a slash
command — chosen so all reporting can thread under the user's own request
message (a slash command leaves no channel message to thread on) and because a
free-text message is the natural home for the future LLM intent resolver. A
slash command could be added later as a secondary convenience.

## Cross-cutting: intent resolution (surface ⟂ resolution ⟂ execution)

Keep three concerns separate so the workload front-end can evolve without
touching execution:

```text
request text ─▶ resolve_workload() ─▶ validated WorkloadSpec ─▶ bench_args
                (v1: deterministic flag parser; future: LLM resolver, 0020)
```

- **v1 is a deterministic parser** of `--txid`/`--block`/`--repetitions`/
  `--warmup`/`--rev`; the **future LLM resolver ([`0020`](../index.md)) plugs in
  behind the same seam** (raw text → structured spec).
- **Authz runs *before* resolution** — an off-allowlist user is rejected without
  spending resolution (or, later, an LLM call).
- **Validation runs *after* resolution** — the resolver (parser or LLM) emits the
  **structured** `WorkloadSpec` (txid xor block, repetitions range, …), which the
  **same deterministic validator** checks. A resolver never emits raw `bench_args`
  directly, so an LLM can't inject arbitrary flags.
- A resolver that is **uncertain** (future LLM) asks a clarifying question
  in-thread / ephemerally — the threaded model already supports it.

## Cross-cutting: threaded reporting (anti-spam)

**All bot output threads under the user's own request message** — there is **no
bot-posted parent**. The user's `@sbgh bench …` mention *is* the single
channel-level message; everything else (progress, result, flamegraph, db link)
is a **threaded reply** under it (`thread_ts` = the request's `ts`). The channel
shows just the request + a tidy thread, even when several profiles run at once.

- **Thread root = the request.** A mention surfaces as an `app_mention` event
  with a `ts`; that `ts` is the thread anchor (and what
  `ProgressTarget::Slack { channel, message_ts }` captures — here `message_ts` is
  the *request's* ts, not a bot message's).
- **Lifecycle status** (`⏳ queued → 🏗 running → ✅ done / ❌ failed`) is shown
  **on the request itself** via emoji **reactions** (`reactions.add`/`remove`) —
  zero extra messages. **Pinned** (not a threaded status reply).
- **Thread** carries the substance — the terminal absolute-timing result, the
  flamegraph, and (S3 only) a `stacks-bench.db` download link in the result
  footer (Phase 4). Live phase progress (Phase 3) updates a **single** in-thread
  message (debounced), not one reply per phase. An artifact that lands *after*
  the result (async/retried upload) is **one** follow-up threaded reply — still
  in-thread, never a new channel message.
- **Rejections — ephemeral, no channel message.** An auth-denied or
  malformed/unresolvable request gets a **`chat.postEphemeral`** reply (visible
  only to the invoker) and creates **no** thread output — rejections never
  clutter the channel or the thread. (Pinned per Codex review; `postEphemeral` is
  the mention-surface analog of a slash command's ephemeral response.)

This is a first-class requirement from Phase 1 onward.

## Phases

> **⚠ Sequencing gate (Codex, 3c review).** Two things must be green before the
> live Socket Mode receive loop is enabled, so an accepted Slack job both *runs*
> and *reports back*:
>
> 1. **rev→commit resolution for `ProgressTarget::Slack`** — ✅ **done.** The
>    reporter's `prepare` now resolves a Slack job's rev (bare branch/tag/SHA)
>    via `resolve_commit`, so it no longer fails the empty-commit guard
>    (`run_once_resolves_slack_rev_commit_in_preflight`).
> 2. **terminal threaded reporting** — ✅ **done.** The reporter's terminal path
>    (`ProgressReporter::{completed,failed,cancelled}`) now posts a Slack job's
>    result as a threaded reply (`thread_ts` = the request ts) and swaps ⏳ →
>    ✅/❌ via the `SlackClient`
>    (`progress::tests::slack_{completed,failed,cancelled}_*`). The client is
>    injected through `Runner::with_slack`; absent, the surface is a silent
>    no-op.
>
> **Both gate items are now green.** The remaining wiring (3d) is sub-sliced:
>
> - **3d-1 — `default_repository` → `(install, repo)` resolution** — ✅ **done**
>   (token-independent). `slack::target::resolve_target` mirrors `/api/resolve`'s
>   two-row DB lookup (active install on the account + materialised `github_repo`
>   for `owner/name`), startup-fatal on misconfiguration
>   (`tests/slack_target.rs`).
> - **3d-2 — real Socket Mode adapter** — ✅ **done.** `slack-morphism`
>   (`hyper` feature) owns the WS transport; `slack::socket::mention_from_callback`
>   maps an `app_mention` push → `MentionEvent` and `slack::api_client::WebApiClient`
>   (reqwest) is the real `SlackClient`. Tested without a live socket: a parsed
>   envelope dispatched through the connector enqueues a job + reacts ⏳
>   (`socket::tests::parsed_mention_dispatched_through_connector_enqueues_job`).
> - **3d-3 — `main` wiring** — ✅ **done.** Behind `[slack].enabled`: resolve
>   the target (startup-fatal), build the one Web API client shared by the
>   reporter (`Runner::with_slack`) and the socket connector, and add the
>   receive-loop as a `try_join!` arm that drains on shutdown and never crashes
>   the daemon on a Slack-side failure (optional surface).
>
> **All slices are green.** The only remaining step is operational: the user
> provides `SBGH_SLACK_APP_TOKEN`/`SBGH_SLACK_BOT_TOKEN`, flips
> `[slack].enabled = true`, and runs a live smoke test (`@sbgh bench …`).

### Phase 0: Slack app + Socket Mode + artifact spike (de-risk)

**Goal:** prove the Slack plumbing and the flamegraph artifact *before* any sbgh
wiring.

**Scope:**

- Register the app: scopes `app_mentions:read` (the entry surface), `chat:write`
  (post results + ephemeral rejections), `reactions:write` (status reactions);
  subscribe to the `app_mention` event over the socket. Mint the app-level
  (`connections:write`) + bot tokens. *(No `commands` scope — slash is not v1.)*
  As-built manifest: `docs/slack-app-manifest.yaml`. `files:write` is **not**
  registered — it's only needed if a later polish uploads the DB/flamegraph as a
  Slack file rather than posting a presigned-URL link (the chosen approach).
- By hand: open a Socket Mode connection, receive an **`app_mention`** envelope,
  **ack within 3 s**, post a **threaded** reply (`thread_ts` = the mention `ts`)
  and add a status reaction. (Entry surface is **decided: mention** — see Scope.)
- **`stacks-bench` artifact spike:** run the intended ad-hoc command locally / in
  the VM and confirm which **profiler flags** produce a flamegraph, **what
  file(s)** are emitted, and **how they surface** in `run.json` / the archive.

**Status:**

- [ ] Spike: socket connection + `app_mention` + threaded reply + reaction proven
- [ ] `stacks-bench` ad-hoc command produces the expected flamegraph artifact
- [ ] Reviewed (Codex)

**Acceptance & Validation:**

- [ ] A hand-driven `@sbgh bench …` round-trips (ack < 3 s, threaded reply +
  status reaction post) — manual spike.
- [ ] The profiler flag set + emitted flamegraph file path are documented (feeds
  Phase 4's manifest entry) — recorded in the design doc / this iteration.

**Notes:** Spike only — throwaway is fine; the point is to retire the two
biggest unknowns (Socket Mode behavior, flamegraph artifact shape).

### Phase 1: Slack connector + ad-hoc enqueue (behind config)

**Goal:** `@sbgh bench …` enqueues a job and acks on the **request** (status
reaction); no bot parent message.

> **Not deployable alone.** Phase 1 stays disabled/behind config until Phase 2
> adds terminal reporting — a job is never enqueued without a result path. The
> MVP is Phases 1 **+** 2.

**Scope:**

- A `slack` connector: a new concurrent task in `main`'s `try_join!` (alongside
  runner + webhook processor + api) — the Socket Mode client loop with
  reconnect/backoff, consuming `app_mention` envelopes (ack each within 3 s).
- **Authz first:** a Slack allowlist in config (workspace/`team_id` + user ids),
  validated **per mention** (the authenticated socket says nothing about *who*
  sent it). Distinct from the GitHub multi-tenant model.
- **Resolve then validate:** `resolve_workload(text)` → `WorkloadSpec` (v1: the
  deterministic flag parser — `--txid`/`--block` mutually exclusive,
  `--repetitions`/`--warmup`/`--rev`) → the shared validator. The
  intent-resolution seam (above) so the LLM resolver ([`0020`](../index.md)) drops
  in later.
- **Accept/reject split (pinned):** on authz or resolve/validate failure,
  `chat.postEphemeral` the reason to the invoker and stop — **no job, no channel
  or thread message**. Only an accepted request proceeds.
- Enqueue a `RunnableJob`: repo/rev from `[slack]` config, `bench_args` from the
  `WorkloadSpec`, `ProgressTarget::Slack { channel, message_ts }` where
  `message_ts` is the **request's** ts (the thread anchor), baseline off.
- Ack on the request: add a `⏳` **reaction** (the lifecycle-status surface) — no
  bot parent message is posted.

**Status:**

- [ ] Initial implementation
- [ ] Integration coverage (fake Slack client → mention parses + enqueues; status
  reaction added; zero `chat.postMessage`)
- [ ] Reviewed (Codex)

**Acceptance & Validation:**

- [ ] A valid `@sbgh bench …` enqueues exactly one `RunnableJob` with the resolved
  `bench_args` and `ProgressTarget::Slack` anchored on the **request ts**, and
  posts **no** channel message (only a reaction) — fake-Slack-client test.
- [ ] An off-allowlist `team_id`/user gets a **`chat.postEphemeral`** rejection,
  **no channel/thread message**, no job — authz test (assert ephemeral + zero
  `chat.postMessage`).
- [ ] An unresolvable/invalid request (e.g. `--txid` + `--block` together) gets an
  ephemeral error, no job — resolve/validate test.

**Tests:** `crates/sbgh-daemon/tests/…` with a fake Socket-Mode/Web-API client.

### Phase 2: Ad-hoc job shape + minimal terminal reporting (MVP)

**Goal:** the no-commit job shape as a real `trigger_kind`, reusing the bench
path, finishing with a **threaded** result. Phases 1+2 = a working text bench.

**Scope:**

- New `trigger_kind = slack_adhoc`; `[slack].default_repository`/`default_rev`;
  workload args flow through as `bench_args`. Commit resolution reuses the
  existing claim-time branch/tag→commit path.
- Baseline comparison (`0009`) **disabled** for this kind — absolute only, no
  `find_baseline` call.
- Execution is the **unchanged** bench `Recipe` + `Driver` — only trigger +
  surface differ.
- **Minimal terminal Slack reporting:** a thin Slack branch at terminal in the
  reporter — for `ProgressTarget::Slack` it posts the result (absolute timing
  text) as a **threaded reply** under the request and swaps the request's status
  **reaction** to ✅/❌. (Not yet the generalized surface trait — that's Phase 3.)

**Status:**

- [ ] Initial implementation
- [ ] Integration coverage
- [ ] Reviewed (Codex)
- [ ] Validated

**Acceptance & Validation:**

- [ ] A `slack_adhoc` job runs end-to-end (reaction ack → result) on the bench
  path, with no `find_baseline` call — integration test over the runnable-job
  lifecycle with a fake Slack client.
- [ ] The result posts **in-thread** (`thread_ts` = the request `ts`) and the
  request's status reaction is swapped to terminal — assert the reply's
  `thread_ts` + the `reactions.add`/`remove`.
- [ ] A failed/cancelled `slack_adhoc` job posts a threaded failure + ❌ reaction
  — failure-path test.

**Tests:** extend the job-lifecycle/reporter suites with a `slack_adhoc` job +
fake Slack client.

### Phase 3: `ReportSurface` generalization + live threaded progress

**Goal:** replace Phase 2's inline Slack branch with a clean surface seam, and
stream live phase progress in-thread.

**Scope:**

- Extract a **`ReportSurface`** trait (post → update-progress → finalize) from
  [reporter.rs](../../crates/sbgh-daemon/src/reporter.rs) /
  [progress.rs](../../crates/sbgh-daemon/src/progress.rs). The **GitHub impl
  preserves today's behavior** (check runs + comments); the **Slack impl**
  subsumes Phase 2's terminal post and adds live progress.
- `ProgressTarget::Slack` routes to the Slack surface; phases **stream** (queued
  → building → running → done) by `chat.update`ing a **single in-thread progress
  message** (debounced like the PR comment), while the request's **reaction**
  tracks coarse status. One structural change; reusable for any future surface;
  orchestrator-side.

**Status:**

- [ ] Initial implementation
- [ ] Integration coverage (existing GitHub reporter tests stay green)
- [ ] Reviewed (Codex)
- [ ] Validated

**Acceptance & Validation:**

- [ ] **GitHub reporting is unchanged** — the existing reporter/progress suites
  stay green (the behavior-preserving guard, like v4 Phase 1).
- [ ] A Slack job streams ≥2 phase updates to a **single** in-thread message
  (not N new replies), debounced — fake-client test asserting one progress `ts`
  is `chat.update`d, not re-posted.

**Tests:** `ReportSurface` unit tests (both impls) + the regression GitHub suites.

### Phase 4: Flamegraph artifact pipeline (the payoff)

**Goal:** deliver the workload's flamegraph into the Slack thread.

**Scope:**

- Capture `stacks-bench`'s flamegraph output in the VM; add it to the **artifact
  manifest** so the driver pulls it into the run bundle next to `run.json` (the
  v8 artifact path, unchanged in shape) — keyed via `artifact_key`.
- **Consumes the artifact store (`0001`).** The bundle is already in the store;
  Slack delivery is a **signed-URL link** (default — `S3Store::signed_url`, the
  simplest; falls back to an authenticated download endpoint in local mode per
  Decision 0001) or a **file upload** (`files.getUploadURLExternal` →
  `files.completeUploadExternal`; `files.upload` is sunset) as a polish.
- **`stacks-bench.db` download link (S3 only).** When `kind = "s3"`, append a
  presigned link to the run's SQLite db (`artifact_key(job, SQLITE_RELATIVE)`) at
  the **bottom of the threaded result summary** — for pulling the raw run into
  the local explorer. Gate it on `store.exists(db_key)` (a live S3 HEAD) so the
  link only appears once the object is actually in the bucket. In `local` mode
  there's no such link (`signed_url → Unsupported`, Decision 0001). This + the
  flamegraph are the consumers that retire the store's `signed_url`/`exists`
  `allow(dead_code)`.
  - **Timing fallback (forward-compatible).** Today's upload is synchronous
    (inside the driver's archive `put`), so by terminal-report time `exists` is
    authoritative and the link goes in the summary footer. If a later model
    uploads asynchronously / retries (worker fleet `0004`, Decision 0003's
    "idempotent + retryable"), and the upload isn't yet confirmed at report time,
    post the db link as a **follow-up threaded reply once `exists` is true** —
    never block the result on the upload.
- Render the threaded result: absolute per-tx/per-block timing **+** the
  flamegraph, with the db link as a footer (S3) — no vs-baseline delta.

**Status:**

- [ ] Initial implementation
- [ ] Integration coverage
- [ ] Reviewed (Codex)
- [ ] Validated

**Acceptance & Validation:**

- [ ] A completed `slack_adhoc` run posts a flamegraph (link or file) as a
  **threaded reply** that resolves to the artifact for that job — integration
  test (fake Slack client + a store-backed artifact); the link/upload references
  the run's `artifact_key`.
- [ ] With `kind = "s3"`, the threaded result summary footer carries a presigned
  **`stacks-bench.db`** link, gated on `store.exists` — integration test
  (store-backed db object present → link rendered; absent → no link / deferred
  reply).
- [ ] In `kind = "local"` the flamegraph falls back to the download endpoint and
  **no db link** is shown (`signed_url → Unsupported`) — Decision 0001 path test.

**Tests:** artifact-manifest + Slack-delivery tests over a store-backed fixture.

### Phase 5 (optional, follow-up): richer UX

Result **buttons** ("Re-run", "Profile again with more repetitions") and a
**modal** form (`views.open` off a button's `trigger_id` — *not* a slash
command, which isn't a v1 surface), optionally a secondary `/bench` slash
command for users who prefer it. Carved out as its own follow-up — not required
for the deliverable.

**Status:**

- [ ] Design pinned
- [ ] Implementation
- [ ] Reviewed (Codex)

## Decisions to pin (before/early in build)

Surfaced from the design's open questions + this iteration's refinements:

1. **Entry surface = `@sbgh` mention** (thread on the user's request; no bot
   parent). **Pinned** — see Scope. (Slash command is a possible later
   secondary surface, Phase 5.)
2. **Threaded reporting** — all output threads under the request; status via a
   **reaction** on the request (not a threaded status reply); rejections via
   `chat.postEphemeral`. **Pinned.**
3. **Intent-resolution seam** — deterministic parser v1 behind a `resolve_workload`
   seam; LLM resolver ([`0020`](../index.md)) plugs in later; authz-before-resolve,
   validate-after-resolve. **Pinned.**
4. **Flamegraph delivery:** signed-URL link first (simplest, rides `0001`), file
   upload as Phase-4 polish. *(Lean: link; confirm with Codex.)*
5. **Authz + cost control:** allowlist granularity (workspace/channel/user) and
   whether ad-hoc profiles need per-user rate limits or a confirm step — a
   profiling run is expensive (full VM + replay). *(Pin in Phase 1.)*
6. **Queue/host sharing:** ad-hoc Slack profiles share the bench host + queue
   with PR benches — same lane or a separate one? *(Pin before Phase 2;
   default: same queue, revisit under fairness.)*
7. **Retention:** how long archived flamegraphs are kept + where the Slack link
   serves from. *(Ties to a future `0001` retention follow-up.)*

## Final Validation

- **MVP (Phases 1+2):** `@sbgh bench --block N --repetitions K` → a status
  reaction on the request + a threaded absolute-timing result, end-to-end on the
  bench path; GitHub reporting untouched.
- **Payoff (Phase 4):** the same flow returns a **flamegraph** in-thread.
- Slack stays opt-in/behind config until at least Phases 1+2 are green.

## Follow-Ups

- [`0020`](../index.md) — LLM intent resolver behind the `resolve_workload` seam.
- Phase 5 UX (result buttons / modal; optional secondary slash command).
- `0001` retention/lifecycle policy (governs flamegraph link longevity).
- Fairness/lane separation if ad-hoc load contends with PR benches (relates to
  `0015` resource-aware admission).
