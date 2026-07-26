# v24.3: Slack Snapshot Reporting and Integration Boundary

Continuation of
[v24.2](0058-github-intent-boundaries.md). Replace
Slack's card/stream/timeline lifecycle with one ordinary, canonical progress
message, then extract the reduced integration into `sbgh-slack`.

> **Status:** shipped — completed locally on 2026-07-26 after a focused
> pre-commit hardening pass fixed two version-fencing defects and restored
> regression coverage.
>
> This continuation intentionally changes Slack presentation while preserving
> authorization, enqueue, execution, persistence, and terminal-result
> semantics. Its snapshot renderer is designed for v25's durable event replay,
> but v25 remains the owner of the event ledger and remote-worker protocol.
> The credentialed Slack sandbox success and crash-window smokes remain
> deployment checks because this environment has no Slack tokens or designated
> test channel.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0060-slack-snapshot-reporting` | primary: one canonical progress/result message | shipped |
| `0061-slack-integration-boundary` | co-primary: extract the reduced Slack integration | shipped |

## Why

The current Slack implementation maintains Block Kit cards, streamed plan
updates, timelines, task sections, keepalive/session state, and several update
paths. That machinery is difficult to reconcile after partial API failures and
will become harder to reason about when v25 replays durable worker events after
an orchestrator restart.

Slack only needs one message per reporting identity. Rendering the complete
message from current projected state makes updates convergent and idempotent:
replay produces the same bytes for the same state instead of appending a second
timeline entry or reconstructing state from the previous Slack message.

The simplification also makes a real crate boundary practical. Today
`sbgh-daemon::slack` reaches into daemon reporting helpers, broad job stores,
and raw SQL. After the behavior change, a small Slack adapter can depend on
narrow ports and provider-neutral inputs.

## Target Shape

```text
current reporter events ──> daemon projection ──> SlackProgressView
                                                   |
                                                   v
                                          canonical full render
                                                   |
                                     post once / chat.update thereafter

sbgh-daemon
  ├──> sbgh-postgres
  ├──> sbgh-intent
  └──> sbgh-slack ──> sbgh-core + sbgh-intent
```

In v25, committed task-neutral events replace the in-memory event source. They
feed the same snapshot projection; the Slack renderer does not change.

## Design Rules

- **Snapshot, never patch.** Every update renders the entire canonical message
  from current state. Code never reads, parses, or incrementally edits the
  previous message body.
- **One message per reporting identity.** A benchmark group/request owns one
  ordinary Slack message in the originating thread. Phase, progress, variant,
  comparison, and terminal state converge into it.
- **Replay is idempotent.** Rendering the same state is byte-identical and does
  not create another message. State progression is monotonic; stale updates
  cannot overwrite a newer snapshot.
- **Readable alignment is explicit.** Fixed-width progress-bar rows use Unicode
  block characters inside a fenced code block. Emoji/status prose stays
  outside aligned columns because emoji display width is not portable.
- **Debounce progress, flush milestones.** Fine-grained progress is
  rate-limited/debounced. Phase changes, failures, cancellation, and terminal
  results bypass the debounce and request an immediate snapshot.
- **Persist identity; reconcile creation.** The message timestamp remains
  durable reporting state. Initial creation uses an opaque stable identity so
  restart can recover from “Slack accepted the post but timestamp persistence
  failed” without blindly posting again.
- **Least authority.** Only the daemon composes `sbgh-slack` and receives Slack
  credentials. Workers never receive them. Reconciliation metadata contains
  opaque IDs only, never repository secrets, tokens, or user text.
- **Keep domain/report policy outside Slack.** Benchmark comparison and result
  selection remain daemon-owned. `sbgh-slack` receives a compact view to
  render; it does not query PostgreSQL or calculate comparisons.
- **Change behavior before moving files.** Land and validate the simple-message
  lifecycle in the daemon first. Extract the proven, smaller integration in a
  separately reviewable phase.

## Scope

- Define `SlackProgressView` and a deterministic plain-message renderer.
- Replace Block Kit cards, streaming, task timelines, and reporting-session
  mutation with one post/update lifecycle.
- Preserve mention authorization, deterministic/LLM intent resolution,
  enqueue, thread placement, useful terminal links/results, and current
  reaction behavior unless an explicit test records a deliberate removal.
- Make initial-message creation recoverable across the Slack
  post/persist crash window.
- Extract Slack transport, Socket Mode intake, request connector, message
  rendering/updating, narrow ports, and focused test support into
  `sbgh-slack`.
- Move target lookup/SQL and reporting projection/comparison policy to daemon
  composition or a persistence adapter before extraction.
- Remove obsolete Slack-specific dependencies and dead code.

**Non-goals:** v25's durable attempt-event ledger or protocol; durable storage
for every fine-grained progress sample; new Slack commands; App Home; portals;
worker-held Slack credentials; parsing old Slack messages; preserving the old
Block Kit layout or streamed timeline.

## Phases

### Phase 1: Snapshot Contract and Creation Reconciliation

**Goal:** Fix the state model and crash semantics before replacing the current
renderer.

**Scope:**

- Define a compact `SlackProgressView` containing reporting identity, current
  phase, bounded progress, variant/run summary, terminal state, and
  daemon-selected result/comparison fields.
- Define monotonic snapshot version/order semantics so a delayed update cannot
  regress the visible message.
- Implement a pure deterministic renderer and “unchanged snapshot” suppression.
- Use Slack message metadata with an opaque reporting identity for initial
  posts. When no timestamp is persisted, search the known request thread for
  an exact bot-authored identity before creating:
  - exactly one match: persist its timestamp and update it;
  - no match after a successful lookup: create;
  - lookup failure or multiple exact matches: fail closed and retry.
- Add only the minimum Slack history scope required by the supported channel
  types; update the app manifest and setup documentation. Do not depend on an
  undocumented client-supplied idempotency field.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] Equal views render byte-identical text; the renderer is pure and never
  consumes prior Slack message contents.
- [x] Progress is clamped/validated, fixed-width rows align in a fenced code
  block, and arbitrary user/repository text cannot break the fence or mention
  unintended users.
- [x] Replaying the same snapshot performs no creation and at most one
  necessary update.
- [x] A stale snapshot cannot overwrite a later phase/version.
- [x] A failure after Slack accepts `chat.postMessage` but before timestamp
  persistence is recovered by exact metadata identity and does not create a
  duplicate.
- [x] Reconciliation is bounded to the known conversation/thread and configured
  bot/app; lookup errors are not treated as “not found.”
- [x] Added Slack scopes and metadata visibility/security trade-offs are
  documented.

**Tests:**

- Golden renderer tests for queued, preparing, running, multi-variant,
  completed, failed, cancelled, and comparison snapshots.
- Property/table tests for progress bounds, ordering, escaping, and stable
  rendering.
- Failure-injection tests for post success plus lost response, timestamp-write
  failure, restart, zero/one/multiple matches, and lookup failure.

### Phase 2: Replace Cards, Streams, and Timelines

**Goal:** Run all Slack reporting through one ordinary message and one
snapshot-update path.

**Scope:**

- Post the initial snapshot as the one group/request message in the originating
  thread; persist its timestamp.
- Map current reporter events and group/run state into
  `SlackProgressView`.
- Coalesce fine-grained updates with a bounded debounce/rate limiter; flush
  phase and terminal transitions immediately.
- Preserve useful result URLs, status/error summaries, variants, and
  daemon-computed comparison output in the terminal snapshot.
- Delete card blocks, stream append/stop, keepalive, timeline mutation,
  task-section planning, and redundant reporting-session machinery once no
  caller remains.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] One accepted request/group creates one normal Slack message and only
  updates that timestamp through terminal state.
- [x] Multi-variant/repetition groups remain one message and expose bounded,
  readable progress without unbounded message growth.
- [x] Progress storms respect Slack rate limits; milestone and terminal
  snapshots are not stranded behind the debounce.
- [x] Duplicate/replayed/out-of-order source events converge to the same
  canonical visible state.
- [x] Restart with persisted timestamp resumes by rendering current state and
  updating the same message.
- [x] Slack update failures remain best-effort reporting failures and do not
  change job execution/terminal classification.
- [x] Existing authorization, enqueue, thread, reaction, and non-Slack report
  surfaces preserve behavior.
- [x] `slack-messaging` and other stream/card-only dependencies are removed
  when unused.

**Tests:**

- Connector/report-surface tests for singleton and grouped benchmarks.
- Debounce tests with deterministic time for progress storms and terminal
  flush.
- Restart/replay, stale-update, update-retry, and concurrent-group isolation
  tests.
- Existing GitHub check/comment and CLI reporting suites as non-regression
  coverage.

### Phase 3: Extract `sbgh-slack`

**Goal:** Move the reduced Slack integration behind an explicit crate boundary
without moving daemon persistence or reporting policy with it.

**Scope:**

- Create a non-published `sbgh-slack` workspace crate.
- Move Slack API transport, Socket Mode, request connector, snapshot
  renderer/updater, Slack-owned DTOs/errors, and focused test support.
- Depend on narrow ports:
  - `BenchmarkQueue` for enqueue;
  - `IntentResolver` from `sbgh-intent`;
  - a one-purpose message-identity persistence port;
  - already-resolved target/config inputs.
- Move raw SQL target resolution out of Slack and into daemon/Postgres
  composition; inject `SlackJobTarget`.
- Keep benchmark summary/comparison calculation and generic `ReportSurface`
  orchestration daemon-side.
- Remove `sbgh-daemon::slack` after rewiring composition and tests.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] `sbgh-slack` has no dependency on `sbgh-daemon`, `sbgh-postgres`, SQLx,
  Octocrab, libvirt, worker, or aggregate `DaemonConfig`.
- [x] No raw SQL or broad `RunnableJobStore`/`JobStore` dependency remains in
  Slack code.
- [x] The daemon is the only production composition root for Slack credentials,
  client construction, rate limiting, and side effects.
- [x] Slack target resolution uses a narrow daemon/persistence seam and
  preserves current missing/disabled/error behavior.
- [x] Test doubles implement narrow ports rather than recreating database or
  daemon machinery.

**Tests:**

- Moved Slack client, Socket Mode, connector, renderer, reconciliation, and
  retry suites under `sbgh-slack`.
- Postgres-backed target-resolution tests under the persistence/daemon owner.
- Cargo dependency-tree assertions for `sbgh-slack`.

### Phase 4: Ratchets, Documentation, and v25 Compatibility

**Goal:** Make the boundary enforceable and document exactly how durable event
projection will drive it.

**Scope:**

- Extend the Cargo-metadata DAG check for `sbgh-slack` with all features and
  runtime/build dependencies.
- Update architecture, Slack setup/manifest, reporting, contributor, and
  operational documentation.
- Update the `0017` design to specify that durable reporter replay projects
  current state into the full snapshot renderer rather than replaying Slack
  mutations.
- Remove stale task-card, stream, timeline, and daemon Slack module references.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] Cargo metadata matches the documented target graph.
- [x] `rg` finds no production card/stream/timeline implementation or stale
  `sbgh_daemon::slack` import.
- [x] Documentation states that v25 replay rebuilds `SlackProgressView` from
  durable projected state and renders the full snapshot.
- [x] v25 remains the sole owner of event durability, sequence gaps, attempt
  fencing, and worker reconnect semantics.
- [x] Documentation links and the planning registry pass repository checks.

**Tests:**

- `scripts/check-package-dag.py`
- `scripts/check-docs.py`
- Cargo Machete, Clippy, and rustfmt through `just lint`.

## Final Validation

- [x] `just build --no-sccache`
- [x] `just lint --no-sccache`
- [x] `just test --no-sccache --summary` — 761 passed, 1 environment skip
- [x] Cargo metadata matches the documented target dependency graph with all
  features.
- [ ] Slack sandbox smoke: one request creates one threaded normal message;
  phase/progress and terminal states update that timestamp with a readable
  fenced progress display.
- [ ] Slack sandbox failure smoke: interrupt after accepted initial post but
  before timestamp persistence, restart, and verify reconciliation updates
  exactly one message.
- [x] A deterministic-time progress storm stays within configured update
  limits and terminal state appears promptly.
- [x] GitHub check/comment, CLI, enqueue, execution, artifacts, cancellation,
  and terminal classification remain unchanged.

## Reopened Pre-Commit Hardening

- [x] Preserve monotonic snapshot versions across queue-to-start and group-run
  transitions.
- [x] Never overwrite a reconciled message with an older snapshot; remove the
  `u64::MAX` enqueue-failure sentinel.
- [x] Exercise real deferred debounce/coalescing behavior with changing
  progress views.
- [x] Restore focused connector coverage for LLM fallback, rate limiting,
  reactions, rejection, repetition/cache gates, and comparison enqueue.
- [x] Restore the full abandoned-session predicate coverage.
- [x] Reject or encode Slack link delimiters and narrow connector configuration
  to the authority it uses.
- [x] Re-run focused tests, build, lint, and the complete workspace suite.

## Follow-Ups

- [v25](../../iterations/v25-worker-fleet-block-validation.md) replaces the
  in-memory reporter source with committed attempt events. Its projector must
  rebuild the current `SlackProgressView`; it must not replay historical Slack
  update commands.
- Richer operator dashboards, Slack App Home, and durable fine-grained progress
  remain separate features.
