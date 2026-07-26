# Design 0017: Task-Neutral Durable Worker Events

- **id:** `0017-generic-phase-events`
- **status:** `planned` (`v25-worker-fleet-block-validation`)
- **depends_on:** `0055-execution-boundary-preparation` (v24),
  `0056-compiler-enforced-execution-boundaries` (v24.1)
- **unblocks:** `0004-worker-fleet`, `0019-block-validation-recipe`
- **iteration:**
  [`v25-worker-fleet-block-validation`](../iterations/v25-worker-fleet-block-validation.md)
- **review:** Codex signed off (design)
- **source:** `0008-execution-architecture` generic-event follow-up + v25
  durable remote-event requirement

Turn the current in-memory `WorkerEvent` channel into a task-neutral, durable
attempt-event boundary before benchmark execution crosses into a worker
process.

## Current State

The repository has two related but different event surfaces:

1. `job_event` is an append-only job audit timeline. It persists queue,
   terminal, comment/check, and provenance events. Its PostgreSQL enum still
   contains benchmark-specific phase variants, but current live worker phases
   are not written through it.
2. `WorkerEvent` is the live recipe-to-reporter stream. It runs over an in-memory
   Tokio channel and has no attempt identity, durable sequence, replay cursor,
   or orchestrator-restart recovery.

Moving the worker across a network without fixing the second surface would make
reporting dependent on one orchestrator process lifetime. Simply writing remote
events into the existing job timeline would also conflate transport deduplication
with user/domain audit history.

## Decision

Use a durable, attempt-scoped worker-event ledger for reliable transport events,
then project accepted events into reporting and the existing job audit model.

Conceptually:

```text
worker EventSink
  -> WorkerEventEnvelope(attempt, reliable_seq, trace_id, payload)
  -> durable attempt-event insert + ACK
  -> reporter projection/catch-up
  -> GitHub/Slack + generic job_event audit rows + terminal state
```

The exact migration names may change during implementation, but the separation
and invariants below do not.

## Reliable vs. Best-Effort Events

- **Reliable, ordered, durable:** phase transitions, terminal outcome, and any
  event required to reconstruct task/report state after restart.
- **Best-effort:** heartbeat and fine-grained progress samples. They may update
  a latest-progress snapshot or be stored when received, but they do not occupy
  the reliable contiguous sequence and cannot block task execution.

Only reliable events advance `reliable_seq`. Dropping a progress sample must not
create a false gap in the durable stream.

## Durable Envelope

Every reliable event carries at least:

- protocol version;
- worker ID;
- job ID and attempt/lease identity;
- monotonically increasing `reliable_seq` within the attempt;
- trace/correlation ID originating at orchestrator assignment;
- task-neutral payload kind and payload;
- worker-emitted timestamp for diagnostics; the orchestrator records its own
  receipt timestamp as authoritative ordering metadata.

The durable store enforces uniqueness on `(attempt_id, reliable_seq)`.

- Repeating the same sequence with the same canonical payload is an idempotent
  success and returns the current acknowledgement.
- Reusing a sequence for a different payload is a protocol violation.
- An event for a fenced/superseded attempt cannot mutate the current job or
  reporter projection. The response tells the worker to stop retrying that
  attempt.
- The orchestrator acknowledges a reliable sequence only after the event is
  durably committed. The acknowledgement cursor is the highest **contiguous**
  durable sequence, not merely the highest sequence observed.

## Task-Neutral Phase Persistence

Add generic phase-started/phase-finished job-event values with a validated phase
label/detail payload. PostgreSQL enum values already deployed for
`phase_build_*`/`phase_bench_*` remain readable; PostgreSQL enum values are not
removed in place. New writers use the generic shape, and compatibility tests
cover historical rows.

Block-validation labels such as `dataset`, `probe`, `validate`, and `reduce`
must use the same persisted shape as benchmark labels. The schema does not gain
one enum value per recipe phase.

## Reporter Projection and Restart

- The reporter consumes committed reliable events in sequence order, whether
  woken by the live ingest path or restarted later.
- Out-of-order durable insertion is allowed, but projection is gap-free: the
  reporter stops at the first missing `reliable_seq` and never projects a later
  event past that gap. A terminal event may be stored out of order, but terminal
  acceptance, artifact promotion, and job terminalization require a contiguous
  reliable prefix through the terminal sequence.
- Projection progress is durable per attempt. On orchestrator restart, pending
  attempts resume from the first committed, unprojected event.
- Existing persisted check/comment/message identifiers remain the basis for
  reconciling external side effects. The event ledger provides repeatable
  internal input; it does not pretend PostgreSQL can transact atomically with
  GitHub or Slack.
- [v24.3](../iterations/v24.3-slack-snapshot-reporting.md) gives Slack a
  deterministic full-snapshot renderer and one durable message identity. On
  catch-up, this projector rebuilds the current `SlackProgressView` from the
  committed event prefix and renders the entire canonical message; it never
  replays historical Slack mutations or interprets the prior message body.
  Re-projecting unchanged state is therefore idempotent, while a later
  contiguous prefix converges the same message to the newer snapshot.
- Check Runs retain their stable `external_id` reconciliation. PR comments gain
  a deterministic hidden marker derived from the stable report-surface identity
  (for example `<!-- sbgh-report:<job-or-group-id> -->`). When no comment ID is
  persisted, the reporter first searches the PR's comments for an exact marker
  authored by the configured App/bot, persists and updates that comment when
  found, and creates only when the search succeeds with no match. A create whose
  response or subsequent ID persistence is lost is therefore found on replay.
  Search failure is not permission to create blindly; the comment side effect
  remains pending/best-effort while a configured Check Run stays authoritative.
  Comment-only mode uses the same marker/retry rule rather than accepting
  duplicates.
- Terminal acceptance is idempotent and fenced by attempt. Artifact promotion
  and job terminalization compose with the accepted terminal event as specified
  by v25's artifact lifecycle.
- Worker reconnect resumes from the highest durably acknowledged reliable
  sequence, not from process-local reporter memory.

## Trace Correlation

The assignment's trace/correlation ID is propagated through worker logs,
reliable events, best-effort progress, artifact manifests, and orchestrator
reporting logs. This is application-level correlation and does not require an
OpenTelemetry deployment in v25.

## Scope

- Versioned task-neutral event DTOs in `sbgh-proto`.
- Durable attempt-event storage, deduplication, acknowledgement, and projection
  cursor.
- Generic phase-started/phase-finished persistence with historical enum-row
  compatibility.
- Reporter catch-up after orchestrator restart.
- Attempt fencing and conflicting-duplicate detection.
- Deterministic PR-comment reconciliation across the create/persist crash
  window.
- Trace/correlation propagation.

**Non-goals:** no general-purpose event-sourcing framework; no durability
promise for every heartbeat/progress sample; no arbitrary workflow DSL; no
removal of historical PostgreSQL enum values; and no multi-version protocol
skew in v25.

## Acceptance

- A benchmark running through the loopback worker survives orchestrator restart:
  committed phases are replayed in order and terminal reporting completes.
- Benchmark and block-validation phases persist through one generic schema.
- Duplicate reliable delivery is idempotent; conflicting payload reuse at the
  same sequence fails closed.
- If sequences 5 and then 4 commit, projection remains stopped at the prior
  contiguous prefix until 4 exists; a terminal at 5 cannot terminalize the job
  or promote artifacts early.
- A stale attempt's events and terminal cannot mutate its successor attempt.
- A dropped best-effort progress sample creates no reliable-sequence gap.
- Reporter catch-up is driven from durable state, not worker resend timing or an
  in-memory channel.
- Historical benchmark-specific phase rows remain readable.
- Crashing after GitHub accepts initial-comment creation but before its ID is
  persisted does not create a second marked comment on reporter replay.
- One trace ID correlates orchestrator assignment, worker execution, events,
  artifacts, and terminal reporting.

## Validation

- Store contract tests for sequence insertion, duplicate/conflict behavior,
  out-of-order gaps, terminal-prefix gating, fencing, and projection cursors.
- Protocol fixtures for every reliable event payload and version rejection.
- Orchestrator-kill/restart integration test during an active loopback task.
- Worker reconnect test beginning from the last durable acknowledgement.
- Generic phase persistence tests for benchmark and block-validation labels.
- Compatibility fixture for historical benchmark-specific `job_event` rows.
- GitHub comment-reconciliation test that loses the create response/ID write,
  restarts the reporter, finds the bot-authored marker, and updates exactly one
  comment.
