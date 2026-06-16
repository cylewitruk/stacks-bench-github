# v17: Slack Reporting Robustness, Reactions & Observability

Three Slack-surface items: fix the queued-card race that double-posts and
poisons the repeat plan-ts chain (`0043`), add an immediate-ack + queued/running
reaction lifecycle (`0044`), and add the LLM/Slack lifecycle logging that
currently doesn't exist (`0045`).

> **Status:** in_progress
>
> Phase 0 design ready for review. Phase 1 is a correctness fix (the double-card
> race); Phases 2-3 are UX + observability and are independent of it.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0043-slack-plan-ts-race` | primary | in_progress |
| `0044-slack-reaction-lifecycle` | primary | in_progress |
| `0045-slack-llm-observability` | primary | in_progress |

## Why

- **Double cards + dead streams (`0043`).** Run 0's plan-card `ts` is recorded by
  the connector *after* `create_unlinked_job` already made the job claimable, with
  a slow Slack `start_plan_stream` round-trip in between. On an idle daemon the
  runner claims the job inside that window, the reporter's `latest_plan_message_ts`
  returns `None`, and it posts a **second** card. Worse, run 0 then has two
  `plan_message_sent` events; the repeat append copies the latest (the orphaned
  queued-card stream), so run 1+ inherit a `ts` that Slack has since discarded →
  `chat.appendStream` → `message_not_found` → the run reports onto a dead message.
  Run 1+ already work when run 0 doesn't race, because the append records their
  `ts` **atomically** with creation — run 0 just needs the same guarantee.
- **Laggy first response (`0044`).** Nothing acknowledges the poster until after
  the LLM round-trip resolves, so a natural-language request feels unanswered for
  seconds. And the single ⏳ doesn't distinguish "queued" from "running".
- **No visibility (`0045`).** The LLM resolver emits no logs at all and the
  connector logs only errors/warnings, so the operator sees nothing of the
  request → resolve → enqueue lifecycle. A log-level change can't help — the lines
  don't exist.

## Scope

- `0043`: make run 0's queued-card `ts` atomic with job creation, and repost
  (rather than go dark) if an inherited message is genuinely gone.
- `0044`: reaction lifecycle 💬 (ack) → ⏳ (queued) → 🚀 (running) → ✅/❌
  (terminal), group-aware for repeats.
- `0045`: info-level LLM + connector lifecycle logging; payloads at debug; no
  secrets.

**Non-goals:** the `0040` queued-receipt-vs-stream split is related but separate;
no change to the LLM provider/schema; no GitHub-surface reaction equivalent.

## Design Decisions

- **Make run 0 match run 1's atomic `ts`.** The fix mirrors what
  `insert_next_run_in_tx` already does for repeats: write the `plan_message_sent`
  event in the **same transaction** as the job + queued event. Since the `ts`
  comes from the Slack post (which needs the job id), the connector must **post
  the card before creating the job**: generate the id, post, then
  `create_unlinked_job(id, detail, plan_ts)`. The signature must stay ergonomic
  for the daemon **warming** caller, which has no Slack surface: it passes a
  generated id + `plan_ts = None` and gains no Slack coupling.
- **Don't trade the claim race for an orphan-card-without-a-job.** Posting before
  the insert introduces the inverse failure: a successful card post followed by a
  failed `create_unlinked_job` leaves a "queued" plan card for a job that never
  existed. The connector must handle it explicitly — best-effort update the posted
  card to a visible failure ("couldn't enqueue — please retry") rather than
  leaving a fake queued card. Net: the post-before-insert window fails *loud and
  honest*, where the current order fails as a silent double card.
- **Reactions transition once per group, not per run.** The reaction lives on the
  shared request message, so only run 0 (`benchmark_run_index == 0`) swaps
  ⏳→🚀; later repeats are no-ops, and only `is_final_repeat` swaps 🚀→✅. A
  failed/cancelled run swaps 🚀→❌ (the chain stops). This reuses the
  `is_repeat_group`/`is_final_repeat` predicates already on the timeline.
- **Generalize the reaction swap.** `swap_reaction` currently hardcodes removing
  ⏳; it must remove whatever is currently present (⏳ or 🚀) so the chain works.
- **Split the stream error taxonomy: `NotStreaming` vs `MissingMessage`.** Today
  `message_not_found` is bucketed as `NotStreaming` → switch to block updates, but
  `update_blocks` then *also* fails silently on a missing message (it only logs).
  So the backstop must cover **both** the append and the block-update layer: on a
  genuinely-missing message (distinct from `message_not_in_streaming_state`, which
  is a TTL lapse where block-update is correct), post a **fresh** card and
  **persist its new `plan_message_ts`** (so later updates and the repeat chain use
  the live `ts`, not the dead one). With `0043` fixed this is a rare backstop, not
  a common path.
- **Logging is net-new, gated by level, secret-free.** Lifecycle at info, payloads
  at debug; never log the API key, and treat the resolved prompt as debug-only.

## Phases

### Phase 1: Atomic queued-card plan ts (`0043`)

**Goal:** Run 0 can never be claimed before its plan `ts` is persisted; an
inherited-but-missing message reposts instead of reporting nowhere.

**Scope:**

- `create_unlinked_job` takes a caller-provided job id + optional
  `plan_message_ts`, writing the job, its queued event, and (when present) the
  `plan_message_sent` event in one transaction. Warming keeps passing a generated
  id + `None`.
- The connector generates the id, posts the queued card first (the post returns
  the `ts`, or `None` on failure), then calls `create_unlinked_job(id, detail,
  ts)`. If that insert **fails after a successful post**, best-effort update the
  posted card to a visible failure ("couldn't enqueue — please retry") instead of
  leaving a fake queued card.
- Split the stream error taxonomy (`NotStreaming` vs `MissingMessage`). On a
  `MissingMessage` at **either** the append or block-update layer, post a fresh
  card and persist its new `plan_message_ts`; `message_not_in_streaming_state`
  keeps switching to block updates as today.
- Update the connector's top-of-file flow doc — the order is now post card →
  create job → react, not enqueue → post → react.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [ ] A Slack run never posts two plan cards, even when claimed immediately on an
  idle daemon.
- [ ] Run 0 has exactly one `plan_message_sent` event; run 1+ inherit the live
  card and resume via `appendStream` with no `message_not_found`.
- [ ] A genuinely-deleted inherited message reposts a fresh card **and persists
  the new `ts`**, so subsequent updates land on it (covered for both append and
  block-update).
- [ ] A failed queued-card post still leaves the job runnable (reporter posts at
  claim) — no regression.
- [ ] A `create_unlinked_job` failure after a successful post leaves a visible
  failure message, not a fake queued card.

### Phase 2: Reaction lifecycle (`0044`)

**Goal:** Instant acknowledgment + a queued-vs-running indicator.

**Scope:**

- Add `ACK_REACTION` (`speech_balloon` 💬) and `RUNNING_REACTION` (`rocket` 🚀).
- Connector: add 💬 immediately after authz, **before** resolution (covers the
  LLM round-trip). On accept → swap 💬→⏳. On reject/unauthorized → remove 💬 (the
  ephemeral reply stays).
- Timeline: at started/running, swap ⏳→🚀 for run 0 only; later repeats are
  no-ops (group already running). At terminal, swap the current reaction (🚀) →
  ✅ only on `is_final_repeat`; a failed/cancelled run swaps →❌; intermediate
  repeats keep 🚀.
- Generalize `swap_reaction` to remove the currently-present reaction (⏳ or 🚀).

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [ ] A mention gets 💬 before the LLM resolves.
- [ ] Accepted → ⏳ → 🚀 → ✅/❌, each transition removing the prior reaction (no
  accumulation).
- [ ] A repeat group transitions once per group: 🚀 persists across intermediate
  repeats, the final run swaps to ✅, a failed/cancelled run swaps to ❌.
- [ ] Every post-ack rejection path removes the 💬 (tested): parser failure, LLM
  invalid/failure, over-cap, and cache-gate rejection all leave no stray ack.

### Phase 3: Slack & LLM observability (`0045`)

**Goal:** The request → resolve → enqueue lifecycle is visible at the default
log level.

**Scope:**

- LLM resolver (`openai`): info on request sent (model, user id, input length) and
  response received (latency, outcome: resolved/invalid/error); request/response
  bodies at debug, with the API key never logged and the prompt debug-only.
- Connector: info on mention received (channel, user), authz outcome, resolution
  path (deterministic parser fast-path vs LLM call), resolved workload summary
  (target/repetitions/rev), reaction transitions, and queued-card posted/resumed.
- Carry a correlation field (the `job_id` once created; the message `ts` before
  that) on the lifecycle lines so a single request is traceable across connector →
  OpenAI → runner → reporter.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [ ] At default `RUST_LOG` (info), a mention shows received → resolving
  (parser/LLM) → [LLM request/response + latency] → accepted/rejected → reaction →
  card posted.
- [ ] No secrets (API key) appear in any log line; the prompt is debug-only.

## Final Validation

- [ ] `just build`
- [ ] `just lint`
- [ ] `just test`
- [ ] Host smoke: a Slack 2-3 repeat request posts exactly one plan card (no
  double), reactions advance 💬→⏳→🚀→✅, run 1+ resume the same card with no
  `message_not_found`/dead-message fallback, and the journal shows the LLM
  request/response + lifecycle at info.

## Follow-Ups

- `0040-slack-queue-receipt-before-stream` overlaps the plan-ts lifecycle and may
  be revisited once `0043` lands (a claim-time stream start sidesteps the orphaned
  pre-claim stream entirely).
