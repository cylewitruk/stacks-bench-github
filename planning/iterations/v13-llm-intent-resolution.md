# v13: LLM intent resolution

Successor to [v5 (`0002`)](../archive/completed/0002-slack-adhoc-profiling.md):
the mention surface and `WorkloadSpec` seam exist, but users still need to
think in flags. v13 makes the normal path "user input → schema-validated
spec", with the current flag parser kept only as compatibility/disabled-mode
plumbing and an internal fast path for already-structured input.

> **Status:** in progress — Slack resolver implementation landed for review.
> Phase 4 (PR comments), real-model eval, and live Slack validation remain open.
>
> The load-bearing rule is that the model returns only a strict JSON object that
> decodes to our intent schema. If it cannot produce a valid spec, it returns an
> invalid result with a short user-facing reason; the daemon never accepts raw
> model text or model-emitted CLI args.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0020-llm-intent-resolution` | primary | in_progress |

## Why

The current Slack surface works, but it asks humans to remember an internal flag
grammar (`--block`, `--txid`, `--warmup`, `--repetitions`, `--rev`). That grammar
was deliberately a bootstrapping seam, not the product interface.

The desired interface is natural input:

- "bench blocks 8123456 to 8200000 on 3.4.0.0.3, 10 reps, 1000 warmup"
- "profile tx f426… twice on feat/stacks-bench"
- "run this branch against block 8123456"

The system should either turn that into a complete, validated workload spec or
tell the user what is missing/invalid. Longer-term, explicit inputs can move to
a Slack modal; the text flag parser should not be the primary UX.

## Item: `0020-llm-intent-resolution`

- **id:** `0020-llm-intent-resolution`
- **status:** `in_progress`
- **priority:** `medium`
- **depends_on:** `0002-slack-adhoc-profiling`
- **source:** v5 seam + high-value list (2026-06); user direction to replace
  flag parsing with schema-filled specs

**Problem:** Bench requests are still resolved through a deterministic flag
parser. That is exact, but ugly, and it is the wrong long-term UI for Slack or
PR comments.

**Scope:** Add an LLM-backed resolver that maps text input to a strict,
schema-validated intent JSON object, then validates and converts it into the
existing `WorkloadSpec`. The model is a translator from user language to typed
fields, not an executor. It cannot emit raw `bench_args`, select arbitrary CLI
flags, or enqueue jobs directly.

**Acceptance:** A natural-language Slack request resolves to a complete
`WorkloadSpec` and enqueues the same benchmark job as an equivalent structured
request. Ambiguous or invalid input produces a short user-facing rejection or
clarifying message, with no enqueue and no reaction. If the provider returns
non-schema JSON, malformed JSON, an invalid enum/value, or an invalid spec, the
request is rejected. Before flipping this on as the normal Slack path, a small
real-model eval set must pass the threshold pinned in Phase 2.

## External API Grounding

OpenAI's current API supports Structured Outputs by setting a JSON Schema output
format. The Responses API reference says `text.format: { "type":
"json_schema", ... }` enables Structured Outputs and that JSON Schema is
preferred over older JSON mode for supported models. The structured-output guide
documents using `strict: true` with the schema. v13 should use that shape rather
than freeform prompting.

- [Structured Outputs guide](https://developers.openai.com/api/docs/guides/structured-outputs)
- [Responses API reference](https://developers.openai.com/api/reference/resources/responses/methods/create)

## Intent Schema

The model output is one of two states:

```json
{
  "status": "resolved",
  "target_kind": "block_range",
  "block": null,
  "block_range": { "start": 8123456, "end": 8200000 },
  "txids": null,
  "repetitions": 10,
  "warmup": 1000,
  "rev": "3.4.0.0.3",
  "reason": null,
  "issues": null
}
```

For an explicit block list, `block` carries selectors:

```json
{
  "status": "resolved",
  "target_kind": "block",
  "block": [
    { "kind": "height", "height": 8123456, "hash": null },
    {
      "kind": "hash",
      "height": null,
      "hash": "c3b1aad400000000000000000000000000000000000000000000000000000000"
    }
  ],
  "block_range": null,
  "txids": null,
  "repetitions": 1,
  "warmup": 0,
  "rev": null,
  "reason": null,
  "issues": null
}
```

or:

```json
{
  "status": "invalid",
  "target_kind": null,
  "block": null,
  "block_range": null,
  "txids": null,
  "repetitions": null,
  "warmup": null,
  "rev": null,
  "reason": "I need a block height, block range, or transaction id to benchmark.",
  "issues": [
    {
      "field": "target",
      "code": "missing",
      "message": "Specify a txid, block height/hash, or block range."
    }
  ]
}
```

Rules:

- `status` is required.
- Use one strict-mode-friendly object with a `status` discriminant and nullable
  fields, not a root `anyOf`. Unknown fields are rejected
  (`additionalProperties: false`).
- For `status = "resolved"`, `target_kind` is one of `block`, `block_range`, or
  `txids`, and exactly the matching target field is non-null.
- A `block` target is a list of block selectors. Each selector is either a
  canonical block height or a hex-encoded block hash; the schema uses a
  per-selector `kind` discriminant plus nullable `height` / `hash` fields so the
  daemon can validate exactly one representation.
- Block selector validation is exact: `kind = "height"` requires `height` and
  `hash = null`; `kind = "hash"` requires `hash` and `height = null`.
- Hex inputs for txids and block hashes may include a user-facing `0x` prefix
  (as block explorers often display them). The daemon strips that prefix,
  validates the remaining value is exactly 32 bytes / 64 hex characters, and
  stores/emits the normalized bare hex form.
- `block_range` means an inclusive range from height `start` to height `end`; it
  is not a two-block list and does not accept hashes. The validator rejects
  `start > end`.
- For `status = "invalid"`, `reason` is required, `issues` may carry
  field-level diagnostics, and every target/run field is null.
- `repetitions` is always populated and must be `>= 1`. Default: `1`.
- `warmup` is always populated and must be `>= 0`. Default: `0`.
- `rev` is `null` or a branch/tag/SHA string. `null` means `[slack].default_rev`.
- `invalid.reason` is short, user-facing, and safe to post back. `issues` is a
  typed list with `field` (`target`, `block`, `block_range`, `txids`,
  `repetitions`, `warmup`, `rev`) and `code` (`missing`, `invalid`,
  `ambiguous`, `unsupported`, `needs_context`) so Slack/modals can point at the
  missing or incorrect input without trusting free-form model text for control
  flow.
- The daemon bounds model-controlled rejection text before posting it: reason
  and issue messages are truncated and the issue list is capped.

The daemon validates the decoded object again before constructing `WorkloadSpec`.
Schema conformance is necessary but not sufficient: strict Structured Outputs
enforce shape (required fields, enums, no unknown properties), while the daemon
owns numeric/value bounds (`repetitions >= 1`, `warmup >= 0`, txid format, range
ordering, non-empty lists, and later ref existence).

## Phases

### Phase 1: Resolver Core + Config

**Goal:** Add the provider-agnostic resolver seam and config without calling a
real provider.

**Scope:**

- Add `[llm]` config, default disabled.
- Add env-only `SBGH_OPENAI_API_KEY`; a TOML key is a hard error.
- Add `provider = "openai"`, `model`, `input_max_chars`, request timeout, and
  per-user rate-limit knobs.
- Add typed `IntentResolution` / `IntentTarget` structs and a
  `validate_intent_resolution` function that produces `WorkloadSpec` or a
  user-facing rejection.
- Update the internal workload target model as needed so `--block` can carry
  both canonical heights and hex-encoded block hashes; the current height-only
  `Vec<u64>` is too narrow for the LLM schema and the eventual parser cleanup.
- Verify the downstream `stacks-bench` CLI accepts normalized bare 64-character
  hex for `--txid` and block-hash `--block` inputs before v13 emits normalized
  values.
- Add a small representative eval fixture format (prompt + expected
  `IntentResolution` or invalid reason class). The fake resolver can replay it;
  the real provider runs it in Phase 2.
- Add an `IntentResolver` trait and a fake/test resolver.

**Acceptance & Validation:**

- [x] Config defaults disabled and requires the env key only when enabled.
- [x] Malformed/extra-field intent JSON is rejected.
- [x] Resolved block-height, block-hash, block-range, txid, warmup, repetition,
      and rev examples validate into the expected `WorkloadSpec`.
- [x] Txid and block-hash examples accept optional `0x` prefixes, reject
      non-hex / wrong-length values, and normalize to bare 64-character hex.
- [ ] Normalized bare-hex txid and block-hash args are accepted by the
      downstream `stacks-bench` parser/CLI path.
- [x] Invalid intent produces a user-facing message and no spec.
- [x] The eval fixture set exists with at least 15 prompts covering common,
      ambiguous, invalid, and flag-shaped inputs.

**Tests:**

- Unit tests in the new resolver module.
- Config layering tests mirroring Slack/env-only token behavior.

### Phase 2: OpenAI Structured-Output Provider

**Goal:** Implement the real OpenAI provider behind the trait.

**Scope:**

- Add a small `OpenAiIntentResolver` over the existing `reqwest` stack.
- Call the Responses API with `text.format.type = "json_schema"` and
  `strict = true`.
- Use no tools, no web access, and no raw command generation.
- Parse only the schema-constrained response body into `IntentResolution`.
- Treat provider errors, refusals, incomplete responses, non-schema output, and
  validation failures as user-facing rejection/clarification, never as enqueue.
- Add an offline/manual eval command or test harness that runs the fixture set
  against the configured real model when `SBGH_OPENAI_API_KEY` is present.

**Acceptance & Validation:**

- [x] Exact JSON request-body tests pin the schema and model request shape.
- [x] Response parsing tests cover resolved, invalid, malformed, and provider
      error envelopes.
- [x] Provider timeout/error returns a safe "could not resolve request" message.
- [ ] Real-model eval pass rate meets the implementation-pinned threshold
      before Phase 3 makes the resolver the normal Slack path. Suggested
      starting gate: 90% exact target/run-field match and 100% no unsafe
      enqueue for invalid/ambiguous prompts.

**Notes:** The default model should be chosen at implementation time from the
current small structured-output-capable OpenAI model family. Do not bake a stale
model name into the plan.

### Phase 3: Slack Wiring

**Goal:** Use the LLM resolver as the normal Slack product path when enabled.

**Scope:**

- Authz remains first.
- Strip the leading mention, then resolve text into `WorkloadSpec`.
- If `[llm].enabled = false`, retain the current deterministic parser for
  compatibility and local operation.
- If `[llm].enabled = true`, keep the deterministic parser as an internal
  fast-path for already-structured requests that parse cleanly. It avoids
  latency/cost/provider dependency for explicit input, but it is not the desired
  user-facing UX.
- If the parser fails and the input is natural language, call the LLM resolver.
- Rejections/clarifications are posted as ephemeral replies; no enqueue and no
  reaction.
- Successful resolutions enqueue exactly as today from `WorkloadSpec`.

**Acceptance & Validation:**

- [x] NL Slack request enqueues with the expected `bench_args` and rev.
- [x] Invalid/ambiguous NL request posts an ephemeral message and enqueues
      nothing.
- [x] Off-allowlist users are rejected before any provider call.
- [x] A flag-shaped request that parses cleanly takes the parser fast-path and
      does not call the provider.
- [x] Provider/rate-limit failures do not enqueue.
- [ ] A hallucinated/nonexistent `rev` fails cleanly through the daemon-owned
      commit-resolution path (no new risk class).
- [ ] If provider latency is noticeable, the Slack handler either posts an
      immediate short acknowledgement or otherwise gives the user timely
      feedback without enqueuing early.

**Tests:**

- Connector tests with a fake resolver proving authz-before-LLM, success,
  invalid, and provider-error behavior.

### Phase 4: PR Comment Surface

**Goal:** Reuse the same resolver for PR comments once Slack is proven.

**Scope:**

- Add a PR-comment path for natural-language benchmark intent.
- Preserve existing explicit `/benchmark` compatibility while this rolls out.
- Reply on the PR when input is invalid or ambiguous.
- Keep the same schema validation and authz/policy gates.

**Acceptance & Validation:**

- [ ] PR comment NL resolves to the same `WorkloadSpec` as Slack for matching
      text.
- [ ] Invalid input comments receive a clear response and do not enqueue.

## Decisions

1. **Schema or no job.** The provider must return strict schema-valid JSON.
   Anything else is a rejection.
2. **Spec, not args.** The model fills a structured intent object; daemon code
   owns validation and conversion to `WorkloadSpec` / `bench_args`.
3. **LLM primary UX, parser fast-path.** Natural language is the desired Slack
   interface, but the deterministic parser stays as an internal fast-path for
   already-structured text. Both paths produce the same `WorkloadSpec`.
4. **Authz before spend.** Team/user authorization and PR policy checks happen
   before provider calls.
5. **No model-side tools.** v13 is intent extraction only. The model cannot
   fetch refs, inspect repos, or trigger work.
6. **Ref validation stays daemon-owned.** The resolver may fill `rev`; existing
   claim-time commit resolution and future pre-enqueue validation remain daemon
   responsibilities.

## Follow-Ups

- Slack modal input can replace explicit flag entry for users who prefer forms.
- A recent-pinned-ref picker can use the `PinnedTarget` snapshot from v11.
- `0030-results-qa-agent` should reuse the provider/config layer, but stays a
  separate output-side feature.
