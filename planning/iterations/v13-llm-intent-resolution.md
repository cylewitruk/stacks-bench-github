# v13: LLM intent resolution

Successor to [v5 (`0002`)](../archive/completed/0002-slack-adhoc-profiling.md):
the mention surface and `WorkloadSpec` seam exist, but users still need to
think in flags. v13 makes the normal path "user input → schema-validated
spec", with the current flag parser kept only as compatibility/disabled-mode
plumbing.

> **Status:** planned — Phase 0 design drafted for review. No code has landed.
>
> The load-bearing rule is that the model returns only a strict JSON object that
> decodes to our intent schema. If it cannot produce a valid spec, it returns an
> invalid result with a short user-facing reason; the daemon never accepts raw
> model text or model-emitted CLI args.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0020-llm-intent-resolution` | primary | planned |

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
- **status:** `planned`
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
request is rejected.

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
  "target": {
    "kind": "blocks",
    "blocks": [8123456, 8200000]
  },
  "repetitions": 10,
  "warmup": 1000,
  "rev": "3.4.0.0.3"
}
```

or:

```json
{
  "status": "invalid",
  "reason": "I need a block height, block range, or transaction id to benchmark."
}
```

Rules:

- `status` is required.
- `resolved.target.kind` is `blocks` or `txids`; exactly one target form is
  present.
- `repetitions` is always populated and must be `>= 1`. Default: `1`.
- `warmup` is always populated and must be `>= 0`. Default: `0`.
- `rev` is `null` or a branch/tag/SHA string. `null` means `[slack].default_rev`.
- `invalid.reason` is short, user-facing, and safe to post back.
- Unknown fields are rejected (`additionalProperties: false`).

The daemon validates the decoded object again before constructing
`WorkloadSpec`; schema conformance is necessary but not sufficient.

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
- Add an `IntentResolver` trait and a fake/test resolver.

**Acceptance & Validation:**

- [ ] Config defaults disabled and requires the env key only when enabled.
- [ ] Malformed/extra-field intent JSON is rejected.
- [ ] Resolved block, block-range, txid, warmup, repetition, and rev examples
      validate into the expected `WorkloadSpec`.
- [ ] Invalid intent produces a user-facing message and no spec.

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

**Acceptance & Validation:**

- [ ] Exact JSON request-body tests pin the schema and model request shape.
- [ ] Response parsing tests cover resolved, invalid, malformed, and provider
      error envelopes.
- [ ] Provider timeout/error returns a safe "could not resolve request" message.

**Notes:** The default model should be chosen at implementation time from the
current small structured-output-capable OpenAI model family. Do not bake a stale
model name into the plan.

### Phase 3: Slack Wiring

**Goal:** Use the LLM resolver as the normal Slack input path when enabled.

**Scope:**

- Authz remains first.
- Strip the leading mention, then resolve text through the configured resolver.
- If `[llm].enabled = false`, retain the current deterministic parser for
  compatibility and local operation.
- If `[llm].enabled = true`, the LLM resolver is primary; the flag parser is not
  the product path.
- Rejections/clarifications are posted as ephemeral replies; no enqueue and no
  reaction.
- Successful resolutions enqueue exactly as today from `WorkloadSpec`.

**Acceptance & Validation:**

- [ ] NL Slack request enqueues with the expected `bench_args` and rev.
- [ ] Invalid/ambiguous NL request posts an ephemeral message and enqueues
      nothing.
- [ ] Off-allowlist users are rejected before any provider call.
- [ ] Provider/rate-limit failures do not enqueue.

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
3. **LLM primary when enabled.** The flag parser remains only for disabled-mode
   compatibility and tests; it is not the desired Slack UX.
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
