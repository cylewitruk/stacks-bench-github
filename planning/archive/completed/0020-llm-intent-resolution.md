# 0020: LLM intent resolution

- **id:** `0020-llm-intent-resolution`
- **status:** `shipped`
- **date:** 2026-06
- **iteration:** v13 (`v13-llm-intent-resolution`)
- **depends_on:** `0002-slack-adhoc-profiling`

Added an LLM-backed intent resolver for Slack benchmark requests. The Slack
surface can now accept natural-language benchmark requests and resolve them into
a daemon-validated `WorkloadSpec` instead of requiring users to remember the
internal flag grammar.

## What shipped

- **Shared workload model** — `WorkloadSpec` and the deterministic parser live
  outside the Slack module so other surfaces can reuse the same input model.
- **Provider seam** — `crate::llm::intent::IntentResolver` abstracts intent
  resolution; Slack is just the first adapter.
- **OpenAI structured-output provider** — the real provider uses the Responses
  API with strict JSON Schema output, env-only `SBGH_OPENAI_API_KEY`, timeouts,
  and no tools.
- **Daemon-owned validation** — model output can only produce a typed intent
  object. The daemon validates target shape, repetitions, warmup, block ranges,
  txid/block-hash normalization, and rejects malformed or ambiguous input.
- **Structured invalid diagnostics** — invalid model output carries bounded,
  typed field issues that Slack can echo back ephemerally to the invoking user.
- **Slack wiring** — Slack authz runs before parser/provider work; explicit
  structured input still takes the deterministic fast path; natural language
  falls through to the resolver when enabled.
- **Eval harness** — representative fixtures exist for real-model evaluation and
  fake-resolver regression tests.

## Validation

- Unit and integration tests covered config, schema drift, OpenAI request/parse
  behavior, daemon validation, Slack connector behavior, authz-before-spend, and
  rate limiting.
- Live Slack validation (2026-06): a natural-language Slack request resolved
  successfully and enqueued the intended benchmark.

## Decisions

1. **Schema or no job.** The provider must return strict schema-valid JSON.
   Anything else is a rejection.
2. **Spec, not args.** The model fills a structured intent object; daemon code
   owns validation and conversion to `WorkloadSpec` / `bench_args`.
3. **Parser fast-path stays.** Natural language is the desired Slack interface,
   but deterministic structured input remains free, local, and provider-free.
4. **Authz before spend.** Team/user authorization happens before provider
   calls.
5. **No model-side tools.** The model cannot fetch refs, inspect repos, or
   trigger work.

## Follow-Ups

- PR-comment natural-language resolution is deferred to
  [`0036`](../../backlog.md).
- Slack modal input can replace explicit flag entry for users who prefer forms.
- `0030-results-qa-agent` should reuse the provider/config layer, but stays a
  separate output-side feature.
