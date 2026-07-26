# v24.2: GitHub and Intent Integration Boundaries

Continuation of
[v24.1](0056-compiler-enforced-crate-boundaries.md).
Finish the partially established GitHub adapter boundary and extract request
intent resolution from the daemon without changing trigger, authorization,
enqueue, or reporting behavior.

> **Status:** shipped — completed locally on 2026-07-26 and continued in
> [v24.3](0060-slack-snapshot-reporting.md).
>
> v24.1 remains shipped. GitHub and intent ownership is now compiler-enforced;
> the credentialed GitHub/Slack smoke remains an explicit deployment check.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0058-github-integration-boundary` | primary: consolidate the GitHub contract and adapter | shipped |
| `0059-intent-resolution-boundary` | co-primary: extract provider-backed request intent resolution | shipped |

## Why

v24.1 moved concrete Octocrab authentication and client code into
`sbgh-github`, but GitHub-specific ports, webhook DTOs, command parsing, and
test support still live in `sbgh-core::github`. That divides one integration
across two crates and leaves the dependency-light domain crate owning
provider-specific concepts.

The Slack request parser has a similar ownership mismatch. Its provider-neutral
workload model is self-contained in `sbgh-daemon::workload`, while the intent
port, schema validation, and OpenAI adapter live under the daemon's vague
`llm` module. Naming this boundary by capability makes the provider replaceable
without turning the daemon into the implementation.

## Target Dependency Graph

Relevant normal dependencies after this iteration:

```text
sbgh-daemon
  ├──> sbgh-core
  ├──> sbgh-github ──> sbgh-core
  └──> sbgh-intent ──> sbgh-core
```

`sbgh-daemon` remains the composition root and sole owner of GitHub side
effects and credentials. A crate boundary does not authorize workers or other
processes to acquire GitHub access.

## Design Rules

- **Core owns provider-neutral domain.** Move `workload.rs` to
  `sbgh-core::workload`; do not create a one-module `sbgh-workload` crate.
- **GitHub owns GitHub concepts.** The API port, command parser, webhook DTOs,
  comparison helper, fake, authentication, and Octocrab adapter belong in
  `sbgh-github`, not `sbgh-core`.
- **The port owns its error contract.** GitHub interfaces return a
  GitHub-owned error/result type rather than `sbgh_core::Error`. Concrete
  Octocrab/auth failures are mapped at the adapter boundary without changing
  caller-visible classification or HTTP behavior.
- **Intent is the capability; OpenAI is an implementation.** The new crate is
  `sbgh-intent`, not `sbgh-llm`. It owns the intent port, structured-output
  schema/validation, provider adapter, and focused test support.
- **Configuration remains composed explicitly.** The daemon may project its
  aggregate configuration into GitHub- and intent-owned constructor inputs.
  Neither extracted crate depends on `DaemonConfig`.
- **This is behavior-preserving.** Existing GitHub API calls, webhook parsing,
  command grammar, allowlists, model prompts, timeouts, fallback behavior,
  observability, and enqueue results remain unchanged.

## Scope

- Move the self-contained request/workload domain into `sbgh-core`.
- Consolidate all existing GitHub-specific contracts and implementations in
  `sbgh-github`.
- Add `sbgh-intent` and move the current intent resolver and OpenAI adapter
  into it.
- Rewire daemon composition and all production/test imports.
- Trim dependencies and extend the Cargo-metadata package-DAG check.
- Update architecture, contributor, and configuration documentation for the
  final ownership.

**Non-goals:** Slack rendering or transport changes; moving Slack into a crate;
v25 worker protocol/event persistence; changing GitHub or intent providers;
changing prompts, models, command syntax, authorization, configuration keys, or
reporting policy; splitting GitHub contracts and Octocrab into separate crates.

## Phases

### Phase 1: Provider-Neutral Workload Domain

**Goal:** Give Slack parsing and intent resolution a shared domain dependency
without either depending on the daemon.

**Scope:**

- Move `sbgh-daemon/src/workload.rs` and its tests to
  `sbgh-core::workload`.
- Re-export only the domain types and resolution functions required by current
  consumers.
- Update daemon, Slack, and intent imports without changing parsing or
  validation.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] `sbgh-core::workload` compiles without daemon, Slack, OpenAI, Reqwest, or
  schema-generation dependencies.
- [x] Existing workload parser/validation tests move intact and pass.
- [x] CLI-like Slack text and structured intent produce the same
  `BenchmarkRequest` values as before.
- [x] `sbgh-daemon` no longer contains a duplicate workload model.

**Tests:**

- Moved `workload.rs` parser, validation, display, comparison, and argument
  conversion tests.
- Existing Slack connector tests that exercise both deterministic parsing and
  resolved intent.

### Phase 2: Complete the GitHub Boundary

**Goal:** Make `sbgh-github` the single owner of GitHub-specific contracts,
DTOs, parsing, test support, and concrete API access.

**Scope:**

- Move `sbgh-core::github::{client,command,webhook,test_support}` into
  `sbgh-github`.
- Keep the fake behind a test/test-support feature so production consumers do
  not acquire test-only machinery.
- Replace the GitHub port's `sbgh_core::Result` use with a GitHub-owned
  error/result contract and preserve boundary mappings.
- Update webhook processing, reporter, admin/API, and test imports.
- Remove `sbgh_core::github` once no consumer remains.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] No GitHub-specific module, webhook DTO, command parser, or GitHub fake
  remains in `sbgh-core`.
- [x] `sbgh-github` owns all Octocrab/JWT dependencies and GitHub-facing
  errors; `sbgh-core` does not reacquire them under any feature.
- [x] Webhook signature verification, payload parsing, command parsing,
  installation-token caching, pagination, comparison, and reporting calls are
  behavior-identical.
- [x] Existing API/status/error mapping and retry classification are preserved
  at daemon boundaries.
- [x] GitHub test consumers use `sbgh-github` test support directly.

**Tests:**

- Moved command, webhook DTO, comparison, client, auth, and fake-client suites.
- Existing webhook processor, GitHub reporter, installer, and admin
  integration suites.
- Dependency checks for `sbgh-core` and `sbgh-github`, including all features.

### Phase 3: Extract `sbgh-intent`

**Goal:** Isolate request-to-intent resolution as a capability with OpenAI as
its current adapter.

**Scope:**

- Create a non-published `sbgh-intent` workspace crate.
- Move `IntentResolver`, structured intent/schema validation, the OpenAI
  adapter, and focused fakes/tests from `sbgh-daemon::llm`.
- Give the crate narrow constructor configuration rather than `DaemonConfig`.
- Keep provider selection and credential ownership in daemon composition.
- Remove the daemon `llm` module after all consumers are rewired.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] `sbgh-intent` depends on `sbgh-core` domain types but not
  `sbgh-daemon`, `sbgh-postgres`, SQLx, Slack transport, or GitHub.
- [x] The daemon selects/configures the provider and injects
  `Arc<dyn IntentResolver>` into Slack composition.
- [x] Prompt text, structured-output schema, validation, timeout, error
  handling, fallback-to-deterministic-parser behavior, and logs remain
  unchanged.
- [x] Secrets remain environment-only and are never placed in domain values,
  logs, fixtures, or worker configuration.

**Tests:**

- Moved intent schema, validation, prompt, and OpenAI adapter tests.
- Slack connector tests for resolver success, invalid output, provider error,
  timeout, disabled resolver, and deterministic-parser fallback.

### Phase 4: Composition, Ratchets, and Documentation

**Goal:** Make the new ownership compiler-visible and leave no stale module or
dependency paths.

**Scope:**

- Wire both crates through daemon composition with narrow configuration
  projection functions.
- Trim workspace and per-package dependencies with Cargo Machete.
- Extend the Cargo-metadata DAG check to cover `sbgh-github` and
  `sbgh-intent` with all features and runtime/build edges.
- Update architecture, test-support, configuration, and contributor docs.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] Cargo metadata matches the documented target graph.
- [x] `sbgh-core` has no normal/build/feature-hidden GitHub, OpenAI, Reqwest,
  JWT, or schema-generation dependency introduced by this work.
- [x] `sbgh-github` and `sbgh-intent` have no dependency on `sbgh-daemon` or
  `sbgh-postgres`.
- [x] `rg` finds no stale `sbgh_core::github`, `sbgh_daemon::workload`, or
  `sbgh_daemon::llm` imports.
- [x] Documentation links and the planning registry pass repository checks.

**Tests:**

- `scripts/check-package-dag.py`
- `scripts/check-docs.py`
- Cargo Machete, Clippy, and rustfmt through `just lint`.

## Final Validation

- [x] `just build --no-sccache`
- [x] `just lint --no-sccache`
- [x] `just test --summary --no-sccache`
- [x] Cargo metadata matches the documented target dependency graph with all
  features.
- [x] Existing GitHub webhook, App authentication, API, reporting, admin, and
  comparison integration tests pass unchanged in observable behavior.
- [x] Existing deterministic and provider-backed Slack intent-resolution tests
  pass unchanged in observable behavior.
- [ ] A local smoke with GitHub and Slack enabled accepts the same request,
  enqueues the same group/runs, and produces the same external side effects as
  the pre-v24.2 build.

## Validation Evidence

Local validation on 2026-07-26:

- The pre-change baseline passed build, lint, and 819 tests with one
  libvirt-host test skipped.
- The completed workspace passes build, lint, Cargo Machete, the all-feature
  package-DAG check, documentation/registry checks, and rustfmt.
- The Docker-backed Nextest run passes 826 tests with one live OpenAI
  evaluation skipped. All moved tests remain present; seven focused
  error-classification, timeout, and composition tests were added.
- Direct dependency inspection confirms `sbgh-core` has no GitHub, Reqwest,
  OpenAI, SQLx, or schema-generation closure, while `sbgh-intent` reaches no
  daemon, PostgreSQL, GitHub, SQLx, or Slack dependency.
- The remaining credentialed smoke would create real GitHub and Slack side
  effects and was not run in the local agent environment.

## Follow-Ups

- [v24.3](0060-slack-snapshot-reporting.md) intentionally
  replaces Slack's
  card/stream/timeline presentation with one replay-safe snapshot message, then
  extracts the smaller Slack integration into `sbgh-slack`.
- [v25](../../iterations/v25-worker-fleet-block-validation.md) remains the owner of worker
  protocol, durable attempt events, and task-neutral reporter projection.
  Neither v24.2 extraction is a load-bearing fleet prerequisite.
