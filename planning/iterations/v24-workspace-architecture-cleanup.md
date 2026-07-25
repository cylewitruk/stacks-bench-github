# v24: Workspace and Architecture Cleanup

Establish an honest Cargo/application boundary and a fleet-ready execution seam
before the repository crosses a process boundary in v25.

> **Status:** in_progress — core implementation is complete; review-driven
> ratchet hardening, clean-checkout CI, and live single-host
> benchmark/build-only smoke remain.
> Started from green trunk `dd16cd3`; v20-v23 remain parked.
>
> This is a behavior-preserving cleanup iteration. It addresses repository
> truth, synthetic integration-test module graphs, accidental dependency
> coupling, and the narrow execution-boundary work that v25 would otherwise
> have to perform while also introducing networking and remote-host failure
> modes.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0053-repository-workspace-cleanup` | repository truth and Cargo/tooling guardrails | in_progress |
| `0054-application-crate-boundaries` | library-first applications and dependency direction | in_progress |
| `0055-execution-boundary-preparation` | fleet-ready in-process execution seam | in_progress |

## Why

The workspace's deployment-oriented crates are directionally sound, and its
test coverage is a major asset. Repeated generative passes have nevertheless
left several boundaries true only by convention:

- `sbgh-daemon` and `sbgh-handler` are binary-only, while integration tests
  compile production files through `#[path = "../src/..."]` in synthetic module
  graphs;
- `sbgh-cli` and `sbgh-handler` pull the broad `sbgh-core` dependency graph for
  small test or edge concerns;
- repository commands and architecture documentation contain stale targets or
  topology;
- workspace-wide dependency features are broader than several consumers need;
- the inline worker is free of direct DB/GitHub calls, but still accepts
  orchestrator-owned `RunnableJob`/`Prepared` values and benchmark-shaped task
  input, so it cannot yet move cleanly into v25's `sbgh-exec`;
- historical slice/phase commentary and very large inline test modules add
  navigation weight, but do not justify a repository-wide cosmetic rewrite.

v24 fixes the load-bearing issues and establishes ratchets. It does not attempt
to make every large file small.

## Scope

- Restore repository-local commands, documentation, Cargo metadata, and CI to a
  truthful, reproducible baseline.
- Make daemon and handler real library-plus-binary applications and test their
  compiled library surfaces.
- Restore the intended CLI and handler dependency direction.
- Finish the narrow in-process execution seam needed by v25: owned execution
  input, task-specific payloads, and dispatch outside scheduler lifecycle code.
- Reduce direct dependency/feature waste after the crate boundaries are fixed.
- Clean historical commentary and oversized inline tests only in files touched
  by the structural work; add a ratchet where it can distinguish stale planning
  narration from durable design references.

**Non-goals:** no worker network protocol, worker registry, remote artifact
transfer, worker-owned leases, or remote execution; no wholesale
`webhook_processor.rs` split; no speculative split of all `JobStore` methods;
no repository-wide test-file or comment rewrite; no canonical-schema project;
no cosmetic `models.rs`/`config.rs` reshuffle beyond ownership cuts required by
the application/execution boundaries; no `sbgh-core` rename; and no behavior
changes to scheduling, benchmarking, reporting, webhook handling, or
persistence semantics.

## Design Rules

- **Move behavior behind real crate boundaries before moving files between
  crates.** v24 keeps execution in-process; v25 changes the process boundary.
- **Composition stays explicit.** `main.rs` parses process concerns and calls a
  library entry point; it does not become a second application implementation.
- **A new abstraction needs two real consumers or a known v25 boundary.** File
  size alone is not sufficient reason to introduce a trait or crate.
- **Task input is discriminated, not an optional-field bag.** Benchmark,
  build-only, and block-validation input must not accrete unrelated fields in
  one flat struct.
- **Internal execution types and wire DTOs are distinct.** v24 prepares an owned
  execution request; v25 decides the serialized protocol representation.
- **Historical comments describe current invariants or link to a live design.**
  Implementation archaeology stays in planning/archive, not production prose.

## Phases

### Phase 1: Repository Truth and Reproducible Guardrails

**Goal:** Make the repository's commands, metadata, CI, and architecture
documentation accurately describe and validate the current workspace.

**Scope:**

- Evaluate Cargo resolver 3 under the pinned Rust toolchain, inspect the
  lockfile/dependency effect, and adopt it if the full workspace remains green.
- Add conservative inherited workspace lints and mark non-publishable internal
  crates `publish = false`; do not introduce a warning-policy migration that
  obscures the structural work.
- Fix or remove stale `just install`/help references to
  `crates/stacks-bench-agent` and ensure every advertised command resolves to a
  real target.
- Replace floating `cargo +nightly fmt` behavior with either a dated/pinned
  formatter toolchain or a stable-compatible rustfmt configuration. Record the
  choice in one place.
- Update architecture documentation to list the actual crates/binaries and
  remove broken source links. Document that the fleet target arrives in v25,
  without describing it as already deployed.
- Add repository-local CI for formatting/linting and the appropriate unit and
  Postgres-backed test tiers. Make Docker requirements explicit and keep local
  `just` commands identical to CI entry points.
- Add a lightweight internal-link/planning-registry check if existing tooling
  does not already catch broken planning and architecture references.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests, if applicable
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] Every documented `just` command references existing packages/targets and
  its `--help` output is current.
- [x] Formatting uses a reproducible toolchain; a developer without a floating
  nightly gets the same result as CI.
- [x] Cargo resolver 3 is either adopted with a reviewed lockfile and green
  suite, or retained resolver 2 has a documented concrete incompatibility and
  follow-up.
- [x] CI runs the repository's standard lint/test entry points and documents
  which tier requires Docker/Postgres.
- [x] Architecture and planning links resolve, and the documented workspace
  inventory matches Cargo metadata.

**Tests:**

- `just build --no-sccache`
- `just lint --no-sccache`
- `just test --summary --no-sccache`
- Cargo metadata/lockfile review and repository link check.

### Phase 2: Library-First Application Crates

**Goal:** Compile and test daemon and handler production code through their real
crate surfaces.

**Scope:**

- Add `src/lib.rs` to `sbgh-daemon` and `sbgh-handler`.
- Keep each `main.rs` thin: process argument/environment handling, tracing,
  dependency construction, signal handling, and one library-level run call.
- Expose the minimum testable library surface; prefer private modules and
  purpose-built constructors over broad `pub` exports.
- Change daemon and handler integration tests to import their package library.
- Remove all production `#[path = "../src/..."]` imports and the dead-code
  allowances they require.
- Add one composition smoke test per application so the production router or
  runtime factory—not a hand-built test approximation—is exercised.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests, if applicable
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] `sbgh-daemon` and `sbgh-handler` each build as a library and binary.
- [x] No integration test includes production Rust source via `#[path]`.
- [x] Integration tests exercise the same router/runtime construction used by
  the binaries.
- [x] Binary startup behavior and configuration precedence remain unchanged.

**Tests:**

- `just test --results -p sbgh-daemon --no-sccache`
- `just test --results -p sbgh-handler --no-sccache`
- Existing handler webhook and daemon job-source/processor/S3/Slack integration
  suites through the library targets.

### Phase 3: Restore Dependency Direction

**Goal:** Make the Cargo graph reflect the intended deployment and trust
boundaries before adding worker crates.

**Scope:**

- Move CLI admin/database integration tests to the crate that owns the admin
  behavior.
- Remove the CLI's `sbgh-core` reexports and normal dependency; remove its
  library target if no genuine library consumer remains.
- Move handler-owned configuration and webhook signature verification out of
  broad core ownership. Keep crypto-provider initialization at an appropriate
  process/bootstrap boundary and preserve its required ordering before any TLS
  client/server construction or handshake.
- Remove the handler's normal `sbgh-core` dependency if the resulting edge
  surface no longer needs it.
- Move dependencies used only by tests to `dev-dependencies`.
- Record intentional remaining `sbgh-core` consumers; do not split or rename the
  core crate merely to improve a package-count metric.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests, if applicable
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] `sbgh-cli` does not transitively depend on SQLx or Octocrab.
- [x] `sbgh-handler` does not transitively depend on SQLx or Octocrab unless a
  reviewed runtime requirement proves unavoidable.
- [x] CLI tests cover command/API-client behavior; core admin tests live with
  the implementation they exercise.
- [x] Handler webhook verification and configuration tests remain complete at
  the handler boundary.
- [x] Each binary installs the selected rustls crypto provider exactly once at
  process bootstrap, before its first TLS use; moving code out of `sbgh-core`
  does not make provider selection implicit or order-dependent.

**Tests:**

- `cargo tree -p sbgh-cli -e normal -i sqlx`
- `cargo tree -p sbgh-cli -e normal -i octocrab`
- `cargo tree -p sbgh-handler -e normal -i sqlx`
- `cargo tree -p sbgh-handler -e normal -i octocrab`
- `just test --results -p sbgh-cli --no-sccache`
- `just test --results -p sbgh-handler --no-sccache`

### Phase 4: Owned Execution Request and Task Dispatch Seam

**Goal:** Make today's inline worker movable without introducing networking or
changing execution behavior.

**Scope:**

- Introduce an owned execution request assembled after orchestrator-side
  preparation. It contains resolved source identity and only the execution
  context needed after handoff.
- Preserve the existing sequencing explicitly: today's inline worker already
  awaits `Prepared` before recipe execution/provisioning, so assembling the
  owned request fully orchestrator-side removes no prepare/provision overlap.
- Remove `RunnableJob` and reporter-owned `Prepared` from the execution function
  signature. Keep the current in-process event sink/channel for v24.
- Separate task payload from backend placement/configuration. Replace the flat,
  benchmark-shaped task input with a discriminated task shape, or rename/narrow
  the existing driver request if it belongs below that layer.
- Move concrete recipe selection out of scheduler lifecycle control flow. A new
  task may require an explicit composition/registration entry, but must not
  require edits to claim, cancellation, event delivery, or terminal handling.
- Introduce the minimum execution-owned configuration needed to stop passing
  aggregate daemon configuration through driver/cache/libvirt construction.
- Preserve the current artifact-store behavior behind an explicit execution
  dependency. Remote upload remains v25 scope.
- Write a dependency-closure test or build-time check that prevents the movable
  execution surface from importing DB, GitHub, Slack, or report-surface types.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests, if applicable
- [x] Reviewed (Codex)
- [ ] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] The executor can run benchmark and build-only tasks from an owned request,
  execution dependencies, event sink, and cancellation token.
- [x] Executor APIs contain no `RunnableJob`, `Prepared`, database, GitHub,
  Slack, or report-surface types.
- [x] Task-specific input is represented by explicit variants/structures rather
  than unrelated optional fields.
- [x] Unknown `(task_kind, build_target)` combinations still fail closed.
- [ ] Existing single-host benchmark, build-only, cancellation, orphan cleanup,
  progress, reporting, and carried-group behavior remain unchanged.

**Tests:**

- Existing runner, recipe, driver, cancellation, orphan-recovery, and reporter
  tests.
- New contract tests around execution-request construction and unsupported task
  dispatch.
- Current single-host end-to-end benchmark smoke after the refactor.

### Phase 5: Dependency Diet and Generative-Debt Ratchet

**Goal:** Finish v24 with a smaller, explainable dependency surface and prevent
the same repository drift from immediately returning.

**Scope:**

- Run an unused-dependency audit after Phases 2–4 and remove confirmed unused
  normal dependencies.
- Replace workspace-wide `tokio = { features = ["full"] }` and broad Reqwest
  features with per-crate requirements where practical.
- Investigate the duplicate Reqwest stacks in `sbgh-smee`; upgrade or isolate
  the event-source dependency only if compatibility and tests support it.
- Externalize oversized inline test modules only in production files materially
  touched by v24, retaining private access through child modules.
- Add small shared test fixtures only where v24 changes expose repeated setup
  maintenance; do not build a generic fixture framework.
- Rewrite stale slice/phase/review narration in touched production files as
  current invariants or links to live design decisions. Add a review/check
  ratchet for newly introduced historical markers without mechanically deleting
  valid compatibility notes.
- Record before/after package counts and build/lint timing as evidence, not as
  release gates.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests, if applicable
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] `cargo machete` (or the adopted equivalent) reports no unexplained unused
  normal dependencies.
- [x] Runtime crates enable only the Tokio/Reqwest features they use, with any
  deliberate broad feature set documented.
- [x] No new production comment narrates an implementation slice/review instead
  of a current contract.
- [x] Test relocation and comment cleanup produce no behavior-only churn outside
  files already touched by the iteration.

**Tests:**

- `just build --no-sccache`
- `just lint --no-sccache`
- `just test --summary --no-sccache`
- Dependency audit and before/after `cargo tree` snapshots for application
  crates.

## Final Validation

- [x] `just build --no-sccache`
- [x] `just lint --no-sccache`
- [x] `just test --summary --no-sccache`
- [x] The execution-closure guard derives or verifies its file coverage,
  rejects aggregate configuration and all relevant DB/GitHub/runtime-client
  dependencies, and cannot be bypassed by a forgotten hardcoded entry.
- [x] Documentation validation checks heading fragments and reference-style
  local links, not only target-file existence.
- [x] CI installs the toolchain pinned by `rust-toolchain.toml` and identifies
  the Docker/Postgres-backed test tier explicitly.
- [x] A per-binary tripwire enforces exactly one rustls provider installation
  before configuration or TLS-client construction.
- [x] Execution-closure scanning remains complete when a production file
  contains a mid-file `#[cfg(test)]` item.
- [ ] CI passes from a clean checkout.
- [x] No production `#[path = "../src/..."]` integration-test imports remain.
- [x] CLI and handler dependency assertions pass.
- [ ] A current single-host benchmark and build-only/cache-warm job complete and
  report identically to pre-v24 behavior.
- [x] The v25 execution surface has an explicit dependency-closure map and no
  unresolved DB/GitHub/Slack coupling.

## Validation Evidence

Local validation on 2026-07-25:

- Workspace topology remained six packages and four binaries; every package is
  explicitly non-publishable.
- Warm runs completed in 13.08 seconds for `just build --no-sccache` and 3.83
  seconds for `just lint --no-sccache`.
- The full Nextest workspace run passed all 830 tests with one skipped. Focused
  runs passed 533 daemon tests (one skipped), 17 handler tests, and 12 CLI
  tests.
- A source-only snapshot with no `.git` or `target` directory completed
  `just build`, `just lint`, and the full Docker-backed `just test` entry
  points. The clean build took 69 seconds, lint took 34.35 seconds, and all 830
  tests passed.
- Cargo resolver 3 is active, the lockfile was reviewed, the documented
  topology/link checks pass, and `cargo machete --with-metadata` reports no
  unused dependencies.
- CLI and handler normal dependency graphs contain no SQLx or Octocrab edge.
  `sbgh-smee` intentionally retains Reqwest 0.12 through
  `reqwest-eventsource` 0.6 and Reqwest 0.13 for its direct client.
- `execution.rs` owns the worker request/task/dependency contract. A
  syntax-aware integration test derives the production closure from module
  declarations and local imports, rejects orchestrator dependencies, and
  proves that a new import is discovered and a mid-file test item cannot hide
  later production code.
- Artifact-store construction now consumes an execution-owned projection
  rather than `DaemonConfig`. Documentation validation checks GitHub-style
  heading anchors and reference links, while the workspace inventory check
  enforces one first-statement rustls provider installation in every binary.
- The CI workflow parses successfully and an `act` pull-request dry run
  resolves the pinned-toolchain, build, lint, and Docker-backed test steps.
- Clean-checkout CI has not yet been observed. This development machine has no
  libvirt/QEMU executable or configured benchmark host, so the live benchmark
  and build-only/cache-warm smoke remains a host-side acceptance check.

## Follow-Ups

- `0004-worker-fleet` and `0019-block-validation-recipe` proceed in v25.
- Webhook vertical decomposition remains demand-driven; trigger matching should
  move to a neutral policy module when that area is next changed.
- General `JobStore` segregation remains gated on the fleet scheduler's actual
  transactional needs.
- Broad comment/test-file cleanup stays opportunistic rather than becoming a
  standalone iteration.
