# v24.1: Compiler-Enforced Crate Boundaries

Continue [v24](0053-workspace-architecture-cleanup.md) by
replacing its transitional source-analysis ratchet with Cargo crate boundaries
before execution crosses a process boundary in v25.

> **Status:** shipped — implementation and all locally available validation
> completed on 2026-07-25. Hosted clean-checkout CI and live libvirt-host parity
> remain deployment checks because this development environment has neither a
> CI runner nor a configured benchmark host.
>
> v24 established an owned in-process execution request and removed direct
> DB/GitHub/Slack/reporting coupling. v24.1 turns that seam into explicit
> driver-interface, worker, and libvirt crates, then separates the remaining
> PostgreSQL and GitHub adapters from `sbgh-core`. Scheduling, execution,
> persistence, and reporting behavior remain unchanged.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0056-compiler-enforced-execution-boundaries` | primary: worker contracts and libvirt adapter | shipped |
| `0057-core-adapter-boundaries` | secondary: dependency-light core and concrete adapters | shipped |

## Why

v24 deliberately kept execution inside `sbgh-daemon`. Its syntax-aware
execution-boundary test is a transitional architecture check because Rust does
not enforce dependency direction between modules in one crate. The test is
useful, but it approximates module resolution and encodes forbidden imports by
name. Cargo should enforce this boundary instead.

The current execution surface also has four concrete leaks that must be fixed
rather than copied into new crates:

- `Driver::binary_cache()` exposes a concrete cache service through a backend
  interface;
- libvirt consumes the full artifact-store interface instead of a narrow
  worker-side staging/upload port;
- libvirt resolves default benchmark arguments instead of receiving a fully
  specified task;
- libvirt and the binary cache still import configuration, memory, and argument
  helpers from `sbgh-core`.

Separately, `sbgh-core` mixes domain types and ports with SQLx, Octocrab,
Reqwest, JWT, migrations, direct admin queries, and adapter-specific errors.
That cleanup is larger and must not block the execution split, but the desired
crate graph should be established before v25 adds protocol and remote-host
failure modes.

## Target Dependency Graph

Arrows point from a crate to its allowed project dependencies:

```text
sbgh-daemon
  ├──> sbgh-core
  ├──> sbgh-postgres ──> sbgh-core
  ├──> sbgh-github   ──> sbgh-core
  ├──> sbgh-driver                    (transitional)
  ├──> sbgh-libvirt ──> sbgh-driver   (transitional)
  └──> sbgh-worker                    (transitional)
         ├──> sbgh-driver
         └──> sbgh-libvirt ──> sbgh-driver
```

All three daemon execution edges are intentionally transitional. v24.1 keeps
the worker library in-process, while the daemon directly names `sbgh-driver`
request/event types because it constructs execution-request data and consumes
the event stream. It also projects aggregate daemon configuration into
`sbgh-libvirt` configuration and supplies the host shell at the composition
root. Driver, cache, and artifact service composition remains worker-owned.
v25 adds `sbgh-proto` and the `sbgh-worker` binary, moves driver-type conversion
and backend composition worker-side, and removes all three daemon execution
edges when the processes communicate only through the versioned protocol.

## Scope

- Add a dependency-light `sbgh-driver` crate containing the internal driver API,
  not wire DTOs or infrastructure.
- Add `sbgh-libvirt` as the concrete libvirt execution adapter.
- Add `sbgh-worker` as an in-process library owning dispatch, recipes, execution
  orchestration, and worker-side service composition.
- Resolve task defaults and normalization before constructing the execution
  request; the backend receives the exact arguments it executes.
- Replace concrete cache/artifact accessors with explicit, narrow worker-owned
  dependencies.
- Give libvirt and cache configuration clear owners and eliminate execution
  imports from `sbgh-core`.
- Delete the source-level execution-boundary ratchet after the crate DAG
  enforces the same rule; retain a lightweight Cargo-metadata assertion for the
  allowed package graph.
- Move PostgreSQL implementations, migrations, and row mappings into
  `sbgh-postgres`.
- Move GitHub authentication and concrete API implementation into
  `sbgh-github`.
- Leave domain types, business policy, and daemon-side ports in a
  dependency-light `sbgh-core`.
- Transfer v24's outstanding hosted-CI and live single-host parity checks to the
  final v24.1 topology.

**Non-goals:** no worker protocol or network transport; no worker registry,
leases, remote artifacts, or remote execution; no block-validation
implementation; no database schema or persistence-semantics change; no
`JobStore` interface segmentation; no cache algorithm or artifact lifecycle
redesign; no configuration-file UX change; no task/reporting behavior change;
and no broad dependency upgrades.

## Design Rules

- **Cargo owns architecture enforcement.** Execution crates cannot list
  `sbgh-core`, SQLx, Octocrab, Axum, Slack, or daemon/reporting crates as normal
  dependencies.
- **The driver API stays narrow.** `sbgh-driver` contains the minimum types and
  ports needed by both worker orchestration and backend adapters. It contains
  no concrete cache, artifact store, config parser, libvirt implementation, or
  worker wire DTO.
- **Inputs are complete at handoff.** The daemon resolves source identity,
  defaults, normalization, and effective benchmark arguments before creating
  the owned execution request. The workload key and executed argument sequence
  derive from the same resolved tokens.
- **Services are composed, not discovered.** Worker composition owns the driver,
  cache control, and artifact sink. A driver does not return concrete services
  through capability/accessor methods.
- **Configuration follows the implementation.** Libvirt-owned configuration
  lives with `sbgh-libvirt`; worker/cache configuration lives with
  `sbgh-worker`. Daemon composition performs explicit conversion from the
  existing aggregate configuration.
- **Driver types are not protocol DTOs.** v25 defines versioned, serializable
  `sbgh-proto` messages and validates conversion into v24.1's internal
  execution types.
- **Adapter extraction preserves behavior.** SQL, transaction boundaries,
  GitHub calls, cache behavior, artifact keys, task ordering, cancellation,
  cleanup, and reporting remain unchanged while ownership moves.
- **Core purity is not an execution prerequisite.** Phases 1–4 establish the
  worker/libvirt boundary without waiting for Phase 5's larger core-adapter
  extraction.

## Phases

### Phase 1: Driver API and Service Ownership

**Goal:** Define a small internal execution API without carrying concrete
daemon services across the new boundary.

**Scope:**

- Create `sbgh-driver`.
- Move the backend-neutral driver, task context/specification, placement,
  outcome, cancellation, and event-sink types and ports.
- Define execution-local error types only where callers require structured
  classification; do not import `sbgh_core::Error`.
- Remove `Driver::binary_cache()`.
- Compose cache control separately from the driver so the worker and backend
  can share one implementation without using the driver as a service locator.
- Narrow libvirt's artifact dependency to the staging/read operations it
  actually requires. The orchestrator retains accepted-terminal promotion and
  consumer-facing artifact resolution.
- Resolve configured defaults and repetition normalization before task handoff.
  Carry exact effective benchmark argument tokens in the task specification.
- Keep human-readable configuration helpers such as `MemorySize` out of the
  driver crate unless its public API genuinely requires them.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests, if applicable
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] `sbgh-driver` contains no concrete backend, cache, artifact store,
  configuration parser, channel implementation, or protocol DTO.
- [x] `Driver` exposes no `BinaryCache`, `ArtifactStore`, daemon config, DB,
  GitHub, Slack, or reporting type.
- [x] Worker composition receives driver, cache-control, and artifact
  dependencies explicitly.
- [x] The exact benchmark argument tokens used for the workload key are handed
  to execution; libvirt performs no default selection or repetition policy.
- [x] Unknown task/target combinations continue to fail closed without touching
  a backend.

**Tests:**

- Contract tests for benchmark/build-only task construction and unsupported
  dispatch.
- Argument-resolution tests proving workload-key input and executed tokens are
  identical.
- Fake driver/cache/artifact tests proving composition uses the injected
  services.

### Phase 2: Libvirt Adapter Crate

**Goal:** Move the concrete VM backend behind a compiler-enforced adapter
boundary.

**Scope:**

- Create `sbgh-libvirt` and move the production `libvirt/` modules and their
  tests without duplicating implementations.
- Move or project libvirt-owned path, VM, LVM, memory, timeout, service-user,
  and tool-binary configuration into `sbgh-libvirt`.
- Remove libvirt dependencies on `sbgh_core::config`,
  `sbgh_core::bench_args`, `sbgh_core::memory`, and `sbgh_core::models`.
- Preserve the existing shell abstraction, provisioning order, cancellation
  safe points, teardown, forensics, cache-hit path, and S3-with-local-mirror
  behavior.
- Ensure test fixtures use driver-API and libvirt-owned values rather
  than daemon jobs or core models.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests, if applicable
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] `sbgh-libvirt` implements the execution `Driver` contract and contains
  the sole production libvirt implementation.
- [x] `cargo tree -p sbgh-libvirt -e normal -i sbgh-core` has no dependency
  path.
- [x] `sbgh-libvirt` has no normal dependency on SQLx, Octocrab, Axum, Slack, or
  daemon/reporting crates.
- [x] Libvirt accepts owned backend configuration and fully resolved task
  inputs; it does not consume `DaemonConfig` or another aggregate app config.
- [x] Existing libvirt unit tests run through the package's real library
  surface without `#[path]` source inclusion.

**Tests:**

- Existing libvirt driver, provisioning, cache, progress, cancellation,
  teardown, and forensics tests after relocation.
- Focused construction test using the production `LibvirtDriver` factory.
- Artifact-port tests with a fake/local adapter; S3-with-local-mirror remains a
  daemon/worker composition and final host-smoke check.

### Phase 3: In-Process Worker Library

**Goal:** Give execution orchestration its final application owner before
changing its process placement.

**Scope:**

- Create the `sbgh-worker` library.
- Move owned execution request handling, task dispatch, recipes, execution
  orchestration, and worker-side cache/artifact composition out of
  `sbgh-daemon`.
- Keep the daemon as the in-process caller for v24.1. It prepares the job,
  resolves source/task inputs, invokes the worker library, and consumes events
  exactly as it does today.
- Preserve runner slot accounting, prepare-before-provision ordering,
  cancellation, orphan cleanup, carried-group behavior, cache pinning, and
  terminal reporting.
- Add a real worker composition test with injected fake adapters.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests, if applicable
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] `sbgh-worker` owns execution dispatch and recipes; `sbgh-daemon` owns
  claim/preparation, scheduling, DB state, GitHub/Slack effects, and reporting.
- [x] `cargo tree -p sbgh-worker -e normal -i sbgh-core` has no dependency path.
- [x] `sbgh-worker` has no normal dependency on SQLx, Octocrab, Axum, Slack, or
  daemon/reporting crates.
- [x] The daemon calls the real worker library in-process; no duplicate or
  compatibility execution path is introduced.
- [x] Benchmark, build-only, cache-warm, cancellation, orphan recovery,
  progress, terminal reporting, and carried-group tests remain behaviorally
  unchanged.

**Tests:**

- Worker composition and dispatch integration tests.
- Existing runner, recipe, cache/pin, cancellation, orphan-recovery, event, and
  reporter suites through the new crate surfaces.
- Dependency-tree assertions for `sbgh-worker`.

### Phase 4: Replace the Transitional Ratchet

**Goal:** Make the crate graph the durable enforcement mechanism.

**Scope:**

- Delete the syntax-aware `execution_boundary.rs` test once Phases 1–3 compile
  and pass through real crate boundaries.
- Remove its `syn` dev-dependency if no longer used.
- Add a small Cargo-metadata check for the allowed project-crate DAG and
  forbidden normal dependencies.
- Update architecture and fleet planning to use
  `sbgh-driver`, `sbgh-libvirt`, and `sbgh-worker`; remove `sbgh-exec` from the
  target design.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests, if applicable
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] A forbidden worker/libvirt dependency fails the Cargo-metadata check, and
  code cannot import a daemon module because it is outside the crate.
- [x] Adding a new backend requires implementing the contract in an adapter
  crate, not extending a source-scanner allow/deny list.
- [x] No source-level execution-closure scanner or hardcoded file inventory
  remains.
- [x] Repository documentation contains no planned `sbgh-exec` crate.

**Tests:**

- Repository metadata/dependency-DAG check.
- `just lint --no-sccache`.

### Phase 5: Core Adapter Boundaries

**Goal:** Make `sbgh-core` primarily domain policy and ports, with concrete
persistence and GitHub integrations owned by adapter crates.

**Scope:**

- Split the god error into context-specific core, persistence, GitHub, and
  application errors before moving concrete adapters.
- Create `sbgh-postgres`; move the pool, migrations, SQLx implementations,
  adapter-specific row types, and row/domain conversions.
- Remove SQLx derives and `sqlx::types::Json` from core domain types. Preserve
  SQL shape and conversion semantics in the Postgres adapter.
- Move direct SQL in `admin` modules to the Postgres/application boundary
  without redesigning existing store interfaces or transaction semantics.
- Create `sbgh-github`; move GitHub App authentication, token caching, Octocrab
  implementation, and Reqwest/JWT-specific errors.
- Keep GitHub-facing domain values and the `GitHubApi` port in `sbgh-core` only
  where they are genuine daemon-domain contracts.
- In-memory stores remained behind the testing feature for this boundary
  refactor and were subsequently retired in favor of production Postgres
  persistence tests and narrow orchestration fakes.
- Update daemon composition to construct the concrete Postgres and GitHub
  adapters.

**Status:**

- [x] Core implementation
- [x] Unit/integration tests, if applicable
- [x] Reviewed (Codex)
- [x] Validated — the acceptance checks below were run

**Acceptance & Validation:**

- [x] `sbgh-core` has no normal dependency on SQLx, Octocrab, Reqwest, or
  `jsonwebtoken`.
- [x] `sbgh-postgres` owns all SQLx derives, migrations, pool aliases, queries,
  and database-specific error conversion.
- [x] `sbgh-github` owns concrete GitHub authentication/client dependencies and
  implementation.
- [x] Existing store and GitHub port shapes remain intact unless an extraction
  cannot compile without a narrowly documented ownership correction.
- [x] Database schema, SQL transaction boundaries, GitHub API behavior, and
  admin command behavior remain unchanged.
- [x] Test doubles do not force production `sbgh-core` to depend on SQLx or
  concrete GitHub clients.

**Tests:**

- Existing Postgres migration/store/admin integration suites against
  `sbgh-postgres`.
- Existing GitHub client/auth and fake-client suites against their new owners.
- Dependency-tree assertions for `sbgh-core`, `sbgh-postgres`, and
  `sbgh-github`.

## Final Validation

- [x] `just build --no-sccache`
- [x] `just lint --no-sccache`
- [x] `just test --summary --no-sccache`
- [x] Cargo metadata matches the documented target dependency graph.
- [x] `sbgh-worker` and `sbgh-libvirt` have no `sbgh-core` dependency.
- [x] `sbgh-core` has no SQLx, Octocrab, Reqwest, or `jsonwebtoken` normal
  dependency.
- [x] No production execution implementation remains in `sbgh-daemon`.
- [x] No source-level execution-boundary ratchet remains.
- [ ] CI passes from a clean checkout using the pinned toolchain and
  Docker-backed test tier.
- [ ] On the current single-host deployment, a benchmark and a
  build-only/cache-warm job complete with the same task ordering, progress,
  artifacts, cache behavior, terminal reporting, and carried-group behavior as
  pre-v24.
- [ ] The live smoke includes the S3-with-local-mirror artifact configuration.

## Validation Evidence

Local validation on 2026-07-25:

- `just build --no-sccache`, `just lint --no-sccache`, and the Docker-backed
  `just test --summary --no-sccache` entry points pass.
- The full Nextest run passes 831 tests with one libvirt-host test skipped.
  The two calibration-parser tests moved with the production implementation;
  the only removed tests are the three obsolete source-ratchet self-tests.
- Pre-commit review removed a new workload-key mismatch failure path while
  retaining token/fingerprint equivalence coverage, made Postgres domain-row
  mapping reject duplicate columns and unsupported types, moved cache
  environment discovery out of `sbgh-driver`, and narrowed libvirt's public
  API. v25 now explicitly persists effective arguments for immutable leases.
- Cargo metadata matches the documented eleven-crate DAG. Direct dependency
  inspection confirms that `sbgh-worker` and `sbgh-libvirt` do not reach
  `sbgh-core`, and that `sbgh-core` does not reach SQLx, Octocrab, Reqwest, or
  `jsonwebtoken`.
- The documentation/registry check, dependency ratchet, Cargo Machete,
  Clippy with warnings denied, and rustfmt check all pass. The dependency
  ratchet resolves all features and rejects forbidden runtime or build
  dependency edges.
- Hosted clean-checkout CI and the live benchmark/build-only/cache-warm smoke,
  including S3 with a local mirror, require external infrastructure and remain
  explicit deployment checks above.

## Follow-Ups

- v25 adds `sbgh-proto`, worker registration/leases, the `sbgh-worker` binary
  process, and durable remote events/artifacts. After parity validation it
  removes the daemon's transitional `sbgh-worker`, `sbgh-driver`, and
  `sbgh-libvirt` dependencies; request/event conversion and backend composition
  become worker-side.
- `JobStore` interface segmentation remains gated on the fleet scheduler's
  actual transactional needs.
- Additional backend/task crates are introduced only with a concrete worker
  capability; block validation is the first planned case.
