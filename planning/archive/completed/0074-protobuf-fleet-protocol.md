# v29: Protobuf Worker Protocol

Successor to
[v28](0066-task-aware-reporting.md). Establish protobuf/gRPC as the first
deployed worker control plane, remove the undeployed JSON prototype, and
preserve the pull scheduler and attempt state machine.

> **Status:** shipped — implementation and local validation completed on
> 2026-07-30.
>
> v29 establishes the transport for the current centrally coordinated fleet.
> Daemon and workers continue to require one exact protocol version.
> Rolling multi-version compatibility is deferred to
> `0075-rolling-worker-protocol-compatibility` and is required before the first
> incompatible change to the published protobuf contract.

## Item

- **id:** `0074-protobuf-fleet-protocol`
- **status:** `shipped`
- **priority:** `high`
- **depends_on:** `0004-worker-fleet`, `0017-generic-phase-events`,
  `0062-sandboxed-worker-execution`
- **relates_to:** `0075-rolling-worker-protocol-compatibility`,
  `0067-github-block-validation-submission`,
  `0078-slack-task-submission`, `0070-slack-block-validation-controls`
- **decision:** [0004-protobuf-fleet-protocol](../../decisions/0004-protobuf-fleet-protocol.md)
- **source:** external worker-operator deployment and protocol review
  (2026-07)

## Shipped

Removed the worker's prototype HTTP/1.1 JSON control client and the daemon's
prototype Axum fleet routes in favor of the generated
`sbgh.fleet.v1.WorkerFleetService` Tonic client/server over HTTP/2 and the TLS
1.3 mutual-X.509 listener. The
schema covers every fleet operation and task/result union; generated messages
are non-`Debug`, bounded at the transport, and converted before entering the
application service. Stable gRPC status details preserve machine-readable
error, retry, and delay behavior. Artifact contents remain delegated directly
over presigned HTTPS.

Added `sbgh-fleet` as the dependency-light owner of transport-neutral fleet
values, validation, and semantic digests. This makes the generated `sbgh-proto`
crate a strict wire adapter while allowing
`sbgh-core` and `sbgh-postgres` to consume fleet contracts without importing
Prost or Tonic. Package-DAG and source-boundary checks prevent generated
messages or a second JSON fleet transport from crossing that edge.

The first protobuf contract uses protocol revision 1 and adds reproducible
vendored-`protoc` generation, Buf schema checks in CI, protobuf-byte round trips
for every RPC message and union, all-RPC generated client/server coverage,
behavior-focused digest tests, structured-error tests, and expanded TLS/plaintext
rejection coverage. Local validation completed with workspace build/lint, 910
passing tests (one environment skip), and `git diff --check`. The real-host
benchmark and block-validation canaries remain rollout gates because they
require the deployed daemon, object store, PKI, and libvirt workers.

## Problem

The undeployed prototype `sbgh-proto` contains handwritten Serde DTOs. Its
worker calls an Axum HTTP/1.1 JSON API through a manual Reqwest client. The
crate name suggests protobuf, but Rust structs and incidental Serde behavior
are the only wire schema.

The immediate need is smaller than a mature rolling-upgrade system. One operator
currently controls the central daemon and one Hetzner worker; a second operator
will install the same current worker release. We first need to establish and
prove the intended protobuf/gRPC protocol on the existing host. Supporting
multiple simultaneous protocol revisions can build on that real published
schema later.

## Outcome

```text
sbgh-worker
  └── outbound TLS 1.3 mTLS
        └── generated sbgh.fleet.v1 gRPC client
              └── register / poll / accept / heartbeat / events / complete
                    └── existing lease + fence + cleanup state machine

sbgh-daemon
  └── generated sbgh.fleet.v1 gRPC server
        ├── certificate identity + registry authorization
        ├── existing fleet application/store behavior
        └── exact protocol-version validation

artifact bytes
  worker ── presigned HTTPS PUT/GET ── object store
```

`sbgh-proto` becomes an actual protobuf crate. The `.proto` schema is the sole
wire source of truth. Generated messages stop at daemon and worker transport
adapters; neither persistence nor execution-domain crates depend on them.
`sbgh-fleet` owns the shared transport-neutral application values so those
inner crates do not acquire Prost or Tonic.

The first deployment installs the daemon and matching worker release, runs one
benchmark and one block-validation canary, then installs the same worker
release on additional hosts.

## Design Rules

- **Preserve pull scheduling.** Every control operation remains
  worker-initiated. `Poll` is a unary long-poll; v29 adds no daemon-pushed work
  or bidirectional assignment stream.
- **Preserve the state machine.** Offers, attempts, leases, fencing generation,
  cancellation precedence, reliable sequence acknowledgement, terminal-prefix
  validation, artifact promotion, cleanup obligations, and worker-restart
  behavior do not change.
- **Use one wire schema.** `.proto` files define requests, responses, unions,
  errors, and service methods. Do not keep a parallel handwritten JSON DTO
  graph.
- **Keep generated types at the boundary.** `sbgh-daemon` and `sbgh-worker`
  convert generated messages into their existing application values.
  `sbgh-core`, `sbgh-driver`, `sbgh-postgres`, recipes, and reporting import no
  generated protobuf types.
- **Keep exact protocol matching for v29.** Registration rejects any revision
  other than the first protobuf revision. The accepted revision belongs to the
  session; later requests bind the session or a fenced attempt. Worker
  `software_version` remains telemetry and may differ without changing the wire
  version.
- **Protobuf is not validation.** Port the current UUID, sequence, timestamp,
  string/list, hash, task-payload, artifact, and terminal-result bounds. Required
  enums reject `UNSPECIFIED`; omitted correctness-sensitive values fail closed.
- **Do not preserve prototype wire shape.** Use protobuf-native request and
  response shapes. Keep application behavior unchanged, but do not carry
  repeated version fields, JSON-only serialization, field numbers, or digest
  constants for an undeployed transport.
- **Keep semantic hashes.** Payload and terminal digests bind offers,
  assignments, reliable events, and completion within the live attempt
  protocol. Test determinism and semantic sensitivity; never hash serialized
  protobuf bytes.
- **Preserve mTLS authorization.** TLS 1.3, server verification, client-CA
  verification, the sole worker URI SAN, client/server EKUs, and
  registry-bound capabilities remain mandatory. Common Name and request fields
  remain invalid identity fallbacks.
- **Keep secrets out of diagnostics.** Lease tokens and repository credentials
  remain opaque and redacted. Generated token-bearing messages must not create
  a new secret-bearing `Debug` or error path.
- **Keep artifacts out of gRPC.** gRPC carries delegated grants and verified
  metadata. Artifact contents continue over presigned HTTPS with exact keys,
  sizes, and SHA-256 verification.
- **Do not enable automatic RPC retries.** Preserve the current explicit worker
  retry/reconnect loops and application idempotency behavior. A later change may
  enable per-method gRPC retries only after proving the method safe.
- **One production transport.** Remove the prototype JSON fleet routes and
  control client before first deployment rather than carrying a dual stack.

## Stable Service Shape

The first schema package is `sbgh.fleet.v1`, with one `WorkerFleetService`:

| RPC | Semantics |
| --- | --- |
| `Register` | bind certificate worker identity, exact protocol version, capabilities, resources, and process session |
| `Poll` | bounded unary long-poll returning no-work, drain, or one offer |
| `Accept` | atomically accept a current offer and return its immutable assignment |
| `FetchRepositoryCredential` | mint a short-lived credential for the current fenced attempt |
| `Heartbeat` | renew the lease and return continue/cancel/drain plus reliable acknowledgement |
| `PublishReliableEvent` | idempotently append one gap-free phase/terminal envelope |
| `PublishProgress` | accept one best-effort bounded progress sample |
| `GrantArtifact` | authorize one exact-key delegated PUT/GET |
| `CompleteAttempt` | verify terminal prefix and artifact manifest, then accept terminal state |
| `ListCleanup` | list cleanup obligations for the current worker session |
| `CompleteCleanup` | acknowledge verified idempotent cleanup |
| `Deregister` | close an idle or draining session |

Use a small protobuf `FleetErrorDetail` in gRPC status details to define the
stable error code, retryability, and optional retry delay consumed by the
worker. Human text is diagnostic and must not drive worker behavior.

Protocol revision 1 is exchanged during registration and persisted on the
worker session. It is not tied to the Cargo package or worker software version.
Later RPCs bind that session or a fenced attempt and do not repeat the revision.
No worker-session schema migration or version negotiation is required.

## Target Source Layout

```text
proto/
  sbgh/fleet/v1/fleet.proto       authoritative messages and service
  buf.yaml                        schema lint rules

crates/sbgh-fleet/
  src/
    model.rs                      transport-neutral fleet values
    validate.rs                   fail-closed application validation
    digest.rs                     canonical semantic digest contract

crates/sbgh-proto/
  build.rs                        pinned Prost/Tonic generation
  src/
    lib.rs                        generated module exports
    convert.rs                    protobuf ↔ fleet-value conversion
    error.rs                      typed gRPC status details

crates/sbgh-daemon/src/fleet/
  service.rs                      transport-neutral fleet operations
  grpc.rs                         Tonic adapter + authenticated peer binding

crates/sbgh-worker/src/
  transport.rs                    generated control client + artifact HTTP client
```

Pin `protoc`, Prost, and Tonic through the workspace so builds do not depend on
an arbitrary host toolchain. Generated Rust lives in Cargo `OUT_DIR`; only the
schema and generator configuration are committed.

## Phases

### Phase 1: Protobuf Schema and Generated Boundary

**Goal:** Define the first deployed worker contract as one generated protobuf
service without changing application behavior.

**Scope:**

- Define every fleet operation, response, union, task payload, event, artifact
  descriptor, terminal result, and error in
  `sbgh.fleet.v1.WorkerFleetService`.
- Assign the initial stable field numbers and enum values.
- Add reproducible Prost/Tonic generation and schema linting.
- Establish protocol revision 1 at registration and on the durable session.
- Implement common wire validation and dependency-free primitive helpers.
- Add daemon/worker conversion adapters without importing generated messages
  into inner crates.
- Keep semantic digests transport-neutral and test their determinism and
  sensitivity rather than prototype digest constants.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [ ] Every fleet operation and task/result variant has one protobuf
  representation.
- [ ] Schema lint passes and code generation is reproducible without a
  host-installed `protoc`.
- [ ] Round trips cover every message and `oneof` variant.
- [ ] Validation bounds have positive and negative protobuf fixtures.
- [ ] Payload/event/terminal digests are deterministic, change with semantic
  input, and never use encoded protobuf bytes.
- [ ] Generated secret-bearing values are absent from normal debug/error
  output.
- [ ] Package-DAG checks prove generated messages remain at transport
  boundaries.

**Tests:**

- Schema/generator checks.
- Message conversion and validation tests.
- Semantic-digest and secret-redaction tests.

### Phase 2: Tonic Daemon Service

**Goal:** Serve the existing fleet behavior through the generated gRPC server.

**Scope:**

- Extract Axum-specific fleet handler behavior into one transport-neutral
  application service; do not duplicate store, scheduler, or coordinator logic.
- Implement the generated Tonic server as a thin peer-authentication,
  validation, conversion, deadline, and status adapter.
- Preserve the existing Rustls TLS 1.3 configuration and URI-SAN worker UUID
  extraction. Require HTTP/2 ALPN on the dedicated fleet listener.
- Preserve request/message limits, global concurrency limits, bounded poll
  duration, and the single-active-poll guard.
- Map existing typed fleet failures onto gRPC status and `FleetErrorDetail`.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [ ] One application service owns fleet behavior; there is no parallel
  JSON/gRPC scheduler, coordinator, or persistence implementation.
- [ ] Missing, untrusted, wrong-EKU, wrong-SAN, multiple-identity, mismatched
  request/certificate, and TLS-below-1.3 clients are rejected before mutation.
- [ ] Plaintext and HTTP/1.1 clients cannot reach the worker service.
- [ ] Protocol mismatch is rejected before session registration or work
  assignment.
- [ ] Poll disconnect/timeout releases its active-poll guard and does not strand
  an offer.
- [ ] Message limits and deadlines fail without leaking a lease or partially
  accepting terminal state.

**Tests:**

- Tonic mTLS identity matrix.
- Transport-neutral service/store tests.
- Poll timeout/disconnect/concurrency tests.
- Oversize-message and structured-error tests.

### Phase 3: Generated Worker Client

**Goal:** Move the worker control plane to gRPC without changing execution or
artifact transfer.

**Scope:**

- Replace the manual Reqwest JSON control client with the generated Tonic
  client.
- Keep the delegated artifact Reqwest client separate.
- Preserve explicit deadlines, reconnect/backoff, reliable resend, server
  acknowledgement, process-restart fencing, cancellation, and cleanup.
- Convert assignments to the same `sbgh-driver::ExecutionRequest` values as
  before.
- Keep automatic gRPC retries disabled.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [ ] Benchmark, build-only, and block-validation assignments produce the same
  execution requests and outcomes as the JSON transport.
- [ ] Lost responses and explicit retries do not duplicate registration,
  acceptance, reliable events, completion, artifact promotion, cleanup, or
  deregistration.
- [ ] Same-process reconnect resumes from the highest contiguous reliable
  acknowledgement.
- [ ] Worker-process restart creates a new session and fences the old attempt.
- [ ] Artifact bytes never transit gRPC or the daemon.

**Tests:**

- Client/server integration tests for every RPC.
- Existing duplicate, conflict, stale-fence, resend, and restart tests.
- Presigned artifact PUT/GET regression tests.

### Phase 4: First Deployment and Prototype Removal

**Goal:** Make protobuf/gRPC the only worker transport before the first fleet
deployment.

**Scope:**

- Deploy the gRPC daemon and matching worker release.
- Run one real benchmark and one real block-validation canary.
- Remove the prototype Axum worker routes, handwritten JSON DTOs, and Reqwest
  control client.
- Update architecture, setup, worker operations, configuration, packaging, and
  health/firewall checks for HTTP/2 gRPC.
- Add boundary checks preventing a second fleet transport or wire model from
  returning.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [ ] No production JSON worker route or JSON control client remains.
- [ ] `sbgh-proto` contains generated protobuf messages plus conversion/error
  adapters, while `sbgh-fleet` owns validation and semantic digests; no
  handwritten mirror protocol remains.
- [ ] Current docs describe protobuf/gRPC and coordinated exact-version
  deployment.
- [ ] Worker PKI, certificate identities, registry policy, and firewall
  exposure apply unchanged to the generated gRPC service.
- [ ] Additional operators can install the same worker release and register
  only their pre-authorized certificate identity/capabilities.

**Tests:**

- Boundary/DAG and documentation checks.
- Full workspace build, lint, and test suites.
- Real-host benchmark and block-validation canaries.

## Final Validation

- [ ] `just build --no-sccache`
- [ ] `just lint --no-sccache`
- [ ] `just test --summary --no-sccache`
- [ ] `git diff --check`
- [ ] Complete fleet lifecycle passes over TLS 1.3 mTLS gRPC: register, poll,
  accept, credential, heartbeat, reliable event, progress, artifact grant,
  terminal completion, cleanup, and deregistration.
- [ ] Existing duplicate/conflict/stale/fence/gap/terminal-prefix tests pass
  without semantic weakening.
- [ ] Failure injection covers daemon restart, same-process reconnect,
  worker-process restart, poll disconnect, response loss, cancellation, lease
  expiry, and cleanup retry.
- [ ] A real benchmark completes through the loopback or current benchmark
  worker and converges reporting.
- [ ] A real block validation completes on the current Hetzner worker inside
  libvirt and reports its typed verdict/provenance.

## Rollout

1. Deploy the gRPC daemon.
2. Install the matching worker release on the first host.
3. Verify mTLS registration and run benchmark/block-validation canaries.
4. Install that worker release on the second operator's host and authorize only
   its intended capabilities.

If the first deployment fails, stop the worker and diagnose the gRPC
configuration or implementation before admitting work. There is no deployed
JSON fleet or fleet history to migrate or restore, and v29 adds no persistence
migration.

Until `0075` ships, a protocol-changing release uses the same coordinated
drain. Ordinary daemon/worker software releases may be deployed independently
without changing the protocol integer when the published wire contract and
semantics remain unchanged.

## Deferred / Non-Goals

- Multiple simultaneously supported protocol versions or revisions.
- Feature negotiation, previous-worker fixtures, compatibility windows, or
  server-first rolling protocol upgrades (`0075`).
- Buf breaking checks against a previous protobuf release; v29 establishes the
  first published baseline.
- Daemon-pushed work, streaming assignment channels, or changing pull
  scheduling.
- Multi-orchestrator HA or leader election.
- Supporting another worker implementation language.
- Moving artifact bytes through gRPC.
- New task kinds, submission surfaces, lifecycle controls, resource fairness,
  autoscaling, or reporting behavior.
- Changing the local `sbgh-driver`/`sbgh-libvirt` execution boundary.

## Follow-Ups

- `0075-rolling-worker-protocol-compatibility` adds bounded compatibility before
  the first incompatible protobuf contract change.
- `0065-job-lifecycle-controls`, `0067-github-block-validation-submission`,
  `0078-slack-task-submission`, and `0070-slack-block-validation-controls`
  remain independent application work.
