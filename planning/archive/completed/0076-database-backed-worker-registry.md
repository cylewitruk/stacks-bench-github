# v30: Database-Backed Worker Registry

Successor to
[v29](0074-protobuf-fleet-protocol.md). Make PostgreSQL
the sole durable authority for worker enrollment and policy, expose that
authority through the authenticated admin API and CLI, and reduce fleet
configuration to daemon bootstrap and transport settings.

> **Status:** shipped — implementation and local validation completed on
> 2026-07-30. Real-host enrollment, rotation, restart, and task canaries remain
> rollout gates.
>
> v29 established the first protobuf worker transport. v30 removes the static
> worker allowlist and daemon-restart reconciliation before onboarding
> independently operated workers.
>
> The database-authority design remains current. Its undeployed private-CA,
> certificate-fingerprint, and worker-local configuration details are
> intentionally continued and replaced by
> [v31](0077-worker-identity-and-config-simplification.md) before
> the first fleet rollout.

## Item

- **id:** `0076-database-backed-worker-registry`
- **status:** `shipped`
- **priority:** `high`
- **depends_on:** `0004-worker-fleet`, `0074-protobuf-fleet-protocol`
- **relates_to:** `0075-rolling-worker-protocol-compatibility`
- **source:** fleet-configuration simplification review (2026-07)

## Problem

The daemon currently has two worker registries with different authority:

- `config.example.fleet.toml` defines worker identity, certificate
  fingerprints, allowed capabilities, measurement profile, enabled state, and
  drain state;
- daemon startup copies most of that policy into `worker_registry`;
- certificate fingerprints remain only in the in-memory TOML model; and
- startup disables database workers missing from the file.

The operator API can persist a drain change, but a daemon restart can overwrite
it with the file value. Adding or rotating a worker certificate requires
editing a daemon-local file and restarting the central service. The database
looks authoritative to status and scheduling code while the file silently
reconciles over it.

This is unsuitable once worker operators manage independent hosts. Worker
enrollment and revocation must be durable control-plane operations, while
process bootstrap, private-key paths, and protocol timing remain ordinary
static daemon configuration.

## Outcome

```text
operator
   |
   +-- sbgh-cli -- authenticated admin API ------------------+
                                                            |
                                                            v
                                             worker_registry
                                             worker_certificate
                                                            |
worker -- TLS 1.3 mTLS -- Register/session RPC -------------+
   |              |                                         |
   |              +-- URI SAN worker UUID                    |
   |              +-- active certificate fingerprint         |
   |              +-- enabled server-owned policy            |
   |                                                        v
   +-- advertised capabilities + discovered resources --> worker_session
```

The admin API is the only production mutation boundary. `sbgh-cli` is its
operator client and never writes PostgreSQL directly. The gRPC `Register` RPC
continues to create an ephemeral process session for an already-enrolled
worker; it cannot create an identity, authorize a certificate, or expand
capabilities.

The daemon reads current registry authorization from PostgreSQL on every worker
RPC. Certificate authorization, revocation, enablement, drain, and capability
policy therefore take effect without restarting the daemon.

## Authority and Ownership

| Concern | Authority after v30 |
| --- | --- |
| Worker UUID and display name | PostgreSQL worker registry |
| Allowed capabilities | PostgreSQL worker registry |
| Benchmark measurement profile | PostgreSQL worker registry |
| Enabled and draining state | PostgreSQL worker registry |
| Authorized certificate fingerprints | PostgreSQL worker certificate rows |
| Advertised capabilities | Current worker session |
| CPU, memory, software, and protocol facts | Current worker session |
| Libvirt paths and execution resource profiles | Worker-local configuration |
| Fleet listener, TLS/key paths, lease key, timing, and limits | Daemon bootstrap configuration |
| CA private key and certificate issuance | Offline operator PKI |

Effective capabilities remain the intersection of server authorization and
worker advertisement:

```text
effective_capabilities =
    worker_registry.allowed_capabilities
    ∩ worker_session.advertised_capabilities
```

The server owns measurement profiles because they determine benchmark
comparability. Workers discover and advertise host resources but cannot assign
themselves a profile or authorize a capability.

## Design Rules

- **PostgreSQL is the sole durable policy authority.** Do not keep a startup
  worker list, write-through TOML reconciler, file-watcher, or
  `disable_workers_except` equivalent.
- **Enrollment is administrative; registration is ephemeral.** A CA-signed
  worker certificate authenticates a cryptographic identity but does not grant
  registry membership or capabilities. The worker `Register` RPC remains
  session-only.
- **Separate authentication, authorization, and advertisement.** TLS verifies
  the client chain and sole worker URI SAN; the registry authorizes the active
  leaf fingerprint and policy; the session records what the worker can
  currently provide.
- **Fail closed at the request boundary.** Every worker RPC rechecks enabled
  state and active fingerprint authorization. Do not cache positive
  authorization in process memory. A request already authorized when a
  concurrent revocation commits may finish; subsequent requests must fail.
- **Keep one mutation boundary.** API handlers use narrow registry ports;
  neither handlers nor CLI commands issue ad hoc registry SQL.
- **Preserve history.** Workers are disabled, not hard-deleted. Certificate
  revocation records remain auditable. Existing sessions, attempts, jobs, and
  submissions retain their foreign-key identity.
- **Rotation uses explicit overlap.** Multiple active certificates may
  temporarily authorize one worker. A fingerprint may belong to only one
  worker for its lifetime.
- **Protect active execution.** Measurement-profile changes, capability-policy
  changes, identity disablement, and final-certificate revocation require a
  drained worker with no active attempt or pending cleanup. Emergency
  containment must be an explicit operation with visible fencing/requeue
  consequences, not an implicit update.
- **Keep the CA offline.** The daemon and API never receive or generate a CA
  private key or worker private key. Certificate enrollment accepts only the
  public leaf certificate.
- **Keep bootstrap configuration static.** Database availability, listener and
  key paths, lease signing, protocol timing, and hard resource limits are
  needed to start or bound the process and do not become mutable registry
  rows.
- **Do not redesign scheduling or the protocol.** Pull scheduling, offers,
  attempts, leases, fencing, reliable events, artifact grants, cleanup, and
  protobuf revision 1 remain unchanged.

## Registry Model

Retain `worker_registry` as the server-owned identity and policy aggregate:

```text
worker_registry
  worker_id
  identity_uri
  display_name
  allowed_capabilities
  measurement_profile
  enabled
  draining
  created_at
  updated_at
```

Add normalized certificate authorization:

```text
worker_certificate
  certificate_sha256   -- exactly 32 bytes; globally unique
  worker_id            -- FK to worker_registry
  created_at
  revoked_at
```

New `worker_session` rows also persist the authenticated leaf fingerprint used
to create the session. That binding supports audit, rotation, and the
precondition that normal revocation cannot strand the certificate currently
authorizing live work. Pre-v30 historical sessions may retain a null
fingerprint; every v30 registration writes one.

The API accepts a bounded public leaf certificate, verifies its format, client
usage, trusted worker-CA signature, and sole
`urn:sbgh:worker:<worker-uuid>` URI SAN, derives the SHA-256 fingerprint, and
stores only the digest. A certificate with the wrong identity, wrong issuer,
wrong EKU, conflicting fingerprint ownership, or invalid validity window is
rejected before persistence. Private keys are never accepted.

An active authorization requires `revoked_at IS NULL`. Revocation is a state
transition, not row deletion. Re-authorizing a revoked fingerprint or assigning
it to another identity fails closed; rotation issues a new certificate.

Worker creation may precede certificate issuance. Such a row is inert: no
session can register until it is enabled and has at least one active
certificate. This supports the natural sequence of creating a UUID, issuing a
certificate for its URI SAN, and then authorizing that certificate.

## Admin API and CLI

Use the existing admin-authenticated HTTP API for registry mutations and keep
read-only fleet overview available under its existing read/admin policy.
Canonical operations are:

| Operation | Purpose |
| --- | --- |
| `POST /api/fleet/workers` | Create an inert or fully specified worker policy; generate a UUID when omitted |
| `GET /api/fleet/workers/{id}` | Inspect policy, active session, and certificate metadata |
| `PATCH /api/fleet/workers/{id}` | Change display name, allowed capabilities, measurement profile, or enabled state under lifecycle guards |
| `POST /api/fleet/workers/{id}/certificates` | Validate and authorize one public leaf certificate |
| `DELETE /api/fleet/workers/{id}/certificates/{fingerprint}` | Revoke one certificate while retaining audit history |
| Existing drain endpoint | Set or clear durable drain state |

DTOs use `deny_unknown_fields`, bounded strings and certificate size, typed
capabilities, canonical lowercase fingerprint output, and typed conflict/
precondition errors. Registry mutation logs include worker identity and
operation but never certificate contents, tokens, or private material.

`sbgh-cli` exposes the same operations without a direct database path:

```text
sbgh fleet add-worker
sbgh fleet show-worker
sbgh fleet update-worker
sbgh fleet enable-worker
sbgh fleet disable-worker
sbgh fleet authorize-certificate
sbgh fleet revoke-certificate
sbgh fleet drain
sbgh fleet undrain
```

The common onboarding flow is:

1. `worker add` creates or accepts a UUID and server-owned policy.
2. The operator issues a leaf certificate for that UUID with
   `scripts/fleet-pki.sh`.
3. `certificate authorize --certificate <path>` uploads the bounded public
   leaf; the daemon validates it and returns the canonical fingerprint.
4. The operator installs the leaf/key and CA on the worker and starts it.
5. `fleet status` confirms the expected session, effective capabilities,
   profile, software, and discovered resources.

CLI commands may combine create and certificate authorization when the
operator already selected a UUID and issued its certificate. A failure after
worker creation leaves only an inert auditable row and never a partially
authorized identity.

## Policy Mutation Semantics

- Display-name and drain changes may apply while a session is online.
- Capability additions/removals and measurement-profile changes require the
  worker to be drained with no active offer/attempt or cleanup obligation.
  They apply to the next session; the operator restarts the worker after the
  policy change.
- Normal disablement requires the same quiescent state and closes any idle
  live session. It never silently cancels active work.
- Revoking one of multiple certificates is allowed while the worker uses
  another active certificate. Revoking the certificate used by a live session
  requires a drained, quiescent worker.
- Emergency disable/revocation is a separate explicit admin action. It fences
  the session, requests cancellation/requeue according to the existing fleet
  state machine, and reports any cleanup that cannot safely complete.
- Enabling requires a non-empty allowed-capability set, at least one active
  certificate, and a non-empty measurement profile when benchmark capability
  is authorized.

These rules prevent a policy edit from silently changing the measurement
environment or stranding a live attempt.

## Canonical Daemon Configuration

Remove `SBGH_FLEET_CONFIG` and `config.example.fleet.toml`. Add a daemon-owned
`[fleet]` section to the canonical daemon configuration containing only:

- listener address;
- server certificate and private-key paths;
- worker CA certificate path;
- lease-HMAC key path;
- heartbeat, lease, offer, session, long-poll, request, upload-grant, and
  staging-GC durations;
- per-artifact and per-attempt artifact limits; and
- an explicit bounded `max_concurrent_requests`.

`max_concurrent_requests` replaces the current value derived from the static
worker-list length. Its default preserves the current minimum concurrency and
is validated against a hard upper bound.

Move GitHub block-validation command defaults out of fleet transport policy
and into the daemon's GitHub/task-trigger configuration. Do not put worker
rows, fingerprints, capabilities, profiles, enabled state, or drain state
under `[fleet]`.

The daemon composition layer owns the combined top-level configuration and
projects existing core settings and daemon-only fleet bootstrap settings into
their consumers. Do not make `sbgh-core` depend on Tonic, TLS listener, or
daemon adapter types merely to parse one file.

## Phases

### Phase 1: Freeze the Authority Contract and Add Credential Persistence

**Goal:** Make the intended identity/policy ownership explicit and persist
certificate authorization safely.

**Scope:**

- Add the forward-only `worker_certificate` migration, constraints, indexes,
  session-fingerprint binding, and representative upgrade tests.
- Define narrow worker-registry query/mutation and authorization ports rather
  than growing the execution-oriented fleet store indiscriminately.
- Move existing drain SQL behind the registry mutation port.
- Define typed enrollment, certificate, policy, conflict, and precondition
  outcomes.

**Acceptance & Validation:**

- [ ] A fingerprint is exactly 32 bytes, globally unique, belongs to one
  worker, and cannot be resurrected or reassigned after revocation.
- [ ] Worker disablement preserves every historical session, attempt, job, and
  submission reference.
- [ ] Every new session is bound to the active certificate fingerprint
  authenticated at its TLS connection; pre-v30 history remains readable.
- [ ] Concurrent create/authorize/revoke operations elect one valid result and
  never leave conflicting active authorization.
- [ ] Migration tests cover fresh schema, existing registry/session history,
  invalid fingerprint length, conflicting ownership, revocation history, and
  transactional rollback.
- [ ] Registry API/application code depends on narrow ports rather than direct
  SQL or the full fleet execution store.

### Phase 2: Admin API and CLI Enrollment

**Goal:** Allow operators to manage worker policy without editing daemon files
or accessing PostgreSQL directly.

**Scope:**

- Implement authenticated create/show/update/enable/disable and certificate
  authorize/revoke API operations.
- Validate bounded public leaf certificates against the configured worker CA,
  EKU, validity, and sole URI SAN before deriving their fingerprint.
- Add CLI commands, structured output suitable for setup scripts, and
  actionable typed errors.
- Extend fleet overview/detail without exposing credentials or unnecessary
  certificate contents.

**Acceptance & Validation:**

- [ ] Read-scoped callers cannot mutate enrollment; ingest and unauthenticated
  callers are rejected.
- [ ] Worker private keys and the CA private key are neither accepted nor
  readable by the API.
- [ ] Wrong-CA, wrong-SAN, wrong-EKU, expired/not-yet-valid, oversized,
  malformed, duplicate, and conflicting certificates fail closed.
- [ ] A worker may be created before certificate issuance but cannot register
  while inert.
- [ ] CLI onboarding, rotation overlap, revocation, enable/disable, drain, and
  inspection exercise the API rather than the database.
- [ ] API and CLI tests cover unknown fields, bounds, idempotent retries,
  concurrent mutations, and safe diagnostics.

### Phase 3: Database-Backed Runtime Authorization

**Goal:** Make current database policy authoritative for every worker request.

**Scope:**

- Replace in-memory `ConfiguredWorker` lookup with an asynchronous registry
  authorization query using worker UUID and peer leaf fingerprint.
- Keep TLS chain and URI-SAN authentication in the listener; intersect
  server-authorized and advertised capabilities during session registration.
- Recheck enabled/fingerprint state before every subsequent RPC.
- Enforce quiescent policy-mutation rules and explicit emergency fencing.
- Preserve current offer, lease, attempt, event, artifact, completion, and
  cleanup behavior.

**Acceptance & Validation:**

- [ ] A CA-valid but unenrolled certificate cannot register.
- [ ] Worker/request UUID mismatch, inactive fingerprint, disabled identity,
  or disjoint capabilities fail before session creation.
- [ ] Adding a worker or overlapping rotation certificate works without a
  daemon restart.
- [ ] Revocation or disablement rejects the next RPC on an already-open HTTP/2
  connection; no positive in-memory authorization cache masks it.
- [ ] Capability/profile policy cannot change underneath active work.
- [ ] Registration and scheduling continue to use the effective capability
  intersection and server-owned measurement profile.
- [ ] Existing retry, fencing, cancellation, cleanup, and all-RPC gRPC suites
  remain behavior-compatible.

### Phase 4: Remove Static Worker Reconciliation and Consolidate Config

**Goal:** Leave one canonical daemon configuration with no worker policy in
files.

**Scope:**

- Merge fleet bootstrap/transport settings into `[fleet]` in
  `config.example.daemon.toml`.
- Move GitHub block-validation defaults to their trigger owner.
- Add explicit bounded transport concurrency independent of worker count.
- Delete `ConfiguredWorker`, startup registry upsert/disable reconciliation,
  the non-empty-worker startup requirement, `SBGH_FLEET_CONFIG`, and
  `config.example.fleet.toml`.
- Update systemd/environment examples and parse/validation tests.

**Acceptance & Validation:**

- [ ] The daemon starts with an empty worker registry and serves the admin API
  and mTLS listener so the first worker can be enrolled dynamically.
- [ ] Restarting the daemon cannot alter any registry policy row.
- [ ] No production source reads worker identities, fingerprints,
  capabilities, profile, enabled state, or drain state from TOML.
- [ ] The checked-in daemon example parses and contains every fleet bootstrap
  setting exactly once.
- [ ] Repository searches and a focused boundary check reject reintroduced
  static worker lists, `disable_workers_except`, or `SBGH_FLEET_CONFIG`.
- [ ] Fleet request capacity is deterministic, bounded, and independent of the
  number of registered workers.

### Phase 5: Operations, Security, and End-to-End Validation

**Goal:** Prove enrollment, rotation, revocation, restart, and execution on the
real deployment path.

**Scope:**

- Rewrite setup and worker-fleet operations around API/CLI enrollment.
- Document normal rotation, quiescent disablement, emergency containment,
  daemon restart, database backup, and rollback.
- Update architecture and daemon API reference material.
- Exercise benchmark and block-validation workers through enrollment,
  execution, rotation, and revocation.

**Acceptance & Validation:**

- [ ] Setup contains no instruction to edit a worker allowlist or restart the
  daemon for enrollment/rotation.
- [ ] Normal rotation proves old+new overlap, cutover, old-certificate
  revocation, and uninterrupted authorization of the replacement.
- [ ] Revocation is demonstrated on an existing HTTP/2 connection, not only a
  fresh TLS handshake.
- [ ] Daemon restart preserves drain, enablement, capabilities, profile, and
  certificate state byte-for-byte.
- [ ] One dynamically enrolled benchmark worker and one dynamically enrolled
  block-validation worker register, claim compatible work, complete, report,
  and clean up.

## Final Validation

- [ ] `just build --no-sccache`
- [ ] `just lint --no-sccache`
- [ ] `just test --summary --no-sccache`
- [ ] `git diff --check`
- [ ] Fresh and upgrade migration suites pass.
- [ ] Admin API authentication, DTO, bounds, idempotency, concurrency, and
  lifecycle-precondition suites pass.
- [ ] PostgreSQL registry/certificate authorization and rollback suites pass.
- [ ] Full protobuf all-RPC, mTLS identity, session, scheduler, fencing,
  cancellation, artifact, cleanup, and reporting suites pass.
- [ ] Config parsing, docs/registry links, package DAG, unused dependency, and
  static-worker-policy boundary checks pass.
- [ ] Real-host benchmark and block-validation enrollment/execution canaries
  pass, including daemon restart and certificate rotation/revocation.

## Rollout and Rollback

This is an additive database migration but an intentional configuration
cutover:

1. Keep workers stopped. Back up PostgreSQL and retain the v29 fleet config for
   rollback only.
2. Install the canonical daemon config with `[fleet]`, deploy the v30
   daemon/API/CLI, and apply the certificate table migration.
3. Confirm the daemon starts with an empty registry and both its admin API and
   mTLS listener are healthy.
4. Inspect worker policy rows retained from v29, create only missing
   identities through `sbgh-cli`, authorize current public certificates, and
   compare the resulting policy with the intended v29 values.
5. Start one worker at a time. Require expected session identity, effective
   capabilities, measurement profile, and discovered resources before
   undraining it.
6. Run one benchmark and one block-validation canary, restart the daemon, and
   confirm policy/session recovery without registry mutation.
7. Exercise one overlap rotation and revoke the old certificate before
   onboarding another operator.

The new certificate table is additive, so rollback stops workers and the
daemon, redeploys v29 with the retained fleet config, and restarts workers
under its file-backed policy. Do not run v29 and v30 daemons concurrently.
Database restore is required only if an unexpected non-additive migration
change is introduced during implementation.

## Deferred / Non-Goals

- Worker self-enrollment, trust-on-first-use, invitation/enrollment tokens, or
  possession of a CA-signed certificate as implicit authorization.
- Daemon-managed CA/private-key storage, certificate issuance, ACME, SPIFFE,
  or automated certificate renewal.
- Dynamic database control of bootstrap secrets, listener addresses, protocol
  timing, hard resource limits, worker-local libvirt policy, or execution
  profiles.
- Multiple orchestrators, leader election, registry replication outside
  PostgreSQL, push scheduling, or cloud worker provisioning.
- Protobuf rolling-version negotiation and independent upgrade compatibility
  (`0075`).
- New task kinds, submission producers, lifecycle commands, scheduling
  policy, or reporting behavior.
