# v31: Worker Identity and Configuration Simplification

Successor to
[v30](../archive/completed/0076-database-backed-worker-registry.md).
Make the first deployed worker identity contract key-based rather than
private-CA based, derive local capabilities from configured recipes, and make
the operator-facing worker configuration belong to `sbgh-worker` rather than
the libvirt adapter.

> **Status:** planned
>
> v29 and v30 established protobuf transport and database-owned worker policy,
> but the fleet has not yet been deployed. v31 consolidates their undeployed
> certificate/SAN bootstrap into the simpler public-key model before that first
> rollout. It does not preserve an unused certificate wire or storage contract.
> The operator confirmed on 2026-07-30 that v30 has not been deployed or pushed
> to a shared environment; only recreatable local development state may contain
> its migration.

## Item

- **id:** `0077-worker-identity-and-config-simplification`
- **status:** `planned`
- **priority:** `high`
- **depends_on:** `0074-protobuf-fleet-protocol`,
  `0076-database-backed-worker-registry`
- **relates_to:** `0075-rolling-worker-protocol-compatibility`
- **decision:**
  [0005-public-key-worker-identities](../decisions/0005-public-key-worker-identities.md)
- **source:** worker configuration and identity simplification review (2026-07)

## Why

The current worker file repeats information that already has a stronger source
of truth:

- `worker_id` duplicates the UUID in the client certificate URI SAN and the
  daemon rejects the request unless those copies agree;
- `capabilities` duplicates the presence of executable benchmark and
  block-validation profiles;
- client certificate, private-key, and daemon-CA paths expose private-PKI
  ceremony to every worker operator; and
- deserializing `sbgh_libvirt::LibvirtConfig` directly nests task, workspace,
  and storage policy under `[libvirt]` merely because libvirt currently
  executes it.

The result has multiple ways to express inconsistent local state and lets an
adapter's internal aggregate dictate the product's public configuration.

## Outcome

```text
central daemon
  public Web-PKI TLS certificate for fleet.example.com
                         |
                         | TLS 1.3 + gRPC
                         v
worker identity private key
  └── ephemeral self-signed TLS wrapper
         └── authenticated SPKI SHA-256
                └── database lookup
                       └── stable worker UUID + allowed policy
```

The worker operator generates one identity key, submits its public half for
administrative enrollment, and configures only the private-key path. The daemon
uses its ordinary public TLS certificate; workers validate it with native
platform roots.

The worker file declares executable recipes. Their presence determines local
advertisement:

```text
[benchmark] present        -> benchmark + build_only
[block_validation] present -> block_validation

effective capabilities =
    locally inferred capabilities
    ∩ worker_registry.allowed_capabilities
```

Server-owned authorization, measurement profile, enablement, drain state,
leases, scheduling, and attempt fencing remain unchanged.

## Design Rules

- **Authenticate keys; authorize registry records.** TLS proves possession of
  the worker key. PostgreSQL maps its SPKI digest to a stable worker UUID and
  current policy. A key alone never self-enrolls or expands capability.
- **Do not derive the logical worker from a rotatable key.** UUIDs retain
  placement, measurement, session, attempt, and audit continuity while keys
  overlap and rotate.
- **Do not trust identity claims in protobuf.** Remove `worker_id` from
  registration. Peer identity comes only from the authenticated connection;
  session and attempt identifiers bind later RPCs.
- **Keep public and private trust directions distinct.** Workers validate the
  daemon through public Web PKI and hostname verification. The daemon validates
  worker key possession, then authorizes the SPKI digest through PostgreSQL.
- **Possession means TLS `CertificateVerify`, not certificate parsing.** The
  custom client verifier may relax the issuer trust anchor for the self-signed
  wrapper, but it must cryptographically verify the TLS 1.3 handshake signature
  against the presented SPKI. No verifier method may manufacture an
  unconditional signature-valid assertion.
- **No insecure fallback.** Do not add private-CA overrides, disabled
  verification, TOFU, request-supplied identities, or custom protobuf
  challenge/signature authentication.
- **Infer support; retain authorization.** Local recipe sections determine what
  the process can execute. Database `allowed_capabilities` remains the
  server-owned upper bound.
- **Make public config worker-owned.** `sbgh-worker` parses and validates the
  operator schema, then explicitly projects it into adapter configuration.
  `sbgh-libvirt` must not own or deserialize the whole worker file.
- **Keep task policy out of adapter namespaces.** Benchmark and
  block-validation profiles, workspace paths, LVM policy, and cache policy are
  top-level worker concerns even when their current implementation uses
  libvirt.
- **Remove fake configurability.** Values fixed by security or implementation
  invariants become constants or safe defaults rather than fields that accept
  only one useful value.
- **Preserve execution behavior.** VM provisioning, chainstate snapshots,
  build cache, artifact flow, task payloads, pull scheduling, cancellation,
  cleanup, and reporting do not change.
- **Set the first deployed baseline once.** Because no fleet protocol or v30
  certificate rows have been deployed, remove the obsolete schema and fields
  directly. Do not add certificate compatibility adapters, aliases, or
  backfills.

## Target Worker Configuration

The checked-in examples converge on this ownership shape:

```toml
orchestrator_url = "https://fleet.example.com"
identity_private_key = "/var/lib/sbgh-worker/identity.key"

[workspace]
jobs_dir = "/var/lib/sbgh-worker/jobs"
git_mirror = "/var/lib/sbgh-worker/git/stacks-core.git"
results_tmpfs_root = "/run/sbgh-worker/jobs"
results_archive_dir = "/var/lib/sbgh-worker/results"

[sandbox]
service_user = "sbgh-worker"
golden_image = "/var/lib/libvirt/images/sbgh-golden-ubuntu24.qcow2"
boot_disk_gib = 64
host_cpus = "6-7"

[commands]
sudo = "/usr/bin/sudo"
virsh = "/usr/bin/virsh"
qemu_img = "/usr/bin/qemu-img"
cloud_localds = "/usr/bin/cloud-localds"
git = "/usr/bin/git"

[lvm]
vg_name = "vg0"
thinpool = "thinpool"
chainstate_base_prefix = "mainnet-"
min_data_free_percent = 5.0
min_metadata_free_percent = 5.0

[benchmark]
build_vcpus = 4
bench_vcpus = 2
build_memory_bytes = 17179869184
bench_memory_bytes = 8589934592
job_timeout_secs = 21600

[binary_cache]
enabled = true
max_bytes = 53687091200
dir = "/var/lib/sbgh-worker/binary-cache"
```

A block-validation worker adds `[block_validation]`; a combined worker contains
both recipe sections. At least one recipe section is required.

The policy-managed `sandbox-egress` network, safe block-validation mount
options, internal snapshot prefixes, and VM monitor timing use implementation
constants or defaults unless implementation review identifies a real
operator-controlled requirement. Absolute command overrides remain explicit
because they are coupled to the sudoers allowlist.

ECDSA P-256 with unencrypted PKCS#8 PEM private keys and SPKI PEM public keys is
the initial identity format. Private-key files are protected by the service
account and mode `0600`. Deployments that require an external secret store may
point `identity_private_key` at a systemd credential path. Passphrases in TOML
and an interactive service-start prompt are out of scope.

## Identity and Registry Model

The first deployed registry schema uses key authorization directly:

```text
worker_identity_key
  public_key_sha256   -- SHA-256(canonical SPKI DER), exactly 32 bytes
  worker_id           -- FK to worker_registry
  created_at
  revoked_at

worker_session
  identity_key_sha256 -- authenticated key used for this session
```

Enrollment accepts one bounded PEM `PUBLIC KEY`, validates the supported
algorithm and canonical SPKI encoding, derives its digest, and stores only the
digest. Private keys and TLS wrapper certificates are rejected. A digest is
globally unique for its lifetime; revocation is immutable.

The worker creates a bounded, short-lived, self-signed client certificate in
memory from the identity key. The daemon's TLS verifier accepts exactly one
well-formed client leaf with the supported algorithm, validity, and
client-auth usage. Its TLS 1.3 `CertificateVerify` path cryptographically
checks the handshake transcript signature against that leaf's SPKI; accepting
the leaf's shape alone is insufficient. The verifier does not grant
authorization at handshake time. The request service hashes the authenticated
SPKI and performs the existing fresh database authorization check before every
RPC.

An unknown key may complete the bounded TLS handshake but cannot register,
poll, fetch credentials, accept work, emit events, terminalize, promote
artifacts, or clean up. Existing listener concurrency, timeouts, message
bounds, and rate controls limit unauthenticated resource use.

Normal key rotation authorizes a second public key, restarts the drained worker
with its replacement private key, verifies the new session, and revokes the old
digest. Emergency revocation retains the current explicit
fence/cancel/requeue behavior.

## Protocol and Pre-Deployment Schema

Remove `worker_id` from `RegisterRequest` and its transport-neutral equivalent.
The daemon passes the worker UUID resolved from the peer SPKI into session
creation. No RPC may fall back to a UUID supplied by the client.

Revision 1 remains the first deployed protobuf baseline. This change occurs
before the first fleet rollout, so it does not trigger `0075` compatibility
machinery or preserve the undeployed field number as an active compatibility
path.

Likewise, implementation should amend the not-yet-deployed v30 registry
migration and upgrade tests so a fresh database creates only
`worker_identity_key` and `identity_key_sha256`. It must not create then rename
or backfill `worker_certificate`. Before editing the migration, verify that no
production migration ledger contains v30; if that assumption is false, stop
and design a forward migration. Local development databases that applied the
unreleased migration may be recreated.

## Admin API and CLI

Replace certificate terminology rather than keeping aliases:

```text
POST   /api/fleet/workers/{id}/identities
DELETE /api/fleet/workers/{id}/identities/{fingerprint}
POST   /api/fleet/workers/{id}/identities/{fingerprint}/emergency-revoke

sbgh fleet authorize-identity
sbgh fleet revoke-identity
sbgh fleet emergency-revoke-identity
```

The request contains one public key. Responses expose the canonical lowercase
`sha256:<hex>` identity fingerprint and lifecycle timestamps, never key
contents or private material.

Add worker-local identity commands with safe file creation:

```text
sbgh-worker identity generate --private-key <path>
sbgh-worker identity public --private-key <path>
```

Generation refuses overwrite, writes the private key with mode `0600`, and
prints or writes only the public key when requested. Diagnostics, tracing, and
`Debug` implementations never emit private key bytes.

## Phases

### Phase 1: Public-Key Identity Contract and Registry

**Goal:** Establish one canonical identity type and persistence model before
changing transport.

**Scope:**

- Add dependency-light SPKI digest and public identity value types at the fleet
  boundary.
- Replace certificate registry/session fields and ports with identity-key
  equivalents.
- Consolidate the undeployed v30 migration and its representative upgrade
  tests.
- Rename API DTOs, mutation outcomes, metrics, logs, and test fixtures without
  legacy aliases.

**Acceptance & Validation:**

- [ ] The same public key always yields the same digest regardless of PEM
  whitespace or regenerated TLS wrapper; a different key yields a different
  digest.
- [ ] Malformed, private, multiple, oversized, noncanonical, and unsupported
  keys fail before persistence.
- [ ] Concurrent enrollment elects one worker; revoked keys cannot be
  resurrected or reassigned.
- [ ] Registry/session history and normal/emergency revocation invariants
  remain covered.
- [ ] Fresh schema and representative upgrade tests contain no certificate
  table, URI identity, or certificate fingerprint.

### Phase 2: Web-PKI Daemon and Key-Authenticated Workers

**Goal:** Replace private-CA client identity with proof of enrolled worker-key
possession while retaining TLS 1.3 and gRPC.

**Scope:**

- Load the daemon's public Web-PKI full chain and private key without a client
  CA.
- Validate daemon hostname and chain from worker native roots.
- Generate the worker's self-signed client wrapper in memory.
- Extract authenticated SPKI identity at the daemon listener and resolve the
  worker through PostgreSQL on every RPC.
- Remove client-supplied worker identity from protobuf and application
  registration.
- Keep the custom client verifier minimal: reject TLS 1.2, delegate TLS 1.3
  signature verification to audited cryptographic verification, and never
  return success solely because a leaf is syntactically valid.

**Acceptance & Validation:**

- [ ] Wrong hostname, untrusted/expired daemon chain, plaintext, TLS downgrade,
  missing client proof, malformed wrapper, and unsupported key fail closed.
- [ ] A full-handshake adversarial test presents an enrolled victim SPKI but
  attempts `CertificateVerify` with a different private key; the handshake
  fails before any gRPC handler or database authorization call. Corrupted
  transcript signatures fail equivalently.
- [ ] A valid but unenrolled key cannot register or invoke another RPC.
- [ ] An enrolled key registers as its database-mapped UUID without sending
  that UUID; no CN, SAN, or request field can override the mapping.
- [ ] Revocation rejects the next RPC on an existing HTTP/2 connection.
- [ ] Two active keys can overlap for one worker; revoking one does not affect a
  session authenticated by the other.
- [ ] All generated gRPC RPCs retain deadlines, retry idempotency, size bounds,
  fencing, and structured error behavior.

### Phase 3: Worker-Owned Configuration and Capability Inference

**Goal:** Make the worker file express product concepts once and project them
into the libvirt adapter.

**Scope:**

- Introduce worker-owned transport, workspace, sandbox, command, LVM, cache,
  benchmark, and block-validation config types.
- Make recipe profiles optional and infer the advertised capability set.
- Add an explicit, tested projection into `sbgh_libvirt::LibvirtConfig`.
- Remove `worker_id`, `capabilities`, client certificate, server CA, and nested
  `[libvirt.*]` from worker configuration.
- Convert one-value security fields and internal naming/timing knobs into
  constants or defaults.

**Acceptance & Validation:**

- [ ] Benchmark-only, validation-only, and combined examples parse, validate
  host resources, preflight every configured recipe, and advertise exactly the
  inferred capabilities.
- [ ] A worker with no recipe fails startup; database policy can narrow but
  never expand inferred support.
- [ ] Legacy identity, capability, and `[libvirt.*]` fields fail as unknown.
- [ ] Task-specific validation remains beside its task profile; the adapter
  receives only a validated projection.
- [ ] Existing VM XML, command construction, resource allocation, chainstate,
  cache, artifact, cancellation, and cleanup golden tests remain unchanged
  except for config construction.

### Phase 4: Operator Workflow, Documentation, and Ratchets

**Goal:** Make key generation, enrollment, daemon certificate renewal, and
worker setup concise and difficult to misuse.

**Scope:**

- Implement identity generation/public-key commands and renamed admin API/CLI
  operations.
- Rewrite setup, daemon API, architecture, worker operations, examples, and
  service-unit guidance.
- Document external ACME/Let's Encrypt issuance, full-chain paths, renewal
  hooks, and daemon restart/reconnect behavior.
- Remove the private-PKI helper and stale certificate/SAN configuration,
  terminology, dependencies, and tests.
- Extend boundary checks to reject reintroduced worker IDs, explicit local
  capabilities, private worker CA, client-certificate paths, and direct
  libvirt-owned worker deserialization.

**Acceptance & Validation:**

- [ ] A new operator can generate one key, provide only its public half to the
  administrator, install the two-line worker identity/endpoint config, and
  register after authorization.
- [ ] Documentation has no worker certificate issuance, CA distribution,
  URI-SAN, or insecure-verification path.
- [ ] Daemon certificate renewal requires no worker config change and workers
  reconnect safely after restart.
- [ ] Private key files are create-new, mode `0600`, non-serializable, and
  absent from logs, API payloads, snapshots, and test output.
- [ ] Config examples and planning/docs registries are parse/link checked.

## Final Validation

- [ ] `just build --no-sccache`
- [ ] `just lint --no-sccache`
- [ ] `just test --summary --no-sccache`
- [ ] `git diff --check`
- [ ] Fresh and representative migration suites pass with the final identity
  schema only.
- [ ] Protobuf generation, Buf lint, all-RPC conversion, package-DAG, and wire
  boundary checks pass.
- [ ] Public-key parser/digest, TLS handshake, hostname/trust, enrollment,
  concurrent ownership, overlap rotation, revocation, and open-connection
  authorization suites pass.
- [ ] All three worker configuration shapes, capability intersection, host
  discovery, libvirt projection, preflight, and stale-field rejection pass.
- [ ] Existing scheduler, lease/fence, event resend, cancellation, artifact,
  cleanup, benchmark, block-validation, and reporting suites remain green.
- [ ] On the real deployment, a publicly trusted daemon endpoint accepts one
  enrolled benchmark worker and one enrolled block-validation worker; both
  complete canaries and reconnect after daemon certificate renewal/restart.

## Rollout

v31 remains part of the first fleet deployment rather than an upgrade:

1. Verify v30's unreleased migration is absent from the production migration
   ledger.
2. Provision a real daemon DNS name and public TLS certificate; configure
   external renewal before exposing the listener.
3. Deploy the v31 daemon/API/CLI and apply the consolidated first-deployment
   registry schema.
4. Generate one key on each worker, enroll only its public key, and retain each
   private key solely on that host.
5. Start workers one at a time and verify mapped UUID, inferred/effective
   capabilities, measurement profile, resources, and preflight.
6. Run benchmark and block-validation canaries, then exercise overlap key
   rotation, revocation on an existing connection, daemon restart, and
   automatic worker reconnect.

Rollback before the first successful rollout stops workers and the daemon and
redeploys the matching pre-v31 binaries/configuration. There is no production
certificate identity state to preserve. After first deployment, the v31
public-key schema and protobuf become the compatibility baseline for `0075`.

## Deferred / Non-Goals

- Application-managed ACME, DNS updates, public certificate issuance, or
  renewal scheduling.
- Private daemon CA fallback, daemon-key pinning, TOFU, insecure TLS, plaintext,
  or disabling hostname/certificate validation.
- Worker self-enrollment, invitation tokens, possession of any key as implicit
  authorization, or automatic capability expansion.
- Encrypted private-key/passphrase UX, interactive service prompts, HSM/TPM
  integration, or remote key custody.
- Deriving stable worker UUIDs from rotatable keys.
- New task kinds, scheduling policy, push execution, lifecycle controls,
  reporting changes, or artifact transport changes.
- Rolling multi-revision worker upgrades (`0075`).
