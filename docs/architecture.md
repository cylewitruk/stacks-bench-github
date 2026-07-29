# Architecture

## Overview

`stacks-bench-github` is a GitHub App and worker fleet for benchmark,
build-only, and block-validation tasks. One active `sbgh-daemon` orchestrator
owns durable truth and external side effects. Separately deployed
`sbgh-worker` processes pull only work authorized for their registered
capabilities.

The Cargo workspace has fourteen crates and five binaries:

```text
crates/
  sbgh-api/          operator/ingest API DTOs and typed client
  sbgh-proto/        versioned, task-neutral worker protocol
  sbgh-core/         domain policy, persistence ports, configuration, models
  sbgh-driver/       backend-neutral local execution contracts
  sbgh-github/       GitHub contracts, webhook DTOs, auth, Octocrab adapter
  sbgh-intent/       request-intent contract and OpenAI adapter
  sbgh-slack/        Slack intake, snapshot renderer, transport
  sbgh-libvirt/      concrete libvirt sandbox and LVM snapshot adapter
  sbgh-postgres/     SQLx stores, migrations, row mappings, admin queries
  sbgh-worker/       worker binary, transport, recipes, local execution
  sbgh-handler/      webhook signature-verification/forwarding binary
  sbgh-cli/          operator API-client binary
  sbgh-daemon/       orchestrator/API/reporting binary
  sbgh-smee/         local-development forwarding binary
```

PostgreSQL is the sole durable state store, and the daemon is its sole client.
The handler and CLI use the authenticated daemon API. Workers have no database,
Slack, GitHub App private-key, or object-store credentials.

## Fleet flow

```text
GitHub / Slack / CLI
          │
          ▼
sbgh-daemon
  webhook policy + immutable enqueue
  PostgreSQL scheduling/leases/events/results
  GitHub/Slack projection and comparison
          │
          │ TLS 1.3 mTLS; workers poll outbound
          ├───────────────────────────────┐
          ▼                               ▼
sbgh-worker                         sbgh-worker
benchmark + build-only              block-validation
sbgh-libvirt sandbox                sbgh-libvirt sandbox
one LVM snapshot                    K attempt-scoped LVM snapshots
          │                               │
          └── exact-key presigned S3 ─────┘
```

Enqueue resolves symbolic refs and effective arguments once and persists a
typed canonical payload plus SHA-256. Assignment never consults mutable daemon
defaults. Registry policy—not worker claims—authorizes capabilities and the
benchmark measurement profile.

The scheduling state machine is `queued → offered → running → terminal`.
Every mutation is bound to worker identity, process session, attempt UUID,
monotonic fencing generation, and an HMAC-authenticated lease token. Lost
poll/accept/event/terminal responses are idempotent. A new worker process gets a
new session and cannot resume its predecessor; cleanup is acknowledged before
safe requeue. Wire protocol v3 includes a bounded, payload-derived requirement
summary in each offer, allowing local resource/LVM admission before lease
acceptance without exposing backend paths to the orchestrator.

Task-neutral reliable events are persisted before acknowledgement and projected
only across their contiguous sequence prefix. Fine progress has an independent
best-effort wire sequence and becomes durable when accepted. The daemon can
restart and replay projection without rerunning work or duplicating accepted
terminals.

## Boundaries

### `sbgh-handler`

The handler reads the raw webhook body, verifies
`X-Hub-Signature-256` with constant-time HMAC comparison, handles `ping`, and
forwards accepted bytes and GitHub headers to the daemon. It does not parse
business events, authorize requests, create jobs, or access PostgreSQL.

### `sbgh-daemon`

The daemon owns:

- webhook classification, authorization, and one task-submission application
  boundary that validates, fingerprints, and atomically persists immutable
  demand;
- worker registry, session, placement, lease, fence, cancellation, and
  recovery state;
- reliable-event/progress ingest and replayable projection;
- exact-key artifact grants, manifest verification, promotion, and staging GC;
- GitHub/Slack reporting, debounce/rate limiting, comparison, and report
  identity reconciliation;
- authenticated operator visibility, drain, and explicit submission recovery.

The daemon has no production dependency on `sbgh-worker`, `sbgh-driver`, or
`sbgh-libvirt`, and contains no inline execution path.

### `sbgh-worker`

The worker owns transport reconnect/resend, active-attempt heartbeat and
cancellation, local recipes, cache/artifact ports, and cleanup. It consumes
`sbgh-proto` DTOs and never sees database rows. A private-repository assignment
may include a short-lived repository-read token. The worker uses it only while
preparing a source disk on the trusted host; it is never mounted into the guest
or written to artifacts.

Every repository revision is treated as adversarial: its build scripts and
produced binaries enter `sbgh-driver` and the concrete `sbgh-libvirt` adapter,
regardless of source authorization. Benchmark/build jobs use one explicitly
writable thin snapshot of the newest read-only origin. A block-validation
assignment uses one resource-profiled VM and K explicitly writable,
attempt-scoped snapshots of the newest local read-only origin,
exposed by stable virtio-scsi serials and mounted with XFS `nouuid`. Origin LVs
are never guest-attached. Both snapshot paths use the same fixed near-full
thin-pool health guard; K remains a compute/device policy rather than a
predicted write-space reservation. Results record the selected origin, and
block validation also records guest-observed coverage. The guest builds or reuses
`stacks-inspect` through the shared binary cache (scoped by executable,
repository, commit, toolchain, build recipe, target, and image), probes epoch
totals, partitions the inclusive range, runs bounded shards, and atomically
writes a typed result. The host partitions and counts the assignment's global
range; only the guest's CLI arguments use epoch-local coordinates. The host
reducer rejects partial, stale, malformed,
identity-mismatched, or ambiguous negative output. Guest-controlled result
files are opened without following final symlinks and must resolve beneath the
results share before parsing, cache publication, or artifact upload; structured
control/diagnostic reads are size-bounded. There is no direct host-process
fallback. Compiler-cache state lives only on the disposable boot overlay; the
fingerprinted binary cache is the sole cross-attempt build-reuse channel.

All guest phases share the versioned `sandbox-egress` libvirt network.
[Its XML and nftables policy](../network/sandbox-egress.xml) permit
repository/dependency fetches while denying the host, worker control plane,
private/link-local destinations, metadata endpoints, and IPv6 fallback.
Environment-specific public infrastructure CIDRs join the deny set through a
root-owned configuration file. Worker preflight accepts no alternate network
name and invokes a fixed root-owned structural verifier; every domain also
requests libvirt port isolation. A disposable-guest qualification proves
positive dependency egress and negative host/private/metadata reachability.
Optional operator TCP probes establish host reachability before and after
proving configured public control-plane endpoints are guest-inaccessible. This
is containment, not a data-loss-prevention guarantee.

### Integration crates

`sbgh-postgres`, `sbgh-github`, `sbgh-slack`, and `sbgh-intent` own their
respective adapters. Core exposes narrow ports/domain types rather than SQLx,
Octocrab, Slack, or provider-specific values. The package DAG check enforces
these normal/build dependency boundaries across all features.

## Identity and secrets

The worker listener is separate from the operator/webhook API and requires TLS
1.3 mutual X.509 authentication. A client certificate carries exactly one URI
identity SAN, `urn:sbgh:worker:<uuid>`, and client-auth usage. Common Name is
ignored. The daemon registry binds the UUID to allowed capability/profile and
enabled/draining state.

Attempt lease tokens are HMAC-bound to worker, session, attempt, and fence.
Workers upload only through short-TTL presigned requests for orchestrator-
derived staging keys with signed size/checksum metadata. A stale attempt cannot
publish an object, and only an accepted terminal promotes verified staging to
logical result keys.

GitHub credentials remain layered:

| Credential | Scope | Storage/owner |
| ---- | ---- | ---- |
| App private key | whole App | daemon-only mode-0600 file |
| App JWT | App, ≤10 min | daemon memory |
| installation token | one installation, ~1 hour | daemon cache |
| repository-read token | one active assignment | worker memory only |
| webhook secret | inbound HMAC | handler secret environment |

## Task model

`sbgh_core::SubmissionSpec` is the persisted requested variant inside a
`TaskSubmission`; it may still contain unresolved workflow state. In contrast,
`sbgh_driver::TaskSpec` is the fully resolved instruction handed to an
execution backend. The distinct names preserve the persistence/execution
boundary and avoid conflating durable planning with one concrete attempt.

The fleet lifecycle is additive across task kinds:

- `benchmark`: comparison-bearing submission assigned by worker pull to one
  worker and measurement profile for every variant/repeat/calibration/carried
  job;
- `build_only`: cache production through the same lease/event/artifact path;
- `block_validation`: one fleet job claimed by a capable worker, with one VM,
  K private snapshots of its newest local RO chainstate origin, guest-observed
  range verification, and its own typed result table;
- future tasks: add a protocol payload, capability, worker recipe, task-specific
  persistence/rendering, and composition registration without changing the
  scheduler/lease/event/terminal state machine.

Moving a partial benchmark submission between workers is never automatic. Explicit
recovery creates a new execution generation and reruns from its first
specification so comparison results never mix measurement environments.

Submission is deliberately separate from scheduling and coordination. Surface
adapters authorize and resolve mutable refs; the submission kernel records a
versioned plan, aggregate provenance, and namespaced idempotency receipt without
consulting live workers. The pull scheduler later matches each concrete job's
stored capability and optional operator constraints, and only then records its
worker/profile assignment. The fleet coordinator alone owns offers, attempts,
leases, fencing, cancellation, and cleanup.

## Reporting

Workers emit events/outcomes only. The daemon is the sole GitHub/Slack
side-effect owner. Slack uses one snapshot-rendered message per request;
reprojection converges from durable state rather than patching previous text.
GitHub check/comment identities use deterministic reconciliation. Block
validation reports invalid blocks as a completed negative/red correctness
result; timeout, process loss, setup, and transport faults remain retryable
infrastructure failures.

## Operations

Production layout, mTLS issuance/rotation/revocation, host characterization,
immutable local chainstate refresh, metrics/alerts, drain/upgrade, failure injection,
and rollback are defined in
[worker-fleet-operations.md](worker-fleet-operations.md). The daemon API is
documented in [daemon-api.md](daemon-api.md); initial host prerequisites remain
in [host-bringup.md](host-bringup.md), with narrow libvirt/LVM permissions
applying to every execution worker rather than the orchestrator.
