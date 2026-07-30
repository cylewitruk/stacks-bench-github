# Architecture

`stacks-bench-github` accepts benchmark and block-validation requests, executes
repository revisions in isolated worker VMs, and projects durable results to
GitHub and Slack. One active `sbgh-daemon` is the trusted orchestrator. Workers
poll outbound and can execute only capabilities authorized by the daemon's
registry.

## System topology

```text
GitHub webhooks ──> sbgh-handler ──┐
Slack Socket Mode ─────────────────┤
operator sbgh-cli ─────────────────┤
                                   v
                              sbgh-daemon
                    policy, submission, scheduling,
                    leases, events, artifacts, reporting
                       |       |              |
                       v       v              v
                   PostgreSQL GitHub/Slack    S3
                       |
                       | protobuf/gRPC over HTTP/2;
                       | TLS 1.3 mutual X.509;
                       | workers poll outbound
               ┌───────┴────────┐
               v                v
          sbgh-worker       sbgh-worker
          benchmark         block validation
               └──── sbgh-libvirt ────┘
                      disposable VM
                   writable LVM snapshot
```

The daemon is the sole PostgreSQL client and the sole owner of GitHub App,
Slack, and object-store credentials. The handler verifies webhook HMACs and
forwards accepted deliveries to the daemon API. The CLI is an authenticated API
client. Workers have no database or provider credentials.

Only one orchestrator may be active. Multi-orchestrator high availability and
leader election are not implemented.

## Workspace boundaries

| Crate | Responsibility |
| --- | --- |
| `sbgh-api` | Operator/ingest HTTP DTOs and typed client |
| `sbgh-core` | Domain models, policy, configuration, and persistence ports |
| `sbgh-fleet` | Transport-neutral fleet values, validation, and semantic digests |
| `sbgh-driver` | Backend-neutral execution contracts |
| `sbgh-github` | GitHub contracts, webhook DTOs, authentication, and API adapter |
| `sbgh-handler` | Webhook HMAC verification and forwarding |
| `sbgh-intent` | Validated request-intent contract and OpenAI adapter |
| `sbgh-libvirt` | Libvirt VM and LVM snapshot adapter |
| `sbgh-postgres` | SQLx persistence adapters and migrations |
| `sbgh-proto` | Generated `sbgh.fleet.v1` protobuf/gRPC boundary and conversions |
| `sbgh-slack` | Slack intake, snapshot rendering, and transport |
| `sbgh-smee` | smee.io forwarding for the deployed webhook edge |
| `sbgh-worker` | Worker transport, recipes, execution, and cleanup |
| `sbgh-daemon` | Orchestration, API, scheduling, event projection, and reporting |
| `sbgh-cli` | Local operator client |

The package DAG check enforces dependency direction across all features and
build dependencies. Provider and persistence crates implement narrow ports;
domain code does not import SQLx, Octocrab, Slack, or libvirt.

## Submission, scheduling, and execution

Submission is separate from scheduling and attempt coordination.

1. A GitHub, Slack, or operator adapter authorizes a request and resolves
   mutable repository references.
2. The submission kernel validates a typed plan, freezes executable inputs,
   calculates a demand digest, and atomically persists the aggregate,
   provenance, jobs, and namespaced idempotency receipt.
3. The pull scheduler matches queued jobs to registry-authorized worker
   capability and optional operator placement constraints.
4. The fleet coordinator owns offers, attempts, leases, fencing,
   cancellation, and cleanup.
5. The worker resolves only worker-local infrastructure, executes the typed
   request, and returns events, artifacts, and a typed terminal result.

A reused idempotency key with the same executable demand returns the original
receipt. Reusing it for different demand fails closed. Assignment never
re-resolves daemon defaults, symbolic refs, or user input.

`sbgh_core::SubmissionSpec` is durable requested state within a
`TaskSubmission`. `sbgh_driver::TaskSpec` is the fully resolved instruction for
one execution backend. They intentionally represent different boundaries.

### Fleet state

Jobs move through `queued -> offered -> running -> terminal`. Every active
mutation is bound to:

- worker identity;
- worker process session;
- attempt UUID;
- monotonically increasing fencing generation;
- HMAC-authenticated lease token.

Poll, accept, event, artifact, and terminal operations are idempotent. A worker
process restart creates a new session and cannot resume its predecessor. The
daemon fences the old attempt, requires cleanup acknowledgement, and only then
requeues eligible work. A same-process reconnect resends unacknowledged
reliable events from a bounded memory buffer; there is no durable worker
outbox.

Reliable events are persisted before acknowledgement and projected only over
their contiguous sequence prefix. Best-effort fine progress has a separate
wire sequence. Durable leases and events let daemon restart/replay converge
without rerunning accepted work or accepting a stale terminal.

The worker control plane is the generated
`sbgh.fleet.v1.WorkerFleetService` gRPC service. Polling remains a bounded
unary long-poll, and automatic gRPC retries are disabled; the worker's explicit
reconnect/resend loops and application idempotency own retry behavior. Artifact
bytes remain outside gRPC.

### Task kinds

- **Benchmark** submissions may contain variants, repetitions, calibration,
  and carried results. The entire comparison generation stays on one worker
  and measurement profile.
- **Build-only** jobs populate the same fingerprinted executable cache without
  producing an external report.
- **Block validation** runs one resource-profiled VM with bounded shard and
  concurrency policy and produces a typed positive or negative verdict.

Moving a partial benchmark comparison to a different worker is never
automatic. Explicit recovery creates a new execution generation and restarts
at the first specification so results never mix measurement environments.

New task kinds add a protocol payload, capability, worker recipe,
task-specific result persistence, and a report-detail variant. They reuse the
submission, scheduler, lease, event, artifact, cancellation, cleanup, and
report-projection state machines.

## Execution isolation

Every repository revision is adversarial, regardless of source authorization.
Build scripts and produced binaries execute only in disposable libvirt VMs.
There is no direct host-process fallback.

All chainstate workers maintain local read-only LVM origins under a shared
naming prefix. For each attempt, the trusted adapter:

1. chooses the lexicographically newest matching read-only origin;
2. checks the thin pool's fixed near-full Data% and Meta% floors;
3. creates explicitly writable attempt snapshots;
4. attaches only snapshots, never the origin;
5. tears down the VM, mounts, loops, and LVs before acknowledging cleanup.

Benchmark execution uses one snapshot. Block validation uses K private
snapshots, stable virtio-scsi serials, and XFS `nouuid`. K is bounded by the
worker's CPU, memory, device, shard, and concurrency policy; it is not used for
speculative disk-write reservation.

The guest builds or reuses `stacks-bench` or `stacks-inspect` through a
worker-local cache keyed by executable kind, repository, commit, toolchain,
recipe, target, and golden image. Persistent compiler state is not mounted into
guests.

Guest result files are size-bounded, opened without following the final
symlink, and required to resolve beneath the attempt results share. Block
validation verifies guest-observed coverage and rejects partial, stale,
malformed, identity-mismatched, or ambiguous negative output. A negative
validation verdict is accepted only when every shard exits normally with a
defined result code.

## Network and identity

The worker listener is separate from the operator API and requires TLS 1.3.
It accepts protobuf/gRPC over HTTP/2 only; there is no JSON worker API. The
daemon uses a Web-PKI server certificate. Each worker proves possession of a
P-256 identity key through an in-memory, self-signed TLS wrapper; the wrapper's
issuer, names, and certificate digest have no authorization meaning.

The daemon derives the canonical SPKI SHA-256 digest from the authenticated
handshake. PostgreSQL maps that digest to a stable worker UUID and authorizes
its active identity key, capabilities,
measurement profile, enabled state, and drain state on every RPC. Registration
records current worker advertisement, whose capabilities can only narrow
server policy. Enrollment and rotation use the admin API and do not require a
daemon restart.

Every guest phase uses the checked-in
[`sandbox-egress`](../network/sandbox-egress.xml) network. Its nftables policy:

- permits public dependency fetches;
- denies the host, private, carrier-grade NAT, loopback, link-local, metadata,
  multicast, and reserved IPv4 ranges;
- denies configured public infrastructure CIDRs;
- denies guest IPv6 forwarding;
- isolates libvirt ports from other guests.

Worker preflight accepts no alternate network name and invokes a fixed,
root-owned structural verifier. A disposable-guest qualification proves both
positive dependency egress and negative protected-destination reachability.
The policy is containment, not data-loss prevention; confidential-source
deployments require an allowlisted dependency proxy or a stricter no-egress
network.

## Credentials and artifacts

| Credential | Owner and lifetime |
| --- | --- |
| GitHub App private key | daemon-only mode-`0600` file |
| GitHub App JWT | daemon memory, at most 10 minutes |
| GitHub installation token | daemon memory, about one hour |
| Repository-read token | one active worker assignment, memory only |
| Slack tokens | daemon environment only |
| S3 access key | daemon environment only |
| Webhook HMAC secret | handler environment only |
| Handler ingest token | handler and daemon environment |
| Worker private key | one worker identity, mode-`0600` |
| Lease HMAC key | daemon-only mode-`0600` file |

Private-repository assignments may carry a short-lived token restricted to
read-only contents for one repository. The worker uses it only on the trusted
host while preparing source; it is not mounted into the VM or written to
artifacts.

Workers upload through short-TTL presigned requests for
orchestrator-derived exact staging keys with signed size and checksum metadata.
A stale attempt cannot choose a key or publish a result. Only an accepted
terminal promotes verified staging objects to logical result keys.

## Reporting

Workers emit events and typed outcomes; they never call GitHub or Slack. The
daemon projects one provider-neutral, submission-scoped report snapshot from
durable state:

- aggregate lifecycle and progress;
- exhaustive `Benchmark`, `BuildOnly`, or `BlockValidation` detail;
- bounded, escaped untrusted diagnostics;
- a monotonic snapshot version.

The authenticated report API and both provider renderers consume this same
snapshot. GitHub checks use stable task names: `stacks-bench` and
`stacks-block-validation`. Build-only remains externally silent. Child jobs and
attempts are lineage, not report identity.

GitHub check/comment and Slack message identities are reconciled
deterministically. Renderers rebuild complete snapshots instead of patching
previous provider text, so replay after a daemon restart is idempotent.
Replayed older events cannot regress a newer terminal snapshot.

An invalid block is a completed negative/red correctness result. Setup,
timeout, process, transport, or incomplete-shard failures remain
infrastructure failures and may be retried according to fleet policy.

## Operator surfaces

- [setup.md](setup.md) installs the current system.
- [worker-fleet-operations.md](worker-fleet-operations.md) covers routine
  worker, certificate, maintenance, and recovery procedures.
- [daemon-api.md](daemon-api.md) documents the operator and ingest API.
- [slack-setup.md](slack-setup.md) configures the optional Slack surface.
