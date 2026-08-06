# Decision 0006: Fleet Lifecycle State Separation

- **status:** draft
- **date:** 2026-08
- **related items:** `0004-worker-fleet`,
  `0075-rolling-worker-protocol-compatibility`,
  `0082-seamless-fleet-lifecycle-maintenance`,
  `0083-multi-orchestrator-high-availability`

## Decision

Daemon process lifecycle, fleet scheduling policy, worker scheduling policy,
worker process lifecycle, and observed session state are separate controls.
One may not implicitly mutate another.

A normal daemon shutdown quiesces only that daemon process: it stops preparing
and offering new assignments, closes its listeners gracefully, and leaves
persisted worker policy and active fleet attempts unchanged. It does not
persistently cordon workers or request attempt cancellation. Workers retry
transient control-plane failures within their confirmed leases; the replacement
daemon reconstructs authoritative state from PostgreSQL. An outage beyond the
confirmed lease remains fail closed through the existing cancellation,
cleanup, expiry, and fencing contracts.

`SIGTERM` and the first `SIGINT` request that process-local quiescence. A second
`SIGINT` forces local process exit without mutating fleet policy or cancelling
remote attempts. Fleet-wide abort remains a separate authenticated operator
mutation. A shutdown supervisor, rather than the fleet coordinator or the
absence of active remote attempts, owns process exit after local components
have quiesced.

Persistent worker policy distinguishes `enabled` from `schedulable`. Cordoning
a worker prevents new offers and lets an active attempt finish, but the worker
process remains registered and connected. Stopping a worker after it becomes
idle is a separate, explicit host-maintenance action. Observed online, idle,
busy, and offline session states are not desired policy.

Session expiry and cleanup recovery are normal resident-worker transitions.
An idle worker whose session expires recovers the same session identity in
process, but only when its authenticated registration facts still match and no
live successor exists. A worker with an expired active lease first cancels and
cleans its local attempt, then recovers that session and reconciles pending
obligations before polling for work. A rejected recovery fails closed rather
than fencing a legitimate successor: the server returns a typed, non-retryable
`session_superseded`, and the resident process exits without minting a new
session identity.
Cleanup obligations are delivered continuously or on every session recovery;
they are never discoverable only at process startup. Requeue remains blocked
until cleanup completion is durably acknowledged. The first implementation
uses periodic worker calls to the deployed `ListCleanup` RPC; it does not add a
push semantic before protocol negotiation exists.

On daemon startup, attempt expiry observes a bounded reconciliation grace that
allows one healthy worker heartbeat opportunity before overdue leases are
expired. The grace does not renew a lease or authorize execution: a confirming
heartbeat does that. After the grace, database time, cleanup obligations, and
fencing remain authoritative, including during the bounded interval before a
worker observes its local lease loss.

Fleet-wide scheduler pause/resume is an explicit, auditable control-plane
policy. It prevents new assignments without rewriting every worker record or
terminating worker sessions. Explicit cancellation, emergency identity
revocation, worker disablement, and process shutdown retain their existing
fail-closed purposes and are never inferred from an ordinary service restart.

## Rationale

The first deployed fleet treated daemon shutdown as a global drain and abort.
This was conservative for commissioning, but it couples a centralized daemon
deployment to every decentralized worker: idle workers exit, active attempts
are cancelled, and operators must manually undrain and restart the fleet.
Those side effects are inappropriate once workers have independent operators,
maintenance windows, or tenants.

Leases and fencing—not process co-liveness—are the correctness boundary for
distributed attempts. Keeping desired policy separate from observed state
makes normal restarts transparent while preserving deterministic behavior for
partitions, stale workers, and explicit emergency actions.

## Consequences

- `SIGTERM` becomes graceful process quiescence rather than fleet abort.
- The shutdown supervisor replaces the fleet coordinator as the owner of local
  process exit; remote fleet idleness is not a daemon-exit prerequisite.
- Global pause, worker cordon, attempt cancellation, and emergency disablement
  require explicit authenticated operator actions with audit metadata.
- A cordoned current-protocol worker remains connected and may be uncordoned
  without a systemd start; shutdown-after-idle remains a separate operation.
- Short single-daemon outages may pause intake, projection, and heartbeats, but
  confirmed attempts continue and reconnect under the same fencing identity.
- Planned outages that may exceed the attempt lease must pause scheduling and
  reach zero active attempts, or use a future explicitly confirmed maintenance
  lease. Leases are not silently weakened for convenience.
- Session-expiry recovery and cleanup-obligation delivery must be exercised
  without relying on systemd restart behavior.
- Mixed-version status must expose whether cordon means revision-1
  `exit_after_idle` or negotiated `resident` behavior; operators may not infer
  one fleet-wide semantic during the compatibility window.
- Protocol negotiation must preserve previous-worker behavior while lifecycle
  semantics roll out. Negotiation is owned by `0075`; live cordon is owned by
  `0082`.
- Multi-orchestrator availability remains separate work (`0083`); this
  decision removes fleet-wide side effects from a restart but does not make a
  single daemon continuously available.
