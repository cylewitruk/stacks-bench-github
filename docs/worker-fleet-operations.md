# Worker fleet operations

v25 runs one active `sbgh-daemon` orchestrator and one or more outbound-polling
`sbgh-worker` processes. The daemon is the only PostgreSQL, GitHub-reporting,
Slack, GitHub App private-key, and object-store credential holder. A worker
receives only an attempt-scoped lease token, exact-key upload grants, and a
short-lived repository-read token.

## Network and identity

Expose the fleet listener only on the private worker network. Allow workers to
initiate TCP connections to it; do not expose PostgreSQL, the operator API,
libvirt, or worker SSH publicly. TLS 1.3 mutual authentication is mandatory,
including for the loopback worker.

Use [fleet-pki.sh](../scripts/fleet-pki.sh) to create a private CA and 90-day
server/client certificates. Keep the encrypted CA key offline. Each client
certificate has client-auth EKU and exactly one identity URI SAN:

```text
urn:sbgh:worker:<worker-uuid>
```

The same UUID must be pre-registered in
[config.example.fleet.toml](../config.example.fleet.toml). Registry policy is
authoritative for the currently accepted leaf-certificate SHA-256
fingerprints, capability, measurement profile, enabled state, and drain;
worker-advertised facts cannot elevate access. `fleet-pki.sh worker` prints the
configuration line for the issued fingerprint.

For certificate rotation, issue a replacement with the same worker UUID, place
both old and new fingerprints in registry policy for the overlap, place the new
certificate/key atomically, drain and restart the worker, then remove the old
fingerprint and restart the daemon after registration succeeds. To revoke one
certificate immediately, remove its fingerprint and restart the daemon. To
revoke the identity, set its registry entry `enabled = false` and restart the
daemon. Fingerprint and registry authorization are checked on every worker API
request; use the network/CA layer as additional containment for a compromised
key.

The lease-HMAC key must contain at least 32 random bytes and be mode `0600`.
Rotate it only after draining all workers and confirming there are no active
attempts; rotation invalidates every outstanding lease token.

## Installation

1. Install the release binaries and units with
   `sudo ./scripts/install-daemon.sh --no-start`. First installation must not
   start the daemon before service identities, configuration, secrets, PKI,
   PostgreSQL, and object storage are ready.
2. Create separate `sbgh` and `sbgh-worker` service users. A worker must not
   have daemon secret files or database connectivity.
3. Install the daemon fleet config at a root-controlled path and set
   `SBGH_FLEET_CONFIG` in `/etc/sbgh/daemon/secrets.env`.
4. Install worker profiles as `/etc/sbgh/worker/<profile>.toml` from the
   benchmark and block-validation examples.
5. Enable `sbgh-worker@benchmark.service` locally and
   `sbgh-worker@block-validation.service` on the dedicated host.
6. Install
   [sbgh-worker-block-validation-hardening.conf](../systemd/sbgh-worker-block-validation-hardening.conf)
   as the block worker's systemd drop-in after adjusting its paths.
7. Start the daemon only after its complete preflight passes, then enable the
   intended worker profile. Use `systemctl enable --now sbgh-daemon.service`
   followed by `systemctl enable --now sbgh-worker@<profile>.service`.

The benchmark worker alone receives the narrow sudo/libvirt permissions
documented in [host-bringup.md](host-bringup.md). The block-validation worker
must not receive sudo.

## Host and dataset qualification

Run the non-destructive characterization before choosing shard counts:

```bash
sudo -u sbgh-worker ./scripts/characterize-worker-host.sh \
  /var/lib/sbgh-worker/host-characterization.md /srv/sbgh/workspaces
```

Archive the report with the deployment record. It captures CPU/NUMA, memory,
NVMe/mount layout, capacity, a bounded `fio` sample when available, and an
actual reflink mutation-isolation proof. Choose `requested_shards` and
`max_concurrency` from this data and a bounded validation run, not the
advertised CPU count alone.

Create a generation with
[prepare-dataset-generation.sh](../scripts/prepare-dataset-generation.sh).
The command rejects symlinks, creates a manifest, removes write permissions,
verifies every copied file against the generated SHA-256 list before
publication, and atomically advances an operator `current` pointer. Worker
startup revalidates the manifest identity and file-list digest and proves the
configured reflink mechanism; the publication-time full-file verification is
the integrity gate for the immutable generation. Worker configuration
must pin the real generation directory—not the pointer—and its manifest
digest. Never mutate a published generation. Refresh into a new generation,
verify it, update registry/worker configuration, drain, restart, and retain old
generations while any attempt, result, or cleanup obligation references them.

At startup a block worker verifies the exact identity/range/digest, rejects
symlinks, performs an actual reflink clone, mutates the clone, and proves the
canonical manifest did not change before advertising the capability.

## Normal operation

```bash
sbgh-cli fleet status
sbgh-cli fleet drain --worker-id <worker-uuid>
sbgh-cli fleet undrain --worker-id <worker-uuid>
sbgh-cli fleet cancel --job-id <job-uuid>
sbgh-cli fleet recover-group --group-id <group-uuid> \
  --worker-id <replacement-worker-uuid> \
  --reason "operator-approved recovery"
```

Cancellation is durable: the active attempt sees it on heartbeat, terminates
its local process group, and may submit only a cancelled terminal. A completed
terminal that won the database race remains immutable.

Cross-worker movement of a partial benchmark group is never automatic.
`recover-group` creates a new execution generation and reruns from the first
spec/run; older results remain auditable and are excluded from the new
comparison. `--worker-id` optionally pins the new generation to an enabled,
non-draining worker authorized for benchmark work; omit it to let normal
compatible-worker placement choose.

The authenticated `/api/fleet` view shows registry/session/resource/dataset,
lease, attempt, trace, and cleanup state. `/api/fleet/metrics` exports
Prometheus text. Alert initially on:

- worker heartbeat age above 30 seconds (warning) or lease TTL (critical);
- negative active-attempt lease remaining time;
- enqueue preparation or compatible-worker scheduling wait above the task SLO;
- reliable ACK lag/gap above zero for more than two heartbeat intervals;
- resend-buffer pressure above 75% of 256 envelopes;
- staging age above 30 minutes or unexpected byte growth;
- any cleanup obligation unresolved for more than one lease TTL.

Tune thresholds from the soak; do not hide a sole-capability worker outage by
silently requeueing.

## Upgrade, maintenance, and restart

v25 requires exact protocol-version equality:

1. Drain all workers and wait for active attempts and cleanup obligations to
   reach zero.
2. Stop workers.
3. Upgrade/restart the daemon and apply its forward-only migration.
4. Upgrade workers to the identical release.
5. Start workers, verify mTLS registration/version/capabilities/dataset, then
   undrain.

A same-process network reconnect resends unacknowledged reliable envelopes from
memory. A worker-process restart creates a new session; the daemon fences the
old attempt and requires local cleanup before requeue. An orchestrator restart
keeps durable leases/events, and the worker resumes from the highest contiguous
ACK. The system intentionally has no multi-orchestrator HA or durable worker
outbox in v25.

## Failure-injection gate

Before production cutover, record the run/job/attempt/trace IDs for:

- lost poll, accept, event, artifact, and terminal responses;
- worker kill during execution and during local cleanup;
- network partition shorter and longer than a lease;
- orchestrator restart during execution and event projection;
- cancellation racing success;
- checksum/wrong-key/expired upload rejection and staging GC;
- an upload with deliberately incorrect bytes but otherwise valid signed
  headers is rejected by the deployed S3-compatible provider. This proves the
  provider recomputes `x-amz-checksum-sha256` rather than merely echoing
  client-supplied metadata.
- known-good and deliberately invalid block ranges;
- graceful drain and certificate rotation/revocation.

Expected invariants: one current attempt per scheduling unit, one accepted
terminal per attempt, no stale mutation after fencing, no artifact visibility
before terminal acceptance, no negative validation result unless every shard
exited normally in `{0,1}`, and honest pending cleanup when a worker never
returns.

## Rollback

Keep the prior release binaries, configuration, and database backup until the
two-worker soak completes. Because v25 migrations are forward-only, rollback
means stopping v25 traffic and restoring the pre-cutover database backup before
starting the old single-host release; never point an old binary at the migrated
schema. Preserve fleet staging and result objects until the incident is
resolved.
