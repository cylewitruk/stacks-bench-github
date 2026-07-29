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

Every execution profile uses the checked-in `sandbox-egress` XML and nftables
policy for all guest phases. The policy permits dependency fetches but denies
the host, private/link-local and metadata destinations, IPv6 fallback, and
operator-listed public infrastructure CIDRs. Worker preflight rejects alternate
network names and invokes the root-owned structural verifier. Each domain
requests libvirt port isolation. Because dependency egress is still egress,
this boundary is not DLP; deployments handling confidential source should put
an allowlisted dependency proxy behind the same contract.

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
   benchmark and block-validation examples. CPU and RAM are discovered at
   process startup; do not copy host-capacity totals into these profiles.
5. Install the network policy with `sudo
   ./scripts/install-sandbox-network.sh --install-only`, add
   environment-specific protected public CIDRs, then apply it with `sudo
   ./scripts/install-sandbox-network.sh`.
6. Add the exact root-owned
   `/usr/local/libexec/sbgh-check-sandbox-network` command to the worker sudo
   allowlist.
7. Enable `sbgh-worker@benchmark.service` locally and
   `sbgh-worker@block-validation.service` on the dedicated host.
8. Install
   [sbgh-worker-block-validation-hardening.conf](../systemd/sbgh-worker-block-validation-hardening.conf)
   as the block worker's systemd drop-in after adjusting its paths.
9. Start the daemon only after its complete preflight passes, then enable the
   intended worker profile. Use `systemctl enable --now sbgh-daemon.service`
   followed by `systemctl enable --now sbgh-worker@<profile>.service`.

Every execution worker receives only the narrow sudo/libvirt permissions
documented in [host-bringup.md](host-bringup.md). Repository builds and produced
executables run in disposable VMs; sudo is used only by the trusted libvirt/LVM
adapter for fixed infrastructure commands.

v26 does not mount a persistent sccache directory into guests. After draining
all pre-v26 workers, an operator may remove the obsolete
`/var/lib/sbgh-worker/sccache`; compiler-cache state is guest-local and the
fingerprinted binary cache remains the cross-attempt reuse mechanism.

Use a dedicated, guest-safe `stacks-inspect` chain configuration for block
validation. The worker copies it into the VM, so it must not be a production
follower configuration containing RPC passwords, node keys, seed material, or
other reusable credentials.

## Host and chainstate validation

Every benchmark and block-validation chainstate origin must be read-only. The
worker never attaches an origin to a guest; it creates an explicitly writable
attempt snapshot instead. Benchmark and block-validation preflight resolve the
newest LV matching the worker-local `chainstate_base_prefix`, reject a writable
origin, and apply the same fixed thin-pool health floor. Build-only preflight
does not require or allocate chainstate. Every worker is expected to run the
same nightly/on-demand updater and have a sufficiently recent local origin.
Block-validation guests probe the selected snapshot and fail as infrastructure
when the requested range is absent; successful results record the exact origin
and observed range. Manifests and LVM tags are optional provenance only.

At startup, the worker discovers its available logical CPU count and Linux
`MemTotal`, validates every advertised execution profile against that capacity,
and registers those measured facts for fleet observability. Guest vCPU, memory,
CPU placement, shard, and concurrency values remain operator policy in the
profile. Storage is validated by the component that owns it (thin-pool health,
binary-cache limit, and required filesystem paths); the worker does not publish
an ambiguous aggregate storage total.

Before enabling an execution capability, run its worker preflight and record:

```bash
sudo -u sbgh-worker sbgh-worker \
  --config /etc/sbgh/worker/benchmark.toml \
  --preflight-only
sudo -u sbgh-worker sbgh-worker \
  --config /etc/sbgh/worker/block-validation.toml \
  --preflight-only
sudo lvs -o vg_name,lv_name,lv_attr,lv_tags,lv_size,data_percent,metadata_percent
sudo ./scripts/characterize-worker-host.sh \
  /var/lib/sbgh-worker/host-characterization.md /var/lib/sbgh-worker
```

`--preflight-only` verifies the golden image, fixed command binaries, writable
runtime directories, fixed `sandbox-egress` name, root-owned structural
policy, read-only origin, and the shared fixed Data%/Meta% health floor.
The floor rejects an already near-full or mis-provisioned pool; it does not
predict assignment writes and never scales with K. Registration and
block-offer admission run the same checks; the standalone command opens no
fleet session. Block validation rolls each block's processing writes back; its
MB-scale WAL/SHM divergence is not reserved as if every shard were a
write-heavy workload.

Structural verification proves the intended policy is loaded, but not packet
behavior. Before enabling either execution profile—and after any firewall,
routing, libvirt, or protected-CIDR change—run:

```bash
sudo ./scripts/qualify-sandbox-network.sh --execute \
  /var/lib/sbgh-worker/v26-sandbox-egress.md \
  /var/lib/libvirt/images/sbgh-golden-ubuntu24.qcow2
```

The disposable VM must fetch `https://index.crates.io/config.json` (or an
operator-supplied HTTPS dependency canary) while controlled host, RFC1918, and
metadata-like endpoints remain unreachable. Retain the report with the host
qualification evidence.
Use repeatable `--deny-tcp IP:PORT` arguments for safe listening endpoints on
operator-protected public control-plane addresses. The ceremony rejects
addresses outside `protected-ipv4.conf` and first proves each endpoint is
reachable from the host. It repeats that host control after the guest probe, so
an offline or disappearing service cannot produce a vacuous pass.
After any firewall-service reload, restart
`sbgh-sandbox-egress.service`, rerun this ceremony, and only then restart
workers.

Run the checked-in two-snapshot isolation smoke once for each host/storage
setup. It defaults to a dry run:

```bash
sudo ./scripts/qualify-block-validation-lvm.sh \
  /var/lib/sbgh-worker/v26-lvm-isolation.md \
  vg0 mainnet-full-2026-07-26 \
  /mnt/sbgh-chainstate-origin

# After reviewing the resolved values and draining the worker:
sudo ./scripts/qualify-block-validation-lvm.sh --execute \
  /var/lib/sbgh-worker/v26-lvm-isolation.md \
  vg0 mainnet-full-2026-07-26 \
  /mnt/sbgh-chainstate-origin
```

The smoke refuses to overwrite prior evidence, requires the selected origin to
be read-only and mounted from the named LV, creates two explicitly writable
snapshots, mounts them with the production XFS safety options, proves
bidirectional peer/origin write isolation, and fails on a cleanup residue.

Then run one end-to-end canary at the intended K and compare its logical result
and artifacts with the established manual validation. The canary itself
exercises all K devices and exposes an unsuitable K through runtime, timeout,
attachment, or cleanup behavior. Start conservatively and tune K from real job
duration and host telemetry; synthetic throughput measurement is optional
capacity planning, not a release gate.

The future
[0052 managed-node producer](../planning/design/0052-managed-stacks-node-chainstate-producer.md)
may replace the downloader. Until then, each worker independently refreshes
under the same naming prefix, publishes a new read-only LV, and retains old
origins while active snapshots reference them. Distributed generation
registration/promotion/bootstrap is deliberately deferred.

## Normal operation

```bash
sbgh-cli fleet status
sbgh-cli fleet drain --worker-id <worker-uuid>
sbgh-cli fleet undrain --worker-id <worker-uuid>
sbgh-cli fleet cancel --job-id <job-uuid>
sbgh-cli fleet recover-submission --submission-id <submission-uuid> \
  --worker-id <replacement-worker-uuid> \
  --reason "operator-approved recovery"
```

Cancellation is durable: the active attempt sees it on heartbeat, stops the
VM through the common teardown lifecycle, and may submit only a cancelled
terminal. A completed terminal that won the database race remains immutable.

Cross-worker movement of a partial benchmark submission is never automatic.
`recover-submission` creates a new execution generation and reruns from the first
spec/run; older results remain auditable and are excluded from the new
comparison. `--worker-id` optionally pins the new generation to an enabled,
non-draining worker authorized for benchmark work; omit it to let normal
compatible-worker placement choose.

New task demand enters through the daemon's submission kernel before any worker
is selected. A caller-stable producer key makes retries return the original
submission receipt; reusing that key for different executable demand fails
closed. Optional worker/profile values are immutable operator constraints.
Scheduler-owned assignments remain empty until a compatible worker polls, so
submitting while every worker is offline is expected and safe.

The authenticated `/api/fleet` view shows registry/session/resource,
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

The fleet requires exact protocol-version equality. v26 uses wire version 3,
which keeps block offers limited to shard/concurrency requirements while
chainstate selection stays local. Upgrade under a full drain; older workers
cannot register with a v3 daemon:

1. Drain all workers and wait for active attempts and cleanup obligations to
   reach zero.
2. Stop workers.
3. Upgrade/restart the daemon and apply its forward-only migration.
4. Upgrade workers to the identical release.
5. Start workers, verify mTLS registration/version/capabilities, then
   undrain.

The v27.2 submission migration additionally requires a restored-production
rehearsal. Queued demand may remain, but claimed/running jobs, active attempts,
and pending cleanup obligations must be zero. Stop daemon writers, take and
restore a production backup into isolated PostgreSQL, apply the migration
there, and compare submission/job counts plus GitHub/Slack provenance and
idempotency rows before touching production. Duplicate historical Slack
reporting identities must resolve to the earliest submission. A reported
GitHub-plus-Slack identity conflict names corrupt aggregates that must be
investigated; never bypass the guard or discard one producer identity.

Treat nftables, libvirt firewall, and host firewall upgrades as network-policy
maintenance, even when no sbgh release changes. The supported nftables version
is the distro-maintained package on Debian 12 or Ubuntu 24.04 whose rendered
rules pass the exact live verifier. Because that verifier intentionally fails
closed on output drift, perform these steps under drain:

1. Stop workers and record `nft --version`.
2. Apply the package or firewall change.
3. Run `systemd-analyze verify
   /etc/systemd/system/sbgh-sandbox-egress.service` for syntax.
4. Restart `sbgh-sandbox-egress.service` and require `systemctl is-active
   --quiet sbgh-sandbox-egress.service` to succeed.
5. Inspect `journalctl -u sbgh-sandbox-egress.service -n 50 --no-pager` and
   require the `sandbox-egress structural policy check passed` message.
6. Run `/usr/local/libexec/sbgh-check-sandbox-network`, then repeat the active
   disposable-guest ceremony, including configured operator TCP probes.
7. Restart and undrain workers only after every check passes.

`systemd-analyze verify` cannot prove runtime sandbox behavior; the actual unit
start and `ExecStartPost` result are the load-bearing gate.

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
- block VM cache miss followed by a cache hit, with no host-side
  `stacks-inspect` process;
- exact K virtio-scsi devices and serials, XFS `nouuid` mounts, origin
  immutability, shared thin-pool health rejection, partial allocation rollback,
  and attempt-scoped restart cleanup;
- sandbox network structural verification plus the disposable-guest positive
  dependency and negative host/private/metadata/operator-endpoint probes;
- graceful drain and certificate rotation/revocation.

Expected invariants: one current attempt per scheduling unit, one accepted
terminal per attempt, no stale mutation after fencing, no artifact visibility
before terminal acceptance, no negative validation result unless every shard
exited normally in `{0,1}`, no repository-produced host process, and honest
pending cleanup when a worker never returns.

## Rollback

Keep the prior release binaries, configuration, and database backup until the
two-worker soak completes. Because v25 migrations are forward-only, rollback
means stopping v25 traffic and restoring the pre-cutover database backup before
starting the old single-host release; never point an old binary at the migrated
schema. Preserve fleet staging and result objects until the incident is
resolved.
