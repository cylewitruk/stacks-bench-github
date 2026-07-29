# Worker fleet operations

This runbook covers the current worker fleet after
[setup](setup.md). One active `sbgh-daemon` owns scheduling, leases, durable
events, artifacts, and provider reporting. Workers poll outbound and execute
only registry-authorized capabilities.

## Routine status

Run operator commands as the daemon user:

```bash
alias sbgh='sudo -u sbgh sbgh-cli'

sbgh status
sbgh jobs list
sbgh fleet status
sbgh webhook tail --limit 20
```

`fleet status` is the primary view for registered identities, current
sessions, discovered CPU/memory, active attempts, lease state, traces, and
pending cleanup.

Service logs:

```bash
journalctl -u sbgh-daemon -f
journalctl -u 'sbgh-worker@benchmark' -f
journalctl -u 'sbgh-worker@block-validation' -f
docker compose -f docker/docker-compose.yml logs -f handler smee postgres
```

The Prometheus endpoint is `GET /api/fleet/metrics` with `read` or `admin`
authentication. Alert on:

- heartbeat age above 30 seconds or the configured lease TTL;
- an active attempt with no lease time remaining;
- scheduling wait above the task SLO while a compatible worker should exist;
- reliable event ACK gaps lasting more than two heartbeats;
- resend-buffer pressure above 75%;
- staging objects older than 30 minutes or unexpected staging growth;
- cleanup pending longer than one lease TTL.

Do not hide the loss of a sole-capability worker by automatically moving
comparison work to a different measurement environment.

## Drain, cancellation, and recovery

Drain prevents new offers while allowing the active attempt to finish:

```bash
sbgh fleet drain --worker-id <worker-uuid>
sbgh fleet undrain --worker-id <worker-uuid>
```

Cancel active work by job ID:

```bash
sbgh fleet cancel --job-id <job-uuid>
```

Cancellation is durable. The worker observes it on heartbeat, stops the VM,
runs normal teardown, and may submit only a cancelled terminal. A success
terminal that already won the database race remains immutable.

Recover a submission only after inspecting its current attempt and cleanup
state:

```bash
sbgh fleet recover-submission \
  --submission-id <submission-uuid> \
  --reason "operator-approved recovery"
```

Use `--worker-id <uuid>` only to impose an intentional compatible placement.
Recovery creates a new execution generation and restarts at the first
specification/run. Older results remain auditable but are excluded from the
new comparison.

## Worker admission and preflight

Before enabling a worker profile, run:

```bash
sudo -u sbgh-worker sbgh-worker \
  --config /etc/sbgh/worker/benchmark.toml \
  --preflight-only
```

Use the block-validation profile on that host. Preflight opens no fleet
session and checks:

- worker config and profile resources against discovered CPU and memory;
- certificate/key and server CA files;
- golden image and fixed host commands;
- job, cache, result, and runtime paths;
- exact `sandbox-egress` network name and structural verifier;
- newest matching read-only chainstate origin where the capability needs one;
- the shared fixed thin-pool Data% and Meta% health floors.

Build-only capability does not require a chainstate origin. The fixed pool
floors reject an already near-full or mis-provisioned pool; they are not
per-assignment write prediction.

## Chainstate refresh

Every benchmark and block-validation worker independently maintains a recent
read-only LVM origin under the configured prefix:

```bash
sudo ./scripts/download-chainstate.sh \
  --vg vg0 --thinpool thinpool --prefix mainnet-
sudo lvs -o vg_name,lv_name,lv_attr,origin,data_percent,metadata_percent
```

Run this nightly or on demand. The new LV is published only after checksum
verification and extraction complete, then set read-only and deactivated.
The worker selects the lexicographically newest matching name when preparing
an attempt.

The downloader removes older origins only when they have no active snapshots.
Use `--keep-old` when retaining history or diagnosing a change. Never make a
published origin writable. If preflight reports a writable newest origin,
drain the worker and correct it explicitly:

```bash
sudo lvchange --permission r vg0/mainnet-YYYY-MM-DD
```

The selected origin and guest-observed coverage are recorded with block
validation results. A worker whose local chainstate cannot cover the requested
range fails the attempt as infrastructure; it must not fabricate a partial
verdict.

## Sandbox network

All guest phases use the managed `sandbox-egress` libvirt network. The active
unit must pass its post-start verifier before any worker can start:

```bash
systemctl is-active --quiet sbgh-sandbox-egress.service
journalctl -u sbgh-sandbox-egress.service -n 50 --no-pager
sudo /usr/local/libexec/sbgh-check-sandbox-network
```

The policy permits public dependency egress while denying the host,
private/link-local/metadata destinations, configured public infrastructure
CIDRs, and IPv6 fallback. Each VM also requests libvirt port isolation.
Because public egress remains available, this is containment rather than DLP.

After a routing, firewall, nftables, libvirt, protected-CIDR, or golden-image
change:

1. Drain and stop the worker.
2. Apply or refresh the checked-in policy.
3. Start `sbgh-sandbox-egress.service` and require it to remain active.
4. Require the structural success message in its journal.
5. Run the fixed verifier.
6. Run the disposable-guest ceremony.
7. Start and undrain the worker only after all checks pass.

```bash
sudo ./scripts/install-sandbox-network.sh --refresh
sudo systemctl restart sbgh-sandbox-egress.service
sudo /usr/local/libexec/sbgh-check-sandbox-network
sudo ./scripts/qualify-sandbox-network.sh --execute \
  /var/lib/sbgh-worker/sandbox-egress-qualification.md \
  /var/lib/libvirt/images/sbgh-golden-ubuntu24.qcow2
```

Use `--refresh` only after comparing local policy with the new checked-in
assets; it preserves `protected-ipv4.conf`. Add repeatable
`--deny-tcp IP:PORT` probes for protected public endpoints that accept a safe
host connection. The ceremony first proves host reachability, then requires
guest denial, then proves host reachability again so an offline service cannot
produce a vacuous pass.

The structural verifier intentionally fails closed if the local nftables
version renders a different rule shape. Treat a distro nftables upgrade as a
maintenance event and rerun the full ceremony.

## LVM isolation qualification

Run the two-snapshot smoke when commissioning or changing a worker's LVM/XFS
layout. Drain the worker and mount the selected origin read-only at the path
passed to the script:

```bash
sudo ./scripts/qualify-block-validation-lvm.sh \
  /var/lib/sbgh-worker/lvm-isolation.md \
  vg0 mainnet-YYYY-MM-DD /mnt/sbgh-chainstate-origin

sudo ./scripts/qualify-block-validation-lvm.sh --execute \
  /var/lib/sbgh-worker/lvm-isolation.md \
  vg0 mainnet-YYYY-MM-DD /mnt/sbgh-chainstate-origin
```

The first command is a dry run. The execution creates two writable snapshots,
mounts them with production XFS safety options, proves origin and peer write
isolation, and fails if cleanup leaves an LV or mount.

Then run one end-to-end canary at the intended block-validation shard count.
Start conservatively and tune shard/concurrency policy from real duration and
host telemetry. CPU, memory, and device count are admission limits; synthetic
storage-throughput prediction is not a release gate.

## Certificate lifecycle

Worker certificates are valid for 90 days by default. Each certificate carries
one identity URI SAN:

```text
urn:sbgh:worker:<worker-uuid>
```

### Rotate a worker certificate

1. Issue a replacement for the same UUID with
   [fleet-pki.sh](../scripts/fleet-pki.sh).
2. Add both old and new leaf SHA-256 fingerprints to the worker registry.
3. Restart the daemon so it loads the overlap.
4. Drain the worker.
5. Atomically replace its certificate and private key.
6. Restart the worker and confirm registration under the same UUID.
7. Remove the old fingerprint and restart the daemon.
8. Undrain the worker.

### Revoke

- Remove one fingerprint and restart the daemon to reject that certificate.
- Set `enabled = false` in registry policy and restart the daemon to revoke the
  worker identity.
- Use network and CA revocation as additional containment for a compromised
  key.

The daemon rechecks fingerprint and registry authorization on every worker
request.

### Rotate the lease key

The lease HMAC key must contain at least 32 random bytes and be readable only
by the daemon. Rotate it only after all workers are drained and no attempt or
cleanup obligation remains; replacement invalidates every outstanding lease.

Keep the encrypted private CA key offline and backed up. Never copy it to the
daemon or worker service directories.

## Deploy an application update

The daemon and workers require exact worker-protocol compatibility. Use a full
drain:

1. Drain all workers.
2. Wait for active attempts and cleanup obligations to reach zero.
3. Stop worker services.
4. Take a database backup and retain the current binaries/configuration.
5. Build and validate the new checkout with `just build`, `just lint`, and
   `just test`.
6. Install binaries with `sudo ./scripts/install-daemon.sh --no-start`.
7. Start the daemon; it applies forward-only migrations.
8. Start workers from the same release.
9. Verify mTLS identity, protocol, capabilities, preflight, and fleet state.
10. Run benchmark and block-validation canaries, then undrain.

Never run an older binary against a schema it was not designed to read.
Rollback across a schema-changing deployment means stopping traffic and
restoring the matching database backup before starting the retained binary.
When the newer schema is backward compatible, a binary-only rollback may be
safe, but treat that as an explicitly verified property rather than an
assumption.

## Backup and restore

The checked-in timer runs `pg_dump` inside the Postgres container, compresses
the archive, verifies its shape, publishes it atomically, and prunes according
to retention:

```bash
systemctl status sbgh-pg-backup.timer
journalctl -u sbgh-pg-backup.service
sudo systemctl start sbgh-pg-backup.service
```

Copy backups off-host. Periodically restore one into an isolated PostgreSQL
instance and run the current application migrations and consistency checks
there. Do not point rehearsal binaries at production object storage or provider
credentials.

Before a migration-bearing deployment, rehearse against a recently restored
production backup. Compare submission/job/result counts and provider identity
rows before opening the maintenance window. A migration that reports ambiguous
historical ownership must be investigated; do not bypass its fail-closed guard.

## Failure drills

Exercise these cases on the deployed topology:

- lost poll, accept, event, artifact, and terminal responses;
- worker kill during execution and during cleanup;
- network partition shorter and longer than a lease;
- daemon restart during execution and report projection;
- cancellation racing successful completion;
- checksum, wrong-key, expired-grant, and staging-GC paths;
- valid and deliberately invalid block ranges;
- binary-cache miss followed by a hit;
- chainstate snapshot allocation failure and partial-allocation cleanup;
- sandbox positive-egress and protected-destination denial;
- worker drain and certificate rotation/revocation.

The invariants are:

- at most one current attempt per scheduling unit;
- exactly one accepted terminal per attempt;
- no mutation by a stale fence;
- no logical artifact visibility before terminal acceptance;
- no negative validation verdict unless every shard exited normally;
- no repository-produced process on the worker host;
- unresolved teardown remains visible as cleanup work and blocks unsafe requeue.
