# Worker fleet operations

This runbook covers the current worker fleet after
[setup](setup.md). One active `sbgh-daemon` owns scheduling, leases, durable
events, artifacts, and provider reporting. Workers poll outbound and execute
only registry-authorized capabilities. Control RPCs use generated protobuf over
HTTP/2 with TLS 1.3: workers validate the daemon through Web PKI and prove
possession of their enrolled P-256 keys. Artifact bodies use delegated HTTPS
URLs.

## Routine status

Run operator commands as the daemon user:

```bash
alias sbgh='sudo -u sbgh sbgh-cli'

sbgh status
sbgh submissions list --active
sbgh jobs list
sbgh fleet status
sbgh webhook tail --limit 20
```

Follow a request or a particular execution using the ID from a CLI listing,
GitHub comment, or Slack report:

```bash
sbgh show <job-or-submission-id>
sbgh watch <job-or-submission-id>
sbgh results <job-or-submission-id>
sbgh artifacts ls <job-or-submission-id>
sbgh artifacts get <job-id> appdata/stacks-bench.db --output ./stacks-bench.db
```

Full UUIDs and unique prefixes of at least eight characters are accepted.
Ambiguous IDs require a longer prefix or `job:<uuid>` / `submission:<uuid>`.
A job selector follows that execution; a submission selector includes all its
variants and repeats. Artifact reads from a multi-job submission require a job
selector if several jobs contain the requested name. Downloads stream into a
temporary file and publish the destination only on success, without overwriting
an existing file.

`show`, `watch`, `results`, `submissions list`, `artifacts ls`, and `artifacts get`
accept a local `--json` option. `watch --json` emits JSON Lines; other structured
responses emit one JSON value. `artifacts cat` always emits the original bytes.
The same observation commands are available under `jobs` and `submissions`.

`watch` polls every five seconds (`--interval` changes this), refreshes in place
on a terminal, and emits timestamped changes when redirected. It displays task
progress and worker heartbeat freshness separately. Ctrl-C stops observation
without cancelling work. Completion exits successfully; failure or cancellation
exits unsuccessfully. Guest console streaming is not yet available; use service
logs while running and promoted console artifacts after termination.

Validation progress distinguishes range discovery (`initialize`, no total yet)
from command startup and processed-block samples. A command with no positive
sample is described as initializing **or waiting for progress**: older producers
can remain silent while doing work. Neither elapsed time nor a healthy heartbeat
proves that blocks have been processed.

A processing ETA appears only after ten seconds and at least three counter
advances for the same active commands. It uses up to sixty one-second intervals,
requires the two half-window rates to agree within a factor of two, and disappears
after ten seconds without an advance. Queued commands, initialization and command
changes suppress/reset it. The estimate excludes teardown; it is best-effort
display text, not a deadline or an authoritative result. Short or silent runs
may never show an ETA.

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

If LVM reports a read-only metadata archive or a VM shuts down without any
visible phase record, verify that the installed worker unit matches the
checked-in unit. Filesystem-protection directives must not give the worker a
private mount namespace: libvirt and `virtiofsd` need to see its per-attempt
results tmpfs.

The worker keeps a restrictive process umask. Its libvirt adapter explicitly
makes only each ephemeral VM job directory execute-only to non-owners so QEMU
can traverse to libvirt-owned disks without making job contents listable.

### Targeted worker maintenance

`fleet cordon` prevents new assignments but does not stop an accepted attempt.
The current negotiated `resident` semantic keeps the process online; the
supported previous worker reports `exit_after_idle` and exits on cordon.
`fleet uncordon` only changes scheduling policy: it never starts a service.
Legacy `drain`/`undrain` remain aliases for these scheduling changes; use the
explicit commands with a reason for maintenance audit receipts.

Select the worker UUID and its local service profile explicitly. These commands
require the new daemon/CLI; inspect `cordon_semantic` before choosing the exit
path. They do not update binaries or change any other worker's policy.

```bash
sbgh fleet worker-status --worker-id "$worker_id" --json
cordon=$(sbgh fleet cordon --worker-id "$worker_id" \
  --reason 'operator: planned worker update' --json) || exit
sbgh fleet wait-worker --worker-id "$worker_id" \
  --generation "$(jq -r .generation <<<"$cordon")" \
  --until quiescent --timeout-seconds 3600 --json || exit
```

For a current worker with `shutdown_after_idle` support, explicitly request its
session to exit. This request does **not** cordon it; the earlier cordon remains
in place so a replacement process cannot accept work during maintenance.

```bash
shutdown=$(sbgh fleet shutdown-after-idle --worker-id "$worker_id" \
  --reason 'operator: planned worker process stop' --json) || exit
sbgh fleet wait-worker --worker-id "$worker_id" \
  --generation "$(jq -r .generation <<<"$shutdown")" \
  --session-id "$(jq -r .target_session_id <<<"$shutdown")" \
  --until deregistered --timeout-seconds 300 --json || exit
```

For a previous `exit_after_idle` worker, cordon itself requests the exit. Skip
`shutdown-after-idle` (unsupported) and wait for `deregistered` with the cordon
receipt's generation. Never infer clean exit from an expired/offline session.

Both waits require a cordon, pin the lifecycle generation and session, count
cleanup across all the worker's sessions, and fail if the target changes. The
deadline includes API requests. JSON goes only to stdout; progress and request
UUIDs go to stderr. Reuse a request UUID only to recover the receipt of the
same uncertain mutation. Receipt replay never repeats or retargets an action.

Before touching binaries, config, mounts or rebooting, stop the selected
systemd unit and verify it is inactive on its host. Database deregistration is
not proof that a host process has exited. After installation, run local
preflight and `fleet check`, start the service while still cordoned, and inspect
the running binary/session. Uncordon explicitly only after successful checks.
A failed check leaves the worker cordoned; do not uncordon as an error cleanup.
Previous workers exit while cordoned and need an explicit service start after
uncordon; current workers can remain resident throughout the canary checks.

`worker-status` also reports `admission.schedulable`, stable blocker codes,
effective task capabilities and measurement profile. Admission accounts for
registry enablement, cordon, scheduler pause, session liveness/state, bound-key
authorization, protocol support/floor, capability intersection, shutdown intent,
active attempts and cleanup. All facts come from one database snapshot.

After explicitly uncordoning an existing session, wait for admission with:

```bash
sbgh fleet wait-worker --worker-id "$worker_id" \
  --until schedulable --timeout-seconds 300 --json
```

This wait retains the same generation/session checks; it does not follow a
replacement worker, uncordon, enable an identity or resume the scheduler. An
older API without admission facts fails explicitly. Schedulability is an
instantaneous worker-wide observation, not a capacity reservation or a promise
that a particular job's profile/placement matches. A busy worker may remain
blocked until its current attempt finishes. Active single-host maintenance
remains in v35; physical multi-worker rolling maintenance is the `0080`
onboarding gate in the [planning registry](../planning/index.md).

`worker-status` includes the latest session's optional `preflight` record:
check revision, worker-clock completion time, executable SHA-256 and loaded
configuration SHA-256. The worker reports it only after successful startup
checks; recovery retains exactly the original record. Previous workers report
no record (`preflight=unreported`), not an assumed pass. This is authenticated
worker-reported provenance, **not remote attestation**, continuing health or
proof of the source commit used to build the binary. Compare its executable
digest with the operator's retained build evidence and `/proc/<pid>/exe`.

Revision 1 fingerprints the default-expanded configuration's compact JSON with
the `sbgh-worker-preflight-config-v1` domain prefix and a trailing NUL byte.
Comments and TOML formatting do not matter. Key paths are included, but private
key contents and referenced image/chainstate contents are not read into this
fingerprint. It is not the raw TOML file hash. Changes to the check suite or
fingerprint encoding require a new revision; binary hashes distinguish builds.
`--preflight-only` logs the same fields but does not register or update a session.

### Failed VM forensics

Inspect a terminal attempt through the authenticated operator API:

```bash
sbgh jobs report <job-or-submission-id>
```

This command retains its JSON submission-report output. The explicit
`--submission-id` form remains supported; `--job-id` resolves a job's containing
submission. Use `show` or `results` for job-scoped human output.

The report's `forensics` object contains the last phase, the final 1 MiB of the
guest serial console, the original console size, and the promoted
`console.tail.log` artifact key. The full QEMU console is intentionally not
retained: an untrusted build may emit unlimited output, while the bounded tail
normally contains the compiler or guest-shutdown failure. GitHub and Slack
renderers do not publish this untrusted console text.

While an attempt is still running, the same serial output is available only on
that worker at `/var/lib/sbgh-worker/jobs/<attempt-id>/console.log`. Verified
teardown removes the ephemeral attempt directory after archiving the bounded
tail.

For block validation, each `stacks-inspect` launch writes an
`SBGH_STACKS_INSPECT_COMMAND` JSON record to that console before spawning and
uses separate `shard-<n>-command-<n>.stdout.log` and `.stderr.log` artifacts.
Use the job UUID from `jobs list` or `jobs report` to inspect promoted artifacts
without S3 credentials on the operator shell:

```bash
sbgh jobs artifacts ls <job-uuid>
sbgh jobs artifacts cat <job-uuid> block-validation/block-validation-result.json | jq .
```

`cat` writes only artifact bytes to stdout. The daemon authorizes the admin
cookie, resolves the name against that job's terminal manifest, and reads from
its configured artifact store. It does not accept an arbitrary store key.
The terminal reducer independently reconstructs the expected argv and requires
the command's final `Validating: 100% (N/N)` count to equal its trusted
half-open range. These records are audit evidence for accidental orchestration
drift; they are not cryptographic attestation of repository-built guest code.
While validation runs, the guest tails each command's bounded stdout and emits
monotonic, block-weighted progress across all active shards. GitHub and Slack
show `processed/selected (percent)` plus completed shards. This live telemetry
is best effort; only the terminal reducer determines the validation result.
If a completed command contains no parseable live samples, its console records
one `SBGH_BLOCK_VALIDATION_PROGRESS_WARNING` for canary diagnostics.

## Coordinated protocol deployment

The deployed predecessor uses revision 1. The v35 implementation adds
revision 2 and negotiates the bounded current/previous worker window. It must
be deployed server-first; an old daemon cannot admit a revision-2 worker.
See the [supported baseline](#supported-protocol-baseline) for the exact
predecessor, and [coordinated update](#coordinated-daemon-and-worker-update)
for the conservative qualification workflow.

Worker wire compatibility is separate from daemon/database rollback. The old
daemon cannot start against the expanded migration ledger. Retain the approved
[backup/restore policy and recorded exception](#recovery-policy-for-the-v35-schema-rollout);
do not infer binary-only rollback support from successful mixed-worker tests.

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
sudo install -d -m 0755 -o sbgh-worker -g sbgh-worker \
  /run/sbgh-worker /run/sbgh-worker/jobs
sudo -u sbgh-worker sbgh-worker \
  --config /etc/sbgh/worker/benchmark.toml \
  --preflight-only
```

The worker reports host-wide online CPU count, not the adapter process's
affinity count. This distinction is intentional on benchmark hosts where the
adapter and OS are restricted to housekeeping CPUs while libvirt pins guest
vCPUs onto isolated CPUs. Preflight requires every configured recipe CPU set
and optional `sandbox.host_cpus` set to contain only online CPUs, requires
enough CPUs for its VM, and rejects overlap between guest and housekeeping
sets. CPU placement comes only from worker policy; the daemon cannot override
it.

Verify the endpoint from the worker host before enrollment. This calls the
standard gRPC health service through the production TLS connector and requires
a `SERVING` response without creating a worker session:

```bash
sudo -u sbgh-worker sbgh-worker \
  --config /etc/sbgh/worker/benchmark.toml \
  fleet check --connectivity-only
```

After the public identity is authorized and the drained worker policy is
enabled, verify registration readiness without polling or claiming work:

```bash
sudo -u sbgh-worker sbgh-worker \
  --config /etc/sbgh/worker/benchmark.toml fleet check
```

The full check is read-only: it does not register, replace, fence, or deregister
a worker session, so it is safe while the worker service is active. It verifies
the server-derived worker identity, protocol revision, authorized capability
intersection, drain/profile policy, fleet timing, and bounded daemon/worker
clock skew. Use `sbgh-cli fleet status` and `show-worker` for the durable policy
and live-session view.

Worker startup and the full check reject daemon/worker wall-clock skew above 30
seconds. This is fail-closed because wall time affects credential and delegated
artifact expiry even though lease deadlines use a monotonic clock. The worker
logs the measured `clock_skew_ms` after registration. If the check fails, verify
the time-synchronization service on both hosts (for example,
`timedatectl status`) before restarting the worker; do not relax the bound to
mask a broken clock.

Use the block-validation profile on that host. Preflight opens no fleet
session and checks:

- worker config and profile resources against discovered CPU and memory;
- worker-owned guest/housekeeping CPU placement against Linux online CPUs;
- identity private key;
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

For scheduled refreshes, first retire any previous cron or systemd schedule
and let an in-progress manual download finish. The worker installer installs
the service and timer without enabling them. Verify the installed command,
then enable the daily timer (04:00–04:15 local time):

```bash
sudo systemctl cat sbgh-chainstate-refresh.service
sudo systemctl enable --now sbgh-chainstate-refresh.timer
systemctl list-timers --all sbgh-chainstate-refresh.timer
journalctl -u sbgh-chainstate-refresh.service -n 50 --no-pager
```

Missed runs are not caught up; the timer starts at the next scheduled time.

The new LV is published only after checksum
verification and extraction complete, then set read-only and deactivated.
The worker selects the lexicographically newest matching name when preparing
an attempt. Streaming keeps only a bounded temporary range window, retries a
failed range from its first missing byte, and does not retain the complete
compressed archive. A process or host restart therefore starts the archive
again from byte zero.

The downloader removes older origins only when they have no active snapshots.
Use `--keep-old` when retaining history or diagnosing a change. Never make a
published origin writable. If preflight reports a writable newest origin,
drain the worker and correct it explicitly:

```bash
sudo lvchange --permission r vg0/mainnet-YYYY-MM-DD
```

The selected origin, guest-observed epoch counts, resolved global range,
epoch-local segments, shard count, and concurrency are recorded with block
validation results. `recent` saturates at the observed Nakamoto count and uses
`stacks-inspect last N` to select by block height, not by hash-ordered index
offset. It runs on one shard because that upstream mode cannot be partitioned
without changing the selected set. Its reported range is a logical ordinal
accounting range, not a block-height range. `full` covers both epochs. A worker
whose local chainstate cannot cover an explicit range fails the attempt as
infrastructure; it must not fabricate a partial verdict.

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
The policy unit invokes `nft` directly and owns only the
`inet sbgh_sandbox_egress` table; it must not start the host-wide
`nftables.service`, which may flush nft-backed Docker, libvirt, or firewall
rules.

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

Then run one end-to-end canary with the intended
`target_blocks_per_shard`, shard ceiling, and concurrency ceiling. Start
conservatively and tune worker-local policy from real duration and host
telemetry. CPU, memory, and device count are admission limits; synthetic
storage-throughput prediction is not a release gate.

## Identity-key lifecycle

### Rotate a worker identity

1. Generate a replacement P-256 key on the worker and export its public SPKI.
2. Authorize the replacement public key while the old identity remains
   active:

   ```bash
   sbgh fleet authorize-identity \
     --worker-id <worker-uuid> \
     --public-key /path/to/replacement-public.pem
   ```

3. Drain the worker and wait for its active attempt and cleanup to reach zero.
4. Atomically replace `identity.key` and keep it mode `0600`.
5. Restart the worker and confirm registration under the same UUID.
6. Revoke the old identity digest:

   ```bash
   sbgh fleet revoke-identity \
     --worker-id <worker-uuid> \
     --identity <old-lowercase-sha256>
   ```

7. Undrain the worker.

The old/new overlap takes effect immediately and requires no daemon restart.

### Revoke

- For planned removal, drain the worker, wait for quiescence, then use
  `revoke-identity` and `disable-worker`.
- For a suspected compromise, use `emergency-revoke-identity` or
  `emergency-disable-worker`. These immediately reject the next RPC on an
  existing connection and expire affected leases. The fleet coordinator then
  fences attempts and records cleanup/requeue through the normal state
  machine.
- Use network containment while the compromised worker is fenced.

The daemon rechecks identity and registry authorization on every worker
request. Revoked identities remain auditable and can never be reassigned or
reactivated.

### Change worker policy

Drain the worker and wait for active attempts and cleanup obligations to reach
zero before changing capabilities, measurement profile, or enabled state:

```bash
sbgh fleet drain --worker-id <worker-uuid>
sbgh fleet update-worker \
  --worker-id <worker-uuid> \
  --capability benchmark \
  --measurement-profile <profile>
sbgh fleet enable-worker --worker-id <worker-uuid>
```

Restart the worker after a capability/profile change so its new session
advertisement is intersected with the updated policy. Display-name changes and
drain state do not require a restart.

### Rotate the lease key

The lease HMAC key must contain at least 32 random bytes and be readable only
by the daemon. Rotate it only after all workers are drained and no attempt or
cleanup obligation remains; replacement invalidates every outstanding lease.

## Deploy an application update

### Daemon lifecycle and server-first rollout

With the v35 lifecycle implementation, daemon SIGTERM and first SIGINT stop
local intake/listeners without persisting worker drain or cancelling attempts.
A second SIGINT forces local exit. Local graceful shutdown is bounded by one
configured heartbeat interval (10 seconds with the recorded host settings).
The coordinator no longer waits for remote jobs to finish before daemon exit.

Workers with the recovery implementation reconnect using the same session.
They check cleanup before every new poll, and safely recover an expired idle
session only when no successor owns the identity. Older daemons return the
legacy stale-session error; upgraded workers still fail closed against them.
Superseded sessions never mint a replacement UUID automatically.

Each new TLS connection issues a fresh one-hour client certificate wrapper using
the same enrolled P-256 key. Wrapper renewal is not key rotation or fleet session
registration. Expired/not-yet-valid wrappers remain rejected; check host clock
alignment and the installed worker version if those handshake errors recur.
Workers predating this fix reuse their startup wrapper and can lose connectivity
after a daemon restart once it expires. A worker restart is temporary mitigation,
not a fix; use normal maintenance controls and account for active work first.

Install daemon changes first, while the fleet is quiescent, then update workers
using the controlled update scripts. The original pre-v35 daemon had destructive
shutdown behavior; the operator-qualified `d0b6852` daemon already has local-only
shutdown. The pending negotiation/maintenance release adds migrations and
additive protobuf fields, unlike that first lifecycle patch. Do not use an
active-job restart as the first schema-upgrade ceremony.

After both sides are upgraded, a short compatible daemon restart can preserve
active jobs only inside their **remaining confirmed lease**, including reconnect
and renewal time. The recorded 45-second TTL is not a guaranteed outage budget.
Workers cancel at a conservative monotonic deadline independently of blocked
heartbeat RPCs; reassignment remains gated on verified cleanup. Startup waits
two heartbeat intervals before its first expiry sweep but never renews expired
authority. Keep timing settings unchanged during an active-job restart canary.

For longer outages, pause admission and wait for quiescence as described below.
The scheduler and resident worker controls were deployed on October 18.
Explicit revision-1 `fleet drain` still exits the worker after idle; undrain
plus service start remains necessary for that deliberate legacy action.
Use authenticated job cancellation or emergency worker
disable/revoke for destructive actions, never OS signals as a fleet abort.

The [v35 closure checklist](../planning/iterations/v35-seamless-fleet-lifecycle.md#final-validation)
defines the remaining single-host checks. Automated replay/signal tests and
scoped prior restart evidence are retained; the full failure matrix is not a
mandatory production fault-injection ceremony.

### Explicit fleet abort

Normal service signals never cancel remote work. To deliberately cancel fleet
work, first pause admission and inspect its generation. The
abort control requires admin authorization, an explicit scope and a reason:

```bash
pause_generation=$(sudo -u sbgh sbgh-cli fleet scheduler pause \
  --reason 'Operator: deliberate workload cancellation' --json | jq -r '.generation')
sudo -u sbgh sbgh-cli fleet abort --scope active \
  --generation "$pause_generation" --reason 'Operator: stop current execution' --json
sudo -u sbgh sbgh-cli fleet scheduler wait \
  --generation "$pause_generation" --timeout-seconds 300 --json
```

`active` selects executing jobs, including jobs whose expired attempts still
owe cleanup. Use `all-pending` **instead** only when also cancelling currently
queued/claimed jobs is intended. Both select existing jobs once, not future
intake or unmaterialized submission runs. Pause blocks assignment, not intake;
newly arriving jobs remain queued. A concurrent terminal result may win before
cancellation, so receipts distinguish selected from accepted job IDs.

The receipt is atomic with cancellation and is append-only. Preserve the
`request_id` printed to stderr; retry with that UUID and identical arguments
after a lost response. Replay returns the original selection, even after resume,
and cannot cancel later arrivals. It does not pause, resume, cordon or stop any
process. A changed pause generation rejects a new request without mutation.

Acceptance is **not** proof of teardown. Active jobs receive the existing
heartbeat cancellation directive. Expired attempts awaiting cleanup have their
retry obligation changed to cancellation; their job remains nonterminal until
cleanup acknowledgement. Continue to use scheduler wait and host resource
checks before maintenance. Resume only by a separate explicit command. This
control has automated acceptance coverage; no destructive production drill is
required solely for v35 closure. Actual emergency use still needs teardown checks.

### Supported protocol baseline

The supported previous protocol contract is the deployed `0f18950` build,
approved on 2026-10-17. Earlier pre-v33 layouts reused revision 1 despite
incompatible wire changes and are unsupported. Verify build provenance: the
revision number and package version alone cannot identify that historical
cohort. Only the Hetzner worker is currently deployed. Compatibility qualification
targets that predecessor and the new worker, not arbitrary historical binaries.
The October 18 server-first deployment verified predecessor registration and
legacy exit, followed by current-worker revision-2 registration and both workload
canaries. Source-pinned previous/current TLS/RPC tests supplement that evidence;
it does not prove concurrent mixed-cohort workloads on separate physical hosts.
That operational gate belongs to `0080` before multi-worker production rollout.
For future updates, retain release review, a verified recovery package and
operator-controlled canaries; upgrade the daemon before the worker.

After deploying that API, `fleet show-worker` includes `worker.protocol`: selected
`revision` and `features`, plus the explicit `offer` when advertised. A null offer
means the fixed legacy contract; a null protocol means no recorded session or an
older daemon. `fleet status` appends selected protocol/features. These describe
the latest session, including offline sessions: check liveness separately before
counting a worker as upgraded. Capabilities remain authorization, not proof of
protocol feature support.

### Compatibility-floor maintenance

These controls have automated acceptance coverage. Keep the floor at 1 during
this server-first rollout; advancing it is not a v35 qualification exercise.
Upgrading workers does not require immediately retiring revision 1.

When deliberately retiring the old revision, inspect the policy and every
enabled worker's live session before advancing the floor. All enabled workers count as required,
including drained workers; an offline or unregistered enabled worker blocks
advancement. Disable a retired worker only through an explicit policy change.

```bash
sudo -u sbgh sbgh-cli fleet protocol status --json
sudo -u sbgh sbgh-cli fleet status
sudo -u sbgh sbgh-cli fleet scheduler pause --reason 'Qualified protocol upgrade'
sudo -u sbgh sbgh-cli fleet scheduler wait --timeout-seconds 300
sudo -u sbgh sbgh-cli fleet protocol set-floor --minimum-revision 2 \
  --reason 'All required workers upgraded' \
  --rollback-evidence 'REFERENCE-TO-COMPLETED-ROLLBACK-QUALIFICATION' --json
sudo -u sbgh sbgh-cli fleet protocol status --json
```

Replace the evidence placeholder with the actual qualification record. The server
requires paused admission, zero attempt/cleanup/projection/artifact debt, and live
compatible sessions before raising the floor. It serializes this check with
registration, admission and enabled-worker membership changes. A reference is an
operator declaration, not automatic verification of the referenced evidence.

Changes retain an append-only audit with the shared `api:admin` principal, reason,
evidence reference and request UUID. Reuse the printed UUID with `--request-id`
after an uncertain response; replay returns the original receipt, not necessarily
the current policy. Inspect status separately. Rejected changes leave policy
unchanged. No-op requests are audited without advancing the generation.

An explicit rollback to floor 1 uses the same command with a fresh request UUID
and requires paused, quiescent maintenance. It does not rewrite existing session
contracts. Resume scheduling explicitly after checking the resulting policy.
Lowering the floor alone does **not** qualify an old daemon binary: migration
ledger compatibility and its lack of maintenance-policy enforcement must also
pass the separate rollback procedure. Never start an old daemon while relying
on controls it does not implement.

### Scheduler maintenance

The scheduler gate is a separate daemon/CLI deployment with an additive database
migration. It does not require a worker protocol change. Do not use these
commands until that release is installed; the October 16 recovery worker alone
does not provide this gate.

```bash
sudo -u sbgh sbgh-cli fleet scheduler status --json
sudo -u sbgh sbgh-cli fleet scheduler pause --reason 'Operator: planned daemon maintenance'
sudo -u sbgh sbgh-cli fleet scheduler wait --timeout-seconds 1800
```

Pause persists across daemon restarts. It blocks new source freezing, payload
preparation, offers, and acceptance. Unaccepted offers are withdrawn and their
jobs requeued atomically; accepted work continues. It does not change worker
drain policy or terminate sessions. Submission intake, reporting, heartbeats,
cleanup, terminal submission, and artifact promotion remain available.

`wait` pins the first observed pause generation (or `--generation N`) and fails
if another operator resumes/repauses it. Success requires zero offered/running/
cancelling attempts, cleanup obligations, unprojected reliable events, and
active-attempt uploads or accepted terminal-manifest artifacts awaiting promotion.
Even a zero-byte required artifact counts. `garbage_artifacts` reports unreferenced
staging retained for garbage collection; it does not block maintenance. The
configured staging retention grace still applies. Timeout or API
failure exits unsuccessfully and never changes policy. Inspect remaining debt
instead of deleting records to force success. This is a point-in-time readiness
check, not an exclusive maintenance reservation; coordinate other operators.

After maintenance and API health verification:

```bash
sudo -u sbgh sbgh-cli fleet scheduler resume --reason 'Operator: maintenance complete'
sudo -u sbgh sbgh-cli fleet status
```

Pause/resume print a request UUID to stderr before sending. On an uncertain
response, retry the same action/reason with `--request-id UUID`. Replays return
the original receipt without reapplying an old change; inspect `status` for
current policy. Reusing the UUID differently returns a conflict. Fresh no-op
requests are audited but do not advance the generation. Read tokens can inspect
status; only admin can pause/resume. The append-only audit records the shared
authenticated principal `api:admin`; names included in reasons are operator
claims, not individually authenticated identities. New commands support
`--json`; diagnostics and wait progress go to stderr.

Roll out server-first at quiescence with a database backup. Previous daemons do
not enforce this gate: never rely on a paused row while serving an older daemon.
For rollback, use a compatible build with the same migration ledger, or follow
the separately qualified backup/restore procedure with workers stopped and
drained. Do not down-migrate or erase the audit. The pinned predecessor's startup
migrator rejects the newer ledger, even though the migration is additive.
Binary-only rollback to that predecessor will not start. Never remove
migration-history rows or disable verification to bypass it.

The predecessor also predates scheduler-pause, protocol-floor and targeted
shutdown admission checks. Bypassing its migration check is unsafe, not a
shortcut to compatibility. No bridge release is required under the approved
recovery policy below.

Local qualification restores a pre-upgrade dump into a separate test database
and verifies that the predecessor migrator accepts it with the original ledger
and worker policy intact. The October 18 host database restore passed; the
operator waived isolated application rehearsal for that single-host rollout.
This is an accepted recovery limitation, not proof of restored daemon startup.
Keep intake and workers stopped through any restore: a pre-upgrade backup
cannot retain later submissions,
results, audit changes or provider effects. After reopening intake, prefer a
qualified forward repair; any restore needs explicit reconciliation of that
post-backup work, not just a successful `pg_restore`.

#### Scheduler host qualification

After release review and server-first installation, use a controlled canary and
coordinate submission intake with other operators. Keep the existing worker
binary for this check; scheduler pause does not require a worker update.

1. Capture release/binary hashes, worker PID/invocation/restart count, session
   UUID, drain policy, scheduler status, and the canary's accepted attempt UUID.
2. Pause while that attempt is running. Retain the request UUID and receipt;
   submit one additional canary and confirm it stays queued without an attempt.
   The accepted attempt must continue heartbeating and complete normally.
3. Run `fleet scheduler wait --generation N --timeout-seconds 1800`, replacing
   `N` with the pause receipt's generation. Capture its zero-debt snapshot. If
   it times out, inspect the remaining debt; do not resume or clear records just
   to obtain a passing result.
4. Restart only the daemon while paused. Confirm the same paused generation,
   queued canary, resident worker process/session, and unchanged drain policy.
5. Resume, retain its request UUID, and retry that exact request once. Both
   receipts must agree; generation must advance only once. Confirm the queued
   canary is offered and completes without restarting or undraining the worker.
6. Retain final scheduler/fleet status, both canary reports, and audit rows for
   the request UUIDs. Check offer timestamps/generations in the database, not
   sampled CLI output alone, before marking the admission gate qualified.

This procedure qualifies scheduler behavior, not old-daemon rollback or
multi-host operation. A failure leaves maintenance policy for explicit operator
review; do not automatically resume scheduling.

### Worker-only update

For a wire-compatible worker update on a host that also has authenticated
operator CLI access, run from the updated checkout:

```bash
sudo ./scripts/update-worker-host.sh
```

The script requires the v35 maintenance CLI/API before changing policy. It
derives the configured identity, cordons only that worker, pins the receipt
generation while waiting for its attempts and cleanup, and requests explicit
idle shutdown when supported. It stops the local unit, installs `sbgh-worker`,
then runs preflight and registration checks while still cordoned. A replacement
advertising resident cordon must establish a new live session before uncordon;
a previous worker is uncordoned before its service start because it would
otherwise exit. Readiness reports effective cordon semantics, not merely the
protocol revision.

Unrelated workers may continue execution and cleanup. Failure recovery attempts
to cordon only the target and reports if it cannot confirm that policy. Before
admission reopens it also stops the already-quiesced instance; afterwards it
does not stop blindly, since new work may have been accepted. Use
`--profile NAME` for another local instance,
`--timeout-secs N` for long-running attempts, or `--no-build` for an existing
release binary.

If a retry finds the worker **stopped with pending cleanup**, waiting alone cannot
make it quiescent. Inspect `fleet worker-status --worker-id UUID --json`, the
failure log, and the installed binary/configuration first. Do not clear cleanup
rows, uncordon to bypass the gate, or start a binary that failed preflight.
After repairing the failure, run the installed worker's `--preflight-only` and
`fleet check` with the same service user/config. For a compatible worker reporting
`cordon_semantic=resident`, confirm it remains cordoned, manually start only its
systemd instance, and wait for the new live session and acknowledged cleanup.
Then rerun the updater. Keep scheduling policy unchanged and use a fresh
`worker-status` snapshot rather than pinning a retired session.

A legacy exit-on-drain worker may exit before processing cleanup. Do not apply
the resident procedure or blindly uncordon it: use the coordinated release path
with paused admission and an explicitly reviewed compatible recovery binary.
If preflight or registration still fails, leave the worker cordoned and resolve
that failure before retrying.

This workflow does not update or restart the daemon. Use the coordinated update
for protocol, schema, or daemon-and-worker changes.

### Daemon-only update on the combined host

For a daemon-only, wire-compatible update while the deployment has exactly one
registered worker, run from the updated checkout:

```bash
sudo ./scripts/update-combined-host.sh
```

The script derives the `combined` worker identity from its authenticated fleet
check, drains it, waits for attempts and cleanup to reach zero, backs up
Postgres, installs and restarts the daemon, verifies the API and production
worker TLS path, and returns the worker online. A failure after draining leaves
the worker drained and, once quiescent, stopped. Use `--profile NAME` for a
different local instance and `--no-build` for already-built release binaries.

This path does not install a worker binary. Do not use it for worker-protocol,
worker-runtime, or migration compatibility changes; use the coordinated update
below. It also refuses multi-worker fleets because the script only coordinates
one local worker; daemon shutdown itself no longer globally drains workers.

### Coordinated daemon and worker update

Until the negotiated rollout and rollback gates are qualified, use this
conservative coordinated-release path rather than assuming arbitrary versions
are compatible:

1. Drain all workers.
2. Wait for active attempts and cleanup obligations to reach zero.
3. Stop worker services.
4. Take a database backup and retain the current binaries/configuration.
5. Build and validate the new checkout with `just build`, `just lint`, and
   `just test`.
6. On the control-plane host, install with
   `sudo ./scripts/install-daemon.sh --no-start`.
7. On every worker host, install with `sudo ./scripts/install-worker.sh`; the
   installer leaves all instances stopped.
8. Start the daemon; it applies forward-only migrations.
9. Start workers from the same release.
10. Verify worker key identity, TLS, protocol, capabilities, preflight, and
    fleet state.
11. Run benchmark and block-validation canaries, then undrain.

Never run an older binary against a schema it was not designed to read.
Rollback across a schema-changing deployment means stopping traffic and
restoring the matching database backup before starting the retained binary.
Binary-only rollback to the pinned predecessor is unsupported for this rollout.

### Recovery policy for the v35 schema rollout

Operator-approved on 2026-10-17 for the single-host rollout: prefer a reviewed
roll-forward fix that preserves the current database and audit history. If
repair cannot safely restore service, use a coordinated backup/restore fallback;
do not start the old daemon on the expanded schema. This changes daemon rollback
scope only, not current/previous worker compatibility or fencing guarantees.

Before migration, freeze intake and quiesce all attempts, cleanup, artifact and
projection debt. Stop database-writing services before capturing the recovery
point. Retain a verified PostgreSQL backup and matching executable hashes,
configuration, service units and source revision; keep secrets protected.
Verify the backup by restoring into an isolated database. For the October 18
single-host rollout this passed; the operator explicitly waived additional
isolated application/migration rehearsal. Retain that exception and its limits
in the [rollout record](../planning/qualification/2026-10-18-v35-rollout.md).
Do not describe archive integrity or database restore as proof of retained
daemon startup. Future rollout plans must make their own recovery-risk decision.

For a failure after rollout:

1. Contain intake and new assignments. Use scheduler pause if the API is healthy;
   if not, use the documented external service controls. Do not weaken leases.
2. Preserve logs and the failed database state. Prefer a tested forward fix, then
   verify health and canaries before resuming admission.
3. If the operator chooses restore, stop intake, workers and database-writing
   services; verify physical VM/mount/snapshot cleanup before restoring state.
4. Restore the matching database, binaries, configuration and units together.
   Verify health and identity/policy state while intake and workers remain stopped.
5. Reconcile post-backup submissions, accepted results, artifacts, provider
   comments/checks and webhook redeliveries before re-enabling traffic. Preserve
   the failed database for this inventory; a database restore does not undo S3
   objects or external messages. Do not silently discard or automatically replay
   that work.

Keep the compatibility floor at 1 during this qualification. v35 closeout records
the completed database restore, matching recovery package, accepted application
rehearsal exception and operator sign-off. None authorizes a production restore.

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
- worker drain and identity rotation/revocation.

The invariants are:

- at most one current attempt per scheduling unit;
- exactly one accepted terminal per attempt;
- no mutation by a stale fence;
- no logical artifact visibility before terminal acceptance;
- no negative validation verdict unless every shard exited normally;
- no repository-produced process on the worker host;
- unresolved teardown remains visible as cleanup work and blocks unsafe requeue.
