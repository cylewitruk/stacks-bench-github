# Deployment Qualification

Use this playbook for the first production deployment and when commissioning a
new worker host. [Setup](setup.md) owns installation details;
[worker-fleet-operations.md](worker-fleet-operations.md) owns routine operation.
This document orders their commands into fail-closed gates and records the
evidence needed to enable work.

Do not put tokens, private keys, complete environment files, repository-read
credentials, or presigned URLs in a qualification record. Keep workers drained
until the gate that explicitly undrains them.

## Host-action inventory

Review this table against the checkout before changing a host. `manual` means
the action is intentionally environment-specific, not optional.

| Area | Backing asset | Authority | Evidence |
| ---- | ------------- | --------- | -------- |
| Control-plane install | `scripts/install-daemon.sh`, `systemd/sbgh-daemon.service` | daemon host root | only daemon/CLI/unit installed |
| Worker install | `scripts/install-worker.sh`, `systemd/sbgh-worker@.service`, global hardening drop-in | worker host root | only worker artifacts installed; no instance started |
| PostgreSQL/edge | `docker/docker-compose.yml`, `docker/.env.example` | daemon operator | healthy Postgres, handler, and smee |
| Artifact storage | configured S3-compatible provider client (`manual`) | daemon operator | qualification object put/head/get/checksum/delete succeeds |
| Backup/restore | `scripts/pg-backup.sh`, `scripts/pg-restore-check.sh`, `systemd/sbgh-pg-backup.*` | daemon host root | one restored archive |
| Daemon users, paths, secrets | [setup sections 2, 4–7](setup.md#2-create-service-identities-and-directories) (`manual`) | daemon host root | owners/modes and redacted config review |
| Public DNS/TLS/firewall | operator DNS/ACME/firewall tooling (`manual`) | network operator | hostname-verified Web-PKI chain and restricted listener |
| Worker users, paths, sudoers | [setup sections 2 and 8](setup.md#8-prepare-each-execution-worker) (`manual`) | worker host root | owners/modes, `visudo`, denied extra command |
| Host inventory | `scripts/characterize-worker-host.sh` | worker operator | versioned Markdown record |
| Sandbox network | `scripts/install-sandbox-network.sh`, `apply-`, `check-`, `qualify-sandbox-network.sh`, `sbgh-sandbox-egress.service` | worker host root | live verifier and disposable-guest record |
| Golden image | `scripts/build-golden-image.sh` | worker host root | image digest and successful guest boot |
| Chainstate origin | `scripts/download-chainstate.sh` or managed-node snapshot tooling | worker host root | newest matching LV inactive and read-only |
| LVM isolation | `scripts/qualify-block-validation-lvm.sh` | worker host root | two writable snapshots isolated and removed |
| Worker config/preflight | checked worker examples; `sbgh-worker --preflight-only` | worker service user | parsed profile, host-resource and substrate checks |
| Worker identity/registry | `sbgh-worker identity`; `sbgh-cli fleet` | worker and daemon administrators | SPKI digest, stable worker UUID, authorized session |
| Controlled canary | `sbgh-cli jobs validate-blocks`, `jobs report` | daemon administrator | submission/job/attempt IDs and typed report |
| Provider canaries | GitHub comments and Slack Socket Mode | authorized users | canonical check/comment/message identities |

If an asset, command, package path, or required input differs on the target
host, update [setup.md](setup.md) or this table before proceeding. Do not keep a
private host-only correction in shell history.

## Qualification record

Create one operator-owned directory outside the repository. Use a new UTC
identifier for each attempt:

```bash
SBGH_QUALIFICATION_ID=$(date -u +%Y%m%dT%H%M%SZ)
SBGH_QUALIFICATION_DIR=/var/lib/sbgh/qualification/$SBGH_QUALIFICATION_ID
sudo install -d -m 0750 -o sbgh -g sbgh "$SBGH_QUALIFICATION_DIR"
git rev-parse HEAD
```

On each worker host, create its local evidence directory and copy the completed
records to the daemon-host directory after the gate:

```bash
SBGH_QUALIFICATION_ID=<record-id-created-on-daemon-host>
SBGH_WORKER_QUALIFICATION_DIR=/var/lib/sbgh-worker/qualification/$SBGH_QUALIFICATION_ID
sudo install -d -m 0750 -o sbgh-worker -g sbgh-worker \
  "$SBGH_WORKER_QUALIFICATION_DIR"
```

Record the release revision, hosts, OS/kernel, libvirt/QEMU/LVM/nft versions,
golden-image SHA-256, selected chainstate LV, worker UUID, identity-key digest,
submission/job/attempt IDs, provider object IDs, timestamps, and each gate's
outcome. The final section contains a template.

## Gate 1: Release and installer validation

Run locally on the exact revision to deploy:

```bash
just build --release --no-sccache
just lint --no-sccache
just test --summary --no-sccache
git diff --check
```

The installer tests use `DESTDIR` staging roots to prove ownership and
idempotency without root or systemd. `DESTDIR` is packaging/test support; real
installation leaves it unset.

Stop if any command fails. Record the revision and test summary, not the build
tree.

## Gate 2: Control-plane host

Complete [setup sections 1–7](setup.md#1-prepare-the-source-and-packages). The
control-plane host owns the daemon, CLI, PostgreSQL, handler/smee, provider
credentials, S3 credentials, lease key, and public fleet certificate. It owns
no worker identity, libvirt, LVM, mount, or sudo authority.

Before first start, verify:

```bash
sudo stat -c '%n %U:%G %a' \
  /etc/sbgh/daemon/config.toml \
  /etc/sbgh/daemon/secrets.env \
  /etc/sbgh/fleet/orchestrator.crt \
  /etc/sbgh/fleet/orchestrator.key \
  /etc/sbgh/fleet/lease-hmac.key
sudo -u sbgh test ! -r /etc/sbgh/worker/identity.key
```

Require `sbgh:sbgh 600` for daemon config/secrets and fleet private keys;
the public certificate may be `644`.

Before start, inspect the installed leaf and require it to remain valid through
the deployment window:

```bash
openssl x509 -in /etc/sbgh/fleet/orchestrator.crt \
  -checkend 86400 -noout
```

The listener may remain firewalled to worker addresses or a VPN even though its
certificate is publicly trusted. Confirm that the operator API is reachable
only from loopback and the intended local Docker bridge.

Install only control-plane artifacts and validate the unit:

```bash
sudo ./scripts/install-daemon.sh --no-start
sudo systemd-analyze verify /etc/systemd/system/sbgh-daemon.service
test -x /usr/local/bin/sbgh-daemon
test -x /usr/local/bin/sbgh-cli
```

Start PostgreSQL, then the daemon, then the edge containers:

```bash
docker compose -f docker/docker-compose.yml up -d postgres
sudo systemctl enable --now sbgh-daemon.service
sudo -u sbgh sbgh-cli status
docker compose -f docker/docker-compose.yml up -d --build handler smee
curl --fail http://127.0.0.1:8080/health
```

Require a clean daemon migration/startup log and an empty-but-healthy fleet:

```bash
sudo journalctl -u sbgh-daemon.service -b --no-pager
sudo -u sbgh sbgh-cli fleet status
```

Before accepting worker traffic, use the provider's S3-compatible client with
the daemon's configured endpoint, region, bucket, and credentials to put a
unique qualification object, HEAD it, download it, verify its SHA-256, and
delete it. Record the object key, checksum, and successful deletion, but never
the credentials or a presigned URL. A provider-console existence check is not
sufficient. Gate 5 separately proves that a worker can use delegated exact-key
HTTPS grants without receiving these credentials.

Now validate the running listener's public chain and hostname from a worker
network:

```bash
openssl s_client \
  -connect fleet.example.com:9443 \
  -servername fleet.example.com \
  -verify_hostname fleet.example.com \
  -verify_return_error </dev/null
```

Install the backup units, run one backup, and restore it into the isolated
restore-check target before provider traffic. Follow
[setup section 10](setup.md#10-enable-backups).

## Gate 3: Worker substrate

Complete the worker-only parts of [setup sections 1, 2, and
8](setup.md#8-prepare-each-execution-worker). Record host facts before tuning:

```bash
sudo -u sbgh-worker ./scripts/characterize-worker-host.sh \
  "$SBGH_WORKER_QUALIFICATION_DIR/worker-host.md" \
  /var/lib/sbgh-worker
```

Install the worker artifacts. This must not start an instance:

```bash
sudo ./scripts/install-worker.sh
sudo systemd-analyze verify /etc/systemd/system/sbgh-worker@.service
systemctl is-active --quiet sbgh-worker@combined.service && exit 1 || true
test -x /usr/local/bin/sbgh-worker
test -r /etc/systemd/system/sbgh-worker@.service.d/hardening.conf
```

Install and actively qualify the sandbox network:

```bash
sudo ./scripts/install-sandbox-network.sh --install-only
! systemctl list-dependencies sbgh-sandbox-egress.service | grep -q nftables.service
sudoedit /etc/sbgh/network/protected-ipv4.conf
sudo ./scripts/install-sandbox-network.sh
sudo systemctl is-active --quiet sbgh-sandbox-egress.service
sudo /usr/local/libexec/sbgh-check-sandbox-network
sudo ./scripts/qualify-sandbox-network.sh --execute \
  "$SBGH_WORKER_QUALIFICATION_DIR/sandbox-egress.md" \
  /var/lib/libvirt/images/sbgh-golden-ubuntu24.qcow2
```

Add `--deny-tcp IP:PORT` for the daemon's protected public address when a safe,
host-reachable TCP endpoint is available. The script refuses a vacuous probe.

Build the golden image if it is not already the recorded image, then publish a
current read-only chainstate origin:

```bash
sudo ./scripts/build-golden-image.sh \
  /var/lib/libvirt/images/sbgh-golden-ubuntu24.qcow2
sudo ./scripts/download-chainstate.sh \
  --vg vg0 --thinpool thinpool --prefix mainnet-
sudo lvs -o vg_name,lv_name,lv_attr,origin,data_percent,metadata_percent
```

Select the exact newest origin from `lvs`; do not use the placeholder below.
Mount it read-only only for the two-snapshot ceremony:

```bash
sudo install -d -m 0755 /mnt/sbgh-chainstate-origin
sudo mount -t xfs -o ro,nouuid /dev/vg0/mainnet-YYYY-MM-DD \
  /mnt/sbgh-chainstate-origin
sudo ./scripts/qualify-block-validation-lvm.sh --execute \
  "$SBGH_WORKER_QUALIFICATION_DIR/lvm-isolation.md" \
  vg0 mainnet-YYYY-MM-DD /mnt/sbgh-chainstate-origin
sudo umount /mnt/sbgh-chainstate-origin
sudo lvchange -an vg0/mainnet-YYYY-MM-DD
```

Install `/etc/sudoers.d/sbgh-worker` from the exact allowlist in setup. Require
both positive validation and denial of an unrelated root command:

```bash
sudo visudo -cf /etc/sudoers.d/sbgh-worker
if sudo -u sbgh-worker sudo -n /bin/true; then
  echo "unexpected broad worker sudo authority" >&2
  exit 1
fi
```

## Gate 4: Worker config, identity, and registration

For the first host, install the combined profile and edit every host-specific
placeholder:

```bash
sudo install -m 0600 -o sbgh-worker -g sbgh-worker \
  config.example.worker-combined.toml /etc/sbgh/worker/combined.toml
sudo -u sbgh-worker "$EDITOR" /etc/sbgh/worker/combined.toml
sudo -u sbgh-worker sbgh-worker \
  --config /etc/sbgh/worker/combined.toml --preflight-only
```

Generate the identity once. Keep the private key on the worker and transfer
only the public SPKI to the daemon administrator:

```bash
sudo -u sbgh-worker sbgh-worker identity generate \
  --private-key /etc/sbgh/worker/identity.key \
  > /tmp/sbgh-worker-public.pem
openssl pkey -pubin -in /tmp/sbgh-worker-public.pem -outform DER \
  | sha256sum
```

On the daemon host, create server-owned policy and authorize that public key:

```bash
alias sbgh='sudo -u sbgh sbgh-cli'
WORKER_ID=$(sbgh fleet add-worker \
  --display-name combined-fsn1-01 \
  --capability benchmark \
  --capability build_only \
  --capability block_validation \
  --measurement-profile hetzner-ax162 | jq -r .worker.worker_id)
sbgh fleet authorize-identity \
  --worker-id "$WORKER_ID" \
  --public-key /tmp/sbgh-worker-public.pem
sbgh fleet enable-worker --worker-id "$WORKER_ID"
sbgh fleet show-worker --worker-id "$WORKER_ID"
```

The worker remains drained. Start the process and verify that the authenticated
session advertises exactly the locally configured capabilities while effective
capability is their intersection with registry policy:

```bash
sudo systemctl enable --now sbgh-worker@combined.service
sbgh fleet status
sbgh fleet show-worker --worker-id "$WORKER_ID"
```

Restart the worker once. Require a new session bound to the same worker UUID,
with no offer while drained. Restart the daemon and require registry policy,
identity history, and drain state to remain unchanged. Only then:

```bash
sbgh fleet undrain --worker-id "$WORKER_ID"
```

## Gate 5: Controlled block-validation probe

Use an allowed repository and a full immutable commit. Pin this first canary to
the commissioned worker so the substrate under test is unambiguous:

```bash
SUBMISSION_ID=$(sbgh jobs validate-blocks \
  --install-id <install-id> \
  --repo-id <repo-id> \
  --commit <full-commit> \
  --worker-id "$WORKER_ID" \
  --idempotency-key "$SBGH_QUALIFICATION_ID/one-block" \
  --selection recent \
  --block-count 1)
sbgh jobs report --submission-id "$SUBMISSION_ID"
sbgh fleet status
```

Poll the report until terminal. Record its job/attempt IDs, selected origin,
observed pre-Nakamoto and Nakamoto counts, one-block resolved range, one shard,
concurrency, verdict, artifacts, and provider-neutral lifecycle. Require every
worker-produced artifact to have been uploaded through a delegated exact-key
HTTPS grant, promoted only after the accepted terminal result, and retrievable
from the configured S3 bucket with its recorded size and SHA-256. The worker
must have no S3 credentials.

Use the observed pre-Nakamoto count as the global epoch boundary. If it is
`P`, submit the inclusive range `P-1..P` with a new idempotency key:

```bash
CROSS_SUBMISSION_ID=$(sbgh jobs validate-blocks \
  --install-id <install-id> \
  --repo-id <repo-id> \
  --commit <full-commit> \
  --worker-id "$WORKER_ID" \
  --idempotency-key "$SBGH_QUALIFICATION_ID/cross-epoch" \
  --selection range \
  --range-start <P-minus-1> \
  --range-end <P>)
sbgh jobs report --submission-id "$CROSS_SUBMISSION_ID"
```

Require two exact, adjacent epoch segments and two checked blocks. After each
terminal, require no attempt domain, mount, loop device, thin snapshot,
temporary results directory, or unpromoted staging object remains. Investigate
any matching resource rather than deleting it blindly.

## Gate 6: Provider journeys

Complete repository policy and role setup from
[setup section 11](setup.md#11-authorize-github-use) and Slack setup from
[slack-setup.md](slack-setup.md). Then run, in order:

1. GitHub `/benchmark` on an allowed PR.
2. GitHub `/validate` on an allowed PR by a user with
   `trigger-block-validation`.
3. A natural-language Slack benchmark.
4. A natural-language Slack request to validate the latest small block count.

For each journey, record the immutable commit, submission/job/attempt IDs,
worker UUID, typed `jobs report` output, promoted artifacts, and GitHub or Slack
identity. Redeliver the same GitHub delivery or Slack envelope where the
provider permits it. Require one canonical submission and one provider object,
not merely two equivalent terminal results.

## Gate 7: Recovery and extended validation

These gates close the first deployment after the four primary journeys:

- Run `recent` with normal policy and verify resolution against the observed
  Nakamoto tail.
- Run `full` and require both epoch segments. A negative verdict is valid only
  after every shard exits normally.
- Repeat an explicit range crossing the observed epoch boundary and require
  exact, adjacent segment coverage plus agreement between the resolved plan,
  shard results, and trusted reducer.
- Cancel a running validation with `sbgh fleet cancel --job-id <job-id>` and
  require worker observation, VM teardown, fenced cancellation, and converged
  reporting. This does not claim user-facing lifecycle commands exist.
- Restart the daemon during an active attempt and after accepted terminal
  state; require lease/event recovery and the same report snapshot.
- Stop the worker during execution and during cleanup; require fencing and a
  visible cleanup obligation before any safe requeue.

Do not automatically move a partial benchmark comparison to another worker.
Explicit recovery starts a new execution generation from the first spec/run.

## Gate 8: Identity and registry drills

Generate a replacement key, authorize its public SPKI while the old key remains
active, drain, switch the worker key, and restart. Require the same worker UUID,
then revoke the old digest:

```bash
sbgh fleet revoke-identity \
  --worker-id "$WORKER_ID" \
  --identity <old-spki-sha256>
```

On a disposable/replacement session, verify emergency revocation rejects the
next RPC on the already-open connection and expires affected leases. Drain
before ordinary capability/profile changes, start a new session, and prove the
worker advertisement cannot expand server policy.

Passing Gates 1–8 qualifies the first fleet deployment. Keep the completed
record and pin semantic-digest vectors for this first deployed protobuf
revision before changing its wire or digest contract.

## Fast-follow: benchmark progress and comparison

These checks close the carried v20/v22 items but do not block declaring the
core fleet live:

1. Run a small benchmark and verify live, bounded snapshot progress plus the
   archived `run.progress.jsonl`.
2. Record whether calibration emits JSONL; silence remains a supported coarse
   phase fallback.
3. Ask Slack naturally for one workload across two explicit refs, initially one
   run per variant.
4. Repeat with two clean runs per variant. Require serial same-worker execution,
   fresh VMs, one calibration per variant, per-variant calibration reuse,
   carried SQLite state, correct run attribution, noise-aware aggregation, and
   one final Slack snapshot.
5. Restart the daemon after a durable event and require replay to converge on
   the same report and provider identity.

## Second worker host

Repeat Gates 3, 4, and the controlled canary on the second machine using a new
identity and registry row. Install only worker-owned artifacts. Workers may run
the same software release without rolling-version support; complete `0075`
before the first incompatible protocol change while independently upgraded
workers exist.

Require capability-aware pull placement, independent drain/revocation, and no
daemon restart or static fleet-file edit. Until this passes, describe the
deployment as separate-process/single-machine, not multi-machine ready.

## Stop and rollback

Stop and keep workers drained after any ambiguous authorization, fencing,
cleanup, reduction, artifact, or provider-idempotency result. Before retrying,
identify the previous attempt's domain, mounts, loop devices, snapshots, lease,
staging objects, and provider identity.

Daemon and workers currently require one exact protocol revision. Roll them
back together. If a failed deployment changed schema incompatibly, stop
traffic and restore the matching database backup before starting retained
binaries. Never bypass sandbox verification, make a base chainstate writable,
relax key authorization, accept a partial negative verdict, or mix comparison
measurements across execution generations.

## Record template

```text
qualification_id:
release_revision:
started_at_utc:
completed_at_utc:

daemon_host:
worker_hosts:
os_kernel:
libvirt_qemu_lvm_nft_versions:
golden_image_sha256:
chainstate_origins:

worker_ids:
worker_identity_digests:
protocol_revision:
measurement_profiles:

gate_1_release:
gate_2_control_plane:
gate_3_worker_substrate:
gate_4_identity_registration:
gate_5_controlled_probe:
gate_6_provider_journeys:
gate_7_recovery:
gate_8_identity_registry:

submission_job_attempt_ids:
github_check_comment_ids:
slack_message_identities:
artifact_keys_and_checksums:
cleanup_evidence:

v20_progress_fast_follow:
v22_comparison_fast_follow:
second_worker:

open_findings:
operator_signoff:
```
