# Setup

This guide installs the current `stacks-bench-github` deployment from a clean
Debian 12 or Ubuntu 24.04 host. It assumes one active `sbgh-daemon`
orchestrator, PostgreSQL on the daemon host, S3-compatible artifact storage,
and one or more `sbgh-worker` processes. A worker may run on the daemon host or
on a separate machine.

For the security and component model, read [architecture.md](architecture.md).
For routine operation after installation, use
[worker-fleet-operations.md](worker-fleet-operations.md).
For the first deployment and each newly commissioned worker, follow
[deployment-qualification.md](deployment-qualification.md) after completing
the relevant setup steps here.

## Deployment checklist

The required components are:

- a GitHub App with a webhook secret and private key;
- the host-side `sbgh-daemon` and operator `sbgh-cli`;
- PostgreSQL plus the containerized webhook handler and smee forwarder;
- S3-compatible object storage;
- a public Web-PKI certificate for the daemon fleet endpoint, worker P-256
  identity keys, and the PostgreSQL-backed worker registry;
- at least one KVM/libvirt worker with the managed `sandbox-egress` network,
  a golden VM image, and a read-only LVM chainstate origin.

Slack and LLM intent resolution are optional. See
[slack-setup.md](slack-setup.md).

## 1. Prepare the source and packages

Install common build packages on the daemon host and every worker host:

```bash
sudo apt update
sudo apt install -y \
  git curl openssl zstd uuid-runtime \
  build-essential pkg-config libssl-dev libclang-dev clang cmake
```

The daemon host also needs Docker:

```bash
sudo apt install -y docker.io docker-compose-v2
sudo usermod -a -G docker "$USER"
```

Start a new login session before running the Docker commands below.

Install `rustup` and `just` if they are not already available, then build the
same checkout on each host:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
cargo install just --locked

git clone https://github.com/cylewitruk/stacks-bench-github.git
cd stacks-bench-github
just build
just lint
```

Install these additional packages on every execution worker:

```bash
sudo apt install -y \
  qemu-system-x86 qemu-utils libvirt-daemon-system libvirt-clients \
  virtinst virtiofsd cloud-image-utils libguestfs-tools \
  git lvm2 util-linux e2fsprogs xfsprogs nftables iproute2 \
  aria2 zstd
sudo systemctl enable --now libvirtd nftables
```

Confirm that KVM is available:

```bash
test -c /dev/kvm
virsh list --all
```

## 2. Create service identities and directories

Use separate identities for the edge, orchestrator, and worker. Run the full
block below on the daemon host. A standalone worker host needs only the
`sbgh-worker` user and its paths. The numeric IDs match
[docker/.env.example](../docker/.env.example); choose unused IDs and update
`docker/.env` if they conflict locally.

```bash
getent passwd 900 901 902 903
getent group 900 901 902 903

sudo groupadd --system --gid 901 sbgh-handler
sudo useradd --system --uid 901 --gid 901 \
  --shell /usr/sbin/nologin sbgh-handler

sudo groupadd --system --gid 902 sbgh
sudo useradd --system --uid 902 --gid 902 \
  --home-dir /var/lib/sbgh --create-home \
  --shell /usr/sbin/nologin sbgh

sudo groupadd --system sbgh-worker
sudo useradd --system --gid sbgh-worker \
  --home-dir /var/lib/sbgh-worker --create-home \
  --shell /usr/sbin/nologin sbgh-worker
sudo usermod -a -G libvirt sbgh-worker
```

Create the host paths:

```bash
sudo install -d -m 0700 -o sbgh-handler -g sbgh-handler /etc/sbgh/handler
sudo install -d -m 0700 -o sbgh -g sbgh /etc/sbgh/daemon
sudo install -d -m 0750 -o sbgh -g sbgh /etc/sbgh/fleet
sudo install -d -m 0700 -o sbgh-worker -g sbgh-worker /etc/sbgh/worker
sudo install -d -m 0755 -o sbgh -g sbgh /var/lib/sbgh
sudo install -d -m 0700 -o 900 -g 900 /var/lib/sbgh/postgres
sudo install -d -m 0755 -o sbgh-worker -g sbgh-worker \
  /var/lib/sbgh-worker/jobs \
  /var/lib/sbgh-worker/results \
  /var/lib/sbgh-worker/git \
  /var/lib/sbgh-worker/binary-cache
```

The Postgres and smee containers use numeric identities only; they do not need
host users.

## 3. Create the GitHub App

Create an App at <https://github.com/settings/apps/new>. A smee channel can be
used as the webhook URL for this single-operator deployment.

Configure these repository permissions:

| Permission | Access |
| --- | --- |
| Checks | Read and write |
| Contents | Read-only |
| Issues | Read and write |
| Metadata | Read-only |
| Pull requests | Read and write |

Subscribe to:

- **Issue comment** for `/benchmark` and exact `/validate`;
- **Push** for branch triggers;
- **Create** for tag triggers;
- **Pull request** for current PR metadata.

Installation and installation-repository events are delivered automatically.
Generate a random webhook secret, record the App Client ID, and generate a
private key:

```bash
openssl rand -hex 32
sudo install -m 0600 -o sbgh -g sbgh \
  ~/Downloads/your-app.pem \
  /etc/sbgh/daemon/github-app.private-key.pem
```

Do not install the App on an account until the daemon is running and that
account has been added to the installer allowlist in step 11.

## 4. Configure PostgreSQL, the handler, and smee

Copy the checked-in container environment:

```bash
cp docker/.env.example docker/.env
$EDITOR docker/.env
```

Set:

- `SMEE_CHANNEL` to the App webhook channel;
- `POSTGRES_OWNER_PASSWORD` to `openssl rand -hex 32`;
- identity values only if the defaults from step 2 were unavailable.

Copy the handler config and create its secret environment:

```bash
sudo install -m 0600 -o sbgh-handler -g sbgh-handler \
  config.example.handler.toml /etc/sbgh/handler/config.toml
INGEST_TOKEN=$(openssl rand -hex 32)
WEBHOOK_SECRET='<the GitHub App webhook secret>'
sudo sh -c "umask 077; printf '%s\n' \
  'SBGH_WEBHOOK_SECRET=$WEBHOOK_SECRET' \
  'SBGH_API_INGEST_TOKEN=$INGEST_TOKEN' \
  > /etc/sbgh/handler/secrets.env"
sudo chown sbgh-handler:sbgh-handler /etc/sbgh/handler/secrets.env
```

Retain the ingest token for the daemon configuration. Start PostgreSQL first:

```bash
docker compose -f docker/docker-compose.yml up -d postgres
docker compose -f docker/docker-compose.yml ps
```

PostgreSQL listens only on `127.0.0.1:5432`. The daemon applies all pending
migrations at startup; no separate migration command exists.

## 5. Configure S3-compatible artifact storage

Create a private bucket and an access key for the daemon. Workers never receive
these credentials; they upload only through short-lived, exact-key grants.

Copy [config.example.daemon.toml](../config.example.daemon.toml) to
`/etc/sbgh/daemon/config.toml` and set:

- `[server].database_url` using the password from `docker/.env`;
- `[github].client_id` and `[github].private_key_path`;
- `[api].listen` to loopback plus the Docker bridge gateway address;
- `[artifacts]` to the S3 endpoint, bucket, and region;
- benchmark defaults and reporting policy appropriate for this host;
- `[tasks.block_validation]` with recent/full/range admission policy and the
  task timeout. Shard sizing and concurrency are worker-local policy.

```bash
sudo install -m 0600 -o sbgh -g sbgh \
  config.example.daemon.toml /etc/sbgh/daemon/config.toml
sudo -u sbgh $EDITOR /etc/sbgh/daemon/config.toml
```

The daemon API must not bind a public interface. Determine the Docker bridge
gateway with `docker network inspect bridge` or `ip addr show docker0`, and
firewall that listener to the local Docker network.

## 6. Configure fleet TLS

Give the fleet endpoint a public DNS name and a Web-PKI certificate (for
example, from Let's Encrypt). Install its full chain and private key:

```bash
sudo install -m 0644 -o sbgh -g sbgh \
  /path/to/fullchain.pem /etc/sbgh/fleet/orchestrator.crt
sudo install -m 0600 -o sbgh -g sbgh \
  /path/to/privkey.pem /etc/sbgh/fleet/orchestrator.key
sudo sh -c 'umask 077; openssl rand -out /etc/sbgh/fleet/lease-hmac.key 32'
sudo chown sbgh:sbgh /etc/sbgh/fleet/lease-hmac.key
```

The certificate DNS SAN must match every worker's `orchestrator_url`. The
listener serves protobuf/gRPC over HTTP/2; do not put an HTTP/1-only proxy in
front of it. Configure the ACME renewal hook to copy the renewed full chain
and key, preserve their ownership/modes, and restart `sbgh-daemon.service`.
Workers reconnect using the unchanged URL and platform trust store.

## 7. Configure the daemon secrets and install binaries

Create `/etc/sbgh/daemon/secrets.env`:

```text
SBGH_API_INGEST_TOKEN=<same value as the handler>
SBGH_ARTIFACTS_S3_ACCESS_KEY_ID=<access key>
SBGH_ARTIFACTS_S3_SECRET_ACCESS_KEY=<secret key>
```

Set it to `sbgh:sbgh` mode `0600`, then install without starting:

```bash
sudo chown sbgh:sbgh /etc/sbgh/daemon/secrets.env
sudo chmod 0600 /etc/sbgh/daemon/secrets.env
sudo ./scripts/install-daemon.sh --no-start
```

## 8. Prepare each execution worker

Install only worker-owned artifacts. The installer deliberately leaves every
profile stopped until configuration, preflight, enrollment, and policy are
complete:

```bash
sudo ./scripts/install-worker.sh
```

### LVM and chainstate

Create or select an LVM thin pool. Publish chainstates as dated, read-only thin
LVs whose names share the configured prefix, for example
`mainnet-2026-07-29`. The worker selects the lexicographically newest matching
origin and gives each attempt a writable snapshot; the origin itself is never
attached to a guest.

If the host does not already have a suitable pool, allocate one from free
extents in the selected volume group. Size it for the retained chainstate
origins plus operational headroom:

```bash
sudo vgs
sudo lvcreate --type thin-pool --name thinpool -L <pool-size> \
  --chunksize 256K -Zn vg0
sudo lvchange --monitor y vg0/thinpool
```

The downloader creates, verifies, populates, and publishes a suitable origin:

```bash
sudo ./scripts/download-chainstate.sh \
  --vg vg0 --thinpool thinpool --prefix mainnet-
sudo lvs -o vg_name,lv_name,lv_attr,origin,data_percent,metadata_percent
```

Schedule the same command nightly or on demand on every chainstate worker.
Keep the naming prefix identical across worker profiles. The worker validates
requested block coverage inside the selected snapshot; centralized dataset
coordination is not required.

### Sandbox network and golden image

Install the checked-in libvirt network and nftables policy:

```bash
sudo ./scripts/install-sandbox-network.sh --install-only
sudoedit /etc/sbgh/network/protected-ipv4.conf
sudo ./scripts/install-sandbox-network.sh
sudo systemctl is-active --quiet sbgh-sandbox-egress.service
sudo journalctl -u sbgh-sandbox-egress.service -n 50 --no-pager
sudo /usr/local/libexec/sbgh-check-sandbox-network
```

The SBGH unit applies and owns only its dedicated nftables table. It does not
start the distribution-wide `nftables.service`; doing so can flush nft-backed
Docker, libvirt, or host-firewall rules on hosts whose `/etc/nftables.conf`
begins with `flush ruleset`.

Add public orchestrator, VPN, metadata-service, and other infrastructure CIDRs
that guests must not reach to `protected-ipv4.conf`.

Build the common Ubuntu image and qualify actual packet behavior:

```bash
sudo ./scripts/build-golden-image.sh \
  /var/lib/libvirt/images/sbgh-golden-ubuntu24.qcow2
sudo ./scripts/qualify-sandbox-network.sh --execute \
  /var/lib/sbgh-worker/sandbox-egress-qualification.md \
  /var/lib/libvirt/images/sbgh-golden-ubuntu24.qcow2
```

Use repeatable `--deny-tcp IP:PORT` probes for protected public endpoints that
are safely reachable from the host. The ceremony must prove dependency egress
works while host, private, metadata, IPv6, and configured protected
destinations remain inaccessible.

### Worker identity and host authority

Generate a non-exported P-256 identity on each worker. The command creates the
private key with mode `0600`, refuses overwrite, and prints only its public
SPKI:

```bash
sudo -u sbgh-worker sbgh-worker identity generate \
  --private-key /etc/sbgh/worker/identity.key \
  > /tmp/sbgh-worker-public.pem
```

Copy
[config.example.worker-benchmark.toml](../config.example.worker-benchmark.toml),
[config.example.worker-block-validation.toml](../config.example.worker-block-validation.toml),
or [config.example.worker-combined.toml](../config.example.worker-combined.toml)
to `/etc/sbgh/worker/<profile>.toml`. Set the orchestrator URL, CPU placement,
VM resources, LVM identifiers, and paths. Capabilities are inferred from the
present `[benchmark]` and `[block_validation]` sections. Block validation uses
`stacks-inspect`'s built-in mainnet network configuration and resolves the
durable selector against the attached chainstate before sizing shards.

```bash
sudo install -m 0600 -o sbgh-worker -g sbgh-worker \
  config.example.worker-combined.toml /etc/sbgh/worker/combined.toml
sudo -u sbgh-worker $EDITOR /etc/sbgh/worker/combined.toml
```

Grant only the fixed host commands used by the trusted adapter:

```text
sbgh-worker ALL=(root) NOPASSWD: /usr/sbin/lvcreate, /usr/sbin/lvremove, /usr/sbin/lvs
sbgh-worker ALL=(root) NOPASSWD: /usr/sbin/mkfs.ext4, /usr/sbin/losetup
sbgh-worker ALL=(root) NOPASSWD: /usr/bin/mount, /usr/bin/umount, /usr/bin/chown
sbgh-worker ALL=(root) NOPASSWD: /usr/bin/virsh
sbgh-worker ALL=(root) NOPASSWD: /usr/local/libexec/sbgh-check-sandbox-network
```

Install this as `/etc/sudoers.d/sbgh-worker`, mode `0440`, and validate it with
`visudo -cf /etc/sudoers.d/sbgh-worker`. The daemon user receives no libvirt,
LVM, or sudo authority. The adapter runs `rmdir` without privilege after it
owns the target directory, so it is intentionally absent from this allowlist.

`install-worker.sh` installs the worker unit template and its global hardening
drop-in. It never installs control-plane artifacts and never starts an
instance.

Run preflight before starting a profile:

```bash
sudo -u sbgh-worker sbgh-worker \
  --config /etc/sbgh/worker/combined.toml --preflight-only
```

Use the profile installed on that host.

## 9. Start the control plane, enroll workers, and start execution

Start the daemon first. It can start with an empty worker registry:

```bash
sudo systemctl enable --now sbgh-daemon.service
sudo -u sbgh sbgh-cli status
```

Enroll each worker through the admin API. New workers start disabled and
draining; authorizing the public SPKI and enabling the policy makes the
identity eligible to register without restarting the daemon:

```bash
alias sbgh='sudo -u sbgh sbgh-cli'

WORKER_ID=$(sbgh fleet add-worker \
  --display-name "combined-fsn1-01" \
  --capability benchmark \
  --capability build_only \
  --capability block_validation \
  --measurement-profile "hetzner-ax162" | jq -r .worker.worker_id)
sbgh fleet authorize-identity \
  --worker-id "$WORKER_ID" \
  --public-key /tmp/sbgh-worker-public.pem
sbgh fleet enable-worker --worker-id "$WORKER_ID"
sbgh fleet show-worker --worker-id "$WORKER_ID"
```

Use `--capability block_validation` without a measurement profile for a
dedicated validation worker. Repeat `--capability` to authorize one host for
multiple task kinds. A worker's advertisement can narrow this server-owned
policy but cannot expand it.

Start the edge containers and the matching worker profile:

```bash

docker compose -f docker/docker-compose.yml up -d --build
curl --fail http://127.0.0.1:8080/health

sudo systemctl enable --now sbgh-worker@combined.service
sbgh fleet status
sbgh fleet undrain --worker-id "$WORKER_ID"
```

Dedicated hosts may instead use the benchmark-only or block-validation-only
example and matching systemd instance. Confirm that every worker
registers with its expected identity, capability, and discovered CPU/memory.
Adding, rotating, draining, disabling, or revoking a worker is a database/API
operation and never requires editing daemon configuration.

## 10. Enable backups

Install [pg-backup.sh](../scripts/pg-backup.sh) and the checked-in systemd
service/timer:

```bash
sudo install -m 0755 scripts/pg-backup.sh /usr/local/bin/sbgh-pg-backup.sh
sudo install -m 0644 systemd/sbgh-pg-backup.service /etc/systemd/system/
sudo install -m 0644 systemd/sbgh-pg-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sbgh-pg-backup.timer
sudo systemctl start sbgh-pg-backup.service
```

Store copies outside the daemon host and periodically prove restoration into
an isolated PostgreSQL instance. A compressed archive that has not been
restored is not a verified backup.

## 11. Authorize GitHub use

Run the CLI as `sbgh` so it can read the daemon's mode-`0600` admin cookie:

```bash
alias sbgh='sudo -u sbgh sbgh-cli'

sbgh installer allow --login <account>
```

Now install the GitHub App on the allowed account and selected repositories.
After the installation webhook is processed:

```bash
sbgh installation list
sbgh repo allow --owner stacks-network --name stacks-core
sbgh policy target allow --on <owner>/<repo>
sbgh policy source allow --on <owner>/<repo>
sbgh user grant --login <user> --on <owner>/<repo> \
  --role trigger-pr-benchmark
sbgh user grant --login <user> --on <owner>/<repo> \
  --role trigger-block-validation
```

Optional automatic benchmark triggers:

```bash
sbgh policy trigger add --on <owner>/<repo> --branch-push main
sbgh policy trigger add --on <owner>/<repo> \
  --tag-created '^v\d+\.\d+\.\d+$'
```

## 12. Run canaries

On an authorized pull request, comment:

```text
/benchmark
```

For block validation:

```text
/validate
```

Verify the complete path:

```bash
sbgh webhook tail --limit 20
sbgh jobs list
sbgh fleet status
```

The benchmark should update a `stacks-bench` check and configured PR comment.
Block validation should update a separate `stacks-block-validation` check.
Restart the daemon during a non-production canary and confirm the same report
identities and `/api/submissions/{id}/report` snapshot converge from durable
state.

The installation is complete only after both the network qualification and
the task canaries pass on the actual worker hosts.
