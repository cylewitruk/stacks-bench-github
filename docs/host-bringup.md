# Host bringup

Step-by-step for going from a fresh Linux host with libvirt installed to a working `/benchmark` flow on a real PR. Assumes you've already read [architecture.md](./architecture.md).

> This doc brings a **fresh** host up directly on v3 (the current API-fronted-daemon architecture). Upgrading an **existing** deployment instead? Use [v2-to-v3-upgrade.md](./v2-to-v3-upgrade.md) (from a v2 host) — or, if you're still on the legacy `jobs` queue (v1), run [v1-to-v2-upgrade.md](./v1-to-v2-upgrade.md) first, then v2→v3. For the v3 → v4 artifact-store upgrade (opt-in S3), see [v3-to-v4-upgrade.md](./v3-to-v4-upgrade.md).

## 0. Prerequisites

| Component | Version / notes |
| ---- | ---- |
| OS | Debian 12 or Ubuntu 24.04 (other distros work, paths may shift) |
| Kernel | KVM available (`grep -E 'vmx\|svm' /proc/cpuinfo`) |
| libvirt | 9.x or newer, daemon running |
| qemu | 8.x or newer (virtio-fs requires this) |
| LVM2 | with a thin-pool already created (see [architecture.md](./architecture.md#lvm-layout)) |
| Postgres | local Docker is fine (`docker compose -f docker/docker-compose.yml up -d`) |

Sanity check the host can run KVM domains at all:

```bash
sudo systemctl status libvirtd
virsh list --all
virsh net-list --all      # 'default' should be active
```

If `default` is missing or inactive:

```bash
sudo virsh net-start default
sudo virsh net-autostart default
```

Required packages (Debian/Ubuntu):

```bash
sudo apt update
sudo apt install -y \
  qemu-system-x86 qemu-utils libvirt-daemon-system virtinst virtiofsd \
  cloud-image-utils libguestfs-tools \
  git lvm2 util-linux e2fsprogs xfsprogs
```

`virtiofsd` is the userspace daemon libvirt spawns to back the virtio-fs
results share. It ships in its own apt package on Debian/Ubuntu; without
it `virsh start` aborts with "Unable to find a satisfying virtiofsd".

## 1. Build the golden Ubuntu 24 image

We start from the official Ubuntu 24.04 cloud image and bake in the toolchain `stacks-bench` will need at build time.

A helper lives at [scripts/build-golden-image.sh](../scripts/build-golden-image.sh). The first run takes ~5–10 minutes and ~3 GiB of disk.

```bash
sudo ./scripts/build-golden-image.sh /var/lib/libvirt/images/sbgh-golden-ubuntu24.qcow2
```

What it does:

1. Downloads `noble-server-cloudimg-amd64.img` from cloud-images.ubuntu.com.
2. Resizes to 32 GiB.
3. Uses `virt-customize` to:
   - install `qemu-guest-agent`, `xfsprogs`, build toolchain (`git build-essential pkg-config libssl-dev libclang-dev cmake clang zstd`), and **`sccache`** (caches rustc output across jobs via a persistent host-side dir bind-mounted into every VM at `/var/cache/sccache`)
   - install rustup → `/opt/{rustup,cargo}` system-wide, with symlinks in `/usr/local/bin`
   - enable `qemu-guest-agent` to start on boot
   - truncate `/etc/machine-id` and `/var/lib/dbus/machine-id` so each VM gets a unique id on first boot
4. Drops the image at the destination path you passed.

If you bump the package list later, re-run this script (it overwrites the qcow2). No rolling upgrade — each per-job VM is a fresh boot from the same golden image, so the new package set is in effect from the next `/benchmark`.

Verify the image is bootable + has the toolchain:

```bash
sudo virt-customize -a /var/lib/libvirt/images/sbgh-golden-ubuntu24.qcow2 \
  --run-command 'cargo --version && rustc --version'
```

You should see `cargo 1.x` and `rustc 1.x`.

## 2. Configure the host LVM layout

The daemon needs three things from LVM, all of which usually live inside the **existing OS volume group** (typically `vg0` or one named after the hostname — whatever `sudo vgs` shows). You do **not** need a dedicated VG; renaming the OS VG is invasive (GRUB + initramfs + reboot) and not worth it.

| Inside the VG | Purpose | Notes |
| ---- | ---- | ---- |
| `sbgh-meta` | Linear XFS LV mounted at `/var/lib/sbgh` | Per-job artifacts, archived SQLite results, bare git mirror. ~50–150 GiB is plenty. |
| `thinpool` | Thin pool | Holds base chainstate LVs and per-job snapshots. Size = total chainstate baselines you want to keep × ~2–3×. |
| `mainnet-YYYY-MM-DD` | Thin LV, XFS | One per chainstate baseline. The daemon snapshots whichever is lexicographically newest. |

### Playbook

```bash
# Substitute your actual VG name everywhere. `sudo vgs` shows what you have.
VG=vg0

# ─── Metadata LV (persistent: /var/lib/sbgh) ────────────────────────────
sudo lvcreate -L 150G -n sbgh-meta "$VG"
sudo mkfs.xfs -f "/dev/$VG/sbgh-meta"
sudo mkdir -p /var/lib/sbgh

META_UUID=$(sudo blkid -s UUID -o value "/dev/$VG/sbgh-meta")
echo "UUID=$META_UUID  /var/lib/sbgh  xfs  defaults,noatime  0  2" \
  | sudo tee -a /etc/fstab
sudo systemctl daemon-reload
sudo mount /var/lib/sbgh
findmnt /var/lib/sbgh         # confirm

# ─── Thin pool ──────────────────────────────────────────────────────────
# -Zn        : don't zero new blocks (faster; chainstate writes overwrite anyway)
# --chunksize: smaller chunks = better space efficiency for snapshot churn
sudo lvcreate --type thin-pool --name thinpool -L 1500G \
    --chunksize 256K -Zn "$VG"
sudo lvchange --monitor y "$VG/thinpool"

# ─── First base chainstate LV ───────────────────────────────────────────
# Use the helper script — it creates today's dated LV, downloads + verifies
# the latest Hiro mainnet snapshot in parallel via aria2c, extracts it, and
# rotates out any older `mainnet-*` LVs (skipping any with active snapshots).
sudo apt install -y aria2 zstd
sudo ./scripts/download-chainstate.sh
# See `scripts/download-chainstate.sh --help` for --vg / --date / --keep-old /
# --connections / etc. Allow several hours; run inside tmux/screen if remote.
```

Manual equivalent (skip if you ran the script):

```bash
# Create the LV
sudo lvcreate -V 500G --thin --name mainnet-2026-05-23 "$VG/thinpool"
sudo mkfs.xfs "/dev/$VG/mainnet-2026-05-23"

# Populate it (mount, write chainstate, unmount, deactivate so snapshots can
# be created against it).
sudo mount "/dev/$VG/mainnet-2026-05-23" /mnt
# ... rsync / curl / extract ...
sudo umount /mnt
sudo lvchange -an "$VG/mainnet-2026-05-23"
```

### Refreshing the chainstate later

The script is idempotent across days: just rerun `sudo ./scripts/download-chainstate.sh` whenever you want a fresher baseline. It picks today's date for the new LV name, and by default rotates out the previous one (with `--keep-old` to retain history). Active benchmark runs against the old LV are protected — the rotation step skips any LV with active snapshots.

### Thin vs thick snapshots

The daemon creates per-job snapshots of the base chainstate LV. Two modes, controlled by `[lvm].chainstate_snapshot_size_gib` in `config.toml`:

- **Thin (default, leave the field unset)**: `lvcreate --snapshot` runs without `-L`, so the snapshot lives in the thin pool and grows on demand. This is what you want when the base LV is itself thin (the playbook above). Cheap, fast, no upfront allocation.
- **Thick (set the field to a GiB value)**: `lvcreate --snapshot -L NG` allocates a fixed COW exception store outside the pool. Use only if your base chainstate LV is a thick (non-thin) volume — otherwise the result is a thick snapshot of a thin volume, which loses the cheap-snapshot property and (on some lvm2 versions) errors out.

If you're following the playbook above, leave the field unset.

## 3. Service users + sudoers

Two host users, one per service. Keeping them separate is the filesystem
half of the security boundary — without it, a compromised handler
container could read the daemon's GitHub App PEM and impersonate the App.
The other half is that the handler has **no** DB access and **no** App key
at all (it's a thin `/api` client): the daemon is the sole DB client and
holds the only App credential (see §6).

Four distinct uids total. Two of them only exist *inside* containers
(postgres and smee) and need no host user — only the host file
ownership of their bind mounts (postgres) or nothing at all (smee)
matters:

| Identity | uid/gid | Where it lives | Holds |
| ---- | ---- | ---- | ---- |
| postgres (container) | 900/900 | Owns `/var/lib/sbgh/postgres` on the host. | DB on disk |
| `sbgh-handler` (host) | 901/901 | Owns `/etc/sbgh/handler` on the host. The handler container is built with this uid so the bind-mounted config is readable. | webhook HMAC secret only |
| `sbgh` (host) | 902/902 | Runs the daemon on the host (libvirt, LVM, sudoers). Owns `/etc/sbgh/daemon`. | GitHub App private key |
| smee (container only) | 903/903 | No host user, no bind mounts, no secrets. Distinct uid only for defense-in-depth against a future container escape. | — |

All four are in the system uid range (100–999) so `useradd --system`
doesn't warn. Numbers picked in the low 900s to dodge the common Ubuntu
allocations at the top of that range (997 `systemd-timesync`, 998
`systemd-network`, 999 `dnsmasq`).

Confirm the ids are free first:

```bash
getent passwd 900 901 902 903     # all four lines must be empty
getent group  900 901 902 903     # ditto
```

If any are taken, pick alternatives and set the corresponding env vars
in `docker/.env` to match — substitute the same numbers in the `useradd`
commands below — then rebuild the images (`docker compose -f
docker/docker-compose.yml build --no-cache`). The smee uid
(`SBGH_SMEE_UID`/`SBGH_SMEE_GID`) is runtime-only and doesn't need a
rebuild.

```bash
# Handler-container shadow user. Owns the handler-side config dir. No
# --create-home, no shell — it's purely a filesystem identity.
sudo groupadd --system --gid 901 sbgh-handler
sudo useradd  --system --uid 901 --gid 901 \
              --shell /usr/sbin/nologin sbgh-handler

# Daemon service user. Runs the actual binary on the host.
sudo groupadd --system --gid 902 sbgh
sudo useradd  --system --uid 902 --gid 902 \
              --shell /usr/sbin/nologin sbgh
sudo usermod -a -G libvirt sbgh        # virsh access without sudo for read-only ops

# Per-job subdirs on the XFS metadata LV (mounted at /var/lib/sbgh in §2)
# and the tmpfs root on /run. All sbgh-owned — handler never touches
# these.
sudo install -d -m 0755 -o sbgh -g sbgh /var/lib/sbgh/jobs
sudo install -d -m 0755 -o sbgh -g sbgh /var/lib/sbgh/results
sudo install -d -m 0755 -o sbgh -g sbgh /var/lib/sbgh/git
# Persistent sccache cache, bind-mounted into every job VM via virtio-fs.
# sccache self-caps at 20 GiB (SCCACHE_CACHE_SIZE inside the VM), so this
# dir can't run away. Hot cache is what turns a ~35-min cold build into a
# ~5-min warm build for subsequent jobs against similar PRs.
sudo install -d -m 0755 -o sbgh -g sbgh /var/lib/sbgh/sccache
# /run/sbgh below is on a tmpfs the kernel wipes every reboot. This install -d
# only seeds it for the CURRENT boot — the daemon unit's `RuntimeDirectory=sbgh`
# recreates /run/sbgh (owned sbgh:sbgh) on every start, so it's durable across
# reboots once the unit is installed. The manual seed is just for first bring-up
# before the unit exists.
sudo install -d -m 0755 -o sbgh -g sbgh /run/sbgh
sudo install -d -m 0755 -o sbgh -g sbgh /run/sbgh/jobs
```

The in-container postgres uid (900) doesn't need a matching host user —
it's only used as a numeric owner for `/var/lib/sbgh/postgres` (created
in §6).

Install the sudoers fragment. Only the daemon user needs sudo;
`sbgh-handler` runs entirely inside an unprivileged container.

```bash
sudo tee /etc/sudoers.d/sbgh >/dev/null <<'EOF'
# Daemon (per-job VM provisioning).
sbgh ALL=(root) NOPASSWD: /usr/sbin/lvcreate, /usr/sbin/lvremove, /usr/sbin/lvs
sbgh ALL=(root) NOPASSWD: /usr/sbin/mkfs.ext4, /usr/sbin/losetup
sbgh ALL=(root) NOPASSWD: /usr/bin/mount, /usr/bin/umount, /usr/bin/chown
sbgh ALL=(root) NOPASSWD: /usr/bin/rmdir
sbgh ALL=(root) NOPASSWD: /usr/bin/virsh
# Chainstate refresh (download-chainstate.sh, runs as the systemd timer
# `sbgh-chainstate-refresh.service`). The XFS format + tar/zstd extract
# both need root, and the LV (de)activation flips an attribute that
# requires lvm2 root too.
sbgh ALL=(root) NOPASSWD: /usr/sbin/mkfs.xfs, /usr/sbin/lvchange
sbgh ALL=(root) NOPASSWD: /usr/bin/aria2c, /usr/bin/mkdir
sbgh ALL=(root) NOPASSWD: /usr/bin/zstd, /usr/bin/tar
EOF
sudo chmod 0440 /etc/sudoers.d/sbgh
sudo visudo -cf /etc/sudoers.d/sbgh     # syntax check
```

Verify as `sbgh`:

```bash
sudo -u sbgh sudo -n /usr/sbin/lvs --version
sudo -u sbgh sudo -n /usr/bin/virsh --version
```

## 4. Register a dev GitHub App

You don't need a public domain — webhook delivery will be tunneled (next section).

1. Go to **<https://github.com/settings/apps/new>** (or your org's developer settings).
2. Fill in:
   - **GitHub App name**: anything unique, e.g. `sbgh-dev-<your-handle>`
   - **Homepage URL**: anything (e.g. your repo URL)
   - **Webhook URL**: paste a fresh smee.io channel URL — visit [smee.io](https://smee.io/) in a browser, it'll redirect you to one, copy that. Or programmatically: `curl -sI https://smee.io/new | awk -F': ' 'tolower($1)=="location"{print $2}' | tr -d '\r\n'`. (We use smee.io because GitHub App webhooks are delivered to a single App-level URL — `gh webhook forward` only works for repo/org webhooks, not App webhooks.)
   - **Webhook secret**: paste a long random string. Generate one with:

     ```bash
     openssl rand -hex 32
     ```

     Save this — it'll go in `/etc/sbgh/handler/secrets.env` as
     `SBGH_WEBHOOK_SECRET` in the next section. The daemon never
     sees it (it doesn't need to verify webhook signatures).
3. **Repository permissions**:

    | Permission | Access |
    | ---- | ---- |
    | Contents | Read-only |
    | Issues | Read & write |
    | Metadata | Read-only |
    | Pull requests | Read & write |

4. **Subscribe to events**:
   - `Issue comment` — `/benchmark` PR commands (required; PR comments arrive on this event).
   - `Push` + `Create` — only if you want auto-baselines on develop pushes / release tags (v2 `branch_push` / `tag_created` triggers). Both are covered by the existing **Contents: Read** permission.
   - `Pull request` — optional; lets v2 pre-materialise PR rows ahead of a `/benchmark`.
   - (`Installation` and `Installation repositories` are delivered to every GitHub App automatically — they're not in this list and need no subscription.)
5. **Where can this GitHub App be installed?**: "Only on this account" (for dev).
6. Click **Create GitHub App**.
7. From the new app's settings page, copy the **Client ID** (the `Iv23li…` value listed near the top, right under "About") — that's `SBGH_GH_CLIENT_ID`. GitHub also displays an "App ID" (a numeric value); we don't use that. Both work today, but Client ID is the recommended modern form and the only one that survives if GitHub ever deprecates the legacy App ID auth path.
8. Scroll down → **Generate a private key**. The browser downloads a `.pem` file. Move it to the daemon's config dir (the handler never sees this file):

    ```bash
    sudo install -d -m 0700 -o sbgh -g sbgh /etc/sbgh/daemon
    sudo install -m 0600 -o sbgh -g sbgh \
      ~/Downloads/sbgh-dev-*.pem /etc/sbgh/daemon/github-app.private-key.pem
    ```

9. In the left sidebar of the app page, click **Install App** → install on your account → choose the fork of `stacks-core` you'll be testing against. Note the **installation ID** in the URL (`/settings/installations/<N>`).

## 5. Lay out the two config directories

Two disjoint dirs, one per service, owned by different users — part of
the security boundary: a compromised handler container can read
`/etc/sbgh/handler` (its own bind mount) but not `/etc/sbgh/daemon`
(owned by a different uid, never mounted into the handler container).

| Path | Owner / mode | Files | Read by |
| ---- | ---- | ---- | ---- |
| `/etc/sbgh/handler/` | `sbgh-handler:sbgh-handler` 0700 | `config.toml`, `secrets.env` | handler container |
| `/etc/sbgh/daemon/` | `sbgh:sbgh` 0700 | `config.toml`, `github-app.private-key.pem` | host daemon |

Both dirs are bind-mounted into their respective containers at the
*same* path on both sides, so file references inside the TOML
(`private_key_path = "/etc/sbgh/daemon/github-app.private-key.pem"`)
resolve identically on host and in container.

### 5a. Handler config

```bash
# Directory (uid 997 from §3).
sudo install -d -m 0700 -o sbgh-handler -g sbgh-handler /etc/sbgh/handler

# config.toml: non-secret settings (bind addr, [api].url).
sudo install -m 0600 -o sbgh-handler -g sbgh-handler \
  config.example.handler.toml /etc/sbgh/handler/config.toml
sudo -u sbgh-handler $EDITOR /etc/sbgh/handler/config.toml
# Set at minimum:
#   [api].url = "http://host.docker.internal:8787"   (the daemon /api)
# The benchmark allowlist is NOT configured here — it's enforced by the
# daemon (DB-backed, via `sbgh-cli`).

# secrets.env: env_file for the handler container. Two secrets, no DB
# password and no App key:
#   - SBGH_WEBHOOK_SECRET   : the webhook HMAC secret.
#   - SBGH_API_INGEST_TOKEN : the shared `ingest`-scope token presented to
#                             the daemon /api. Must MATCH the
#                             daemon's SBGH_API_INGEST_TOKEN (set in
#                             §5b). Generate it once, here, and reuse it.
sudo tee /etc/sbgh/handler/secrets.env >/dev/null <<EOF
SBGH_WEBHOOK_SECRET=<openssl rand -hex 32>
SBGH_API_INGEST_TOKEN=<openssl rand -hex 32>
EOF
sudo chmod 0600 /etc/sbgh/handler/secrets.env
sudo chown sbgh-handler:sbgh-handler /etc/sbgh/handler/secrets.env
```

### 5b. Daemon config

```bash
# Directory (uid 998 from §3). The PEM from step 4.8 already lives here.
sudo install -d -m 0700 -o sbgh -g sbgh /etc/sbgh/daemon

# config.toml: App credentials, LVM/libvirt knobs, etc.
sudo install -m 0600 -o sbgh -g sbgh \
  config.example.daemon.toml /etc/sbgh/daemon/config.toml
sudo -u sbgh $EDITOR /etc/sbgh/daemon/config.toml
# Set at minimum:
#   [server].database_url       = "postgres://sbgh:<POSTGRES_OWNER_PASSWORD>@127.0.0.1:5432/sbgh"
#                                 (owner DSN — the daemon serves the /api admin
#                                  endpoints; use the same value you put in docker/.env in §6)
#   [github].client_id          = "Iv23li..."   (from step 4.7)
#   [github].private_key_path   = "/etc/sbgh/daemon/github-app.private-key.pem"
#   [api].listen                = ["127.0.0.1:8787", "172.17.0.1:8787"]
#                                 (loopback for the CLI; the docker host-gateway IP so the
#                                  handler container can reach /api — see config example)
#   [lvm].vg_name               = "vg0"
#   [lvm].thinpool              = "thinpool"
#   [vm].golden_image           = "/var/lib/libvirt/images/sbgh-golden-ubuntu24.qcow2"

# secrets.env: env-only secrets for the host daemon unit (read via
# the unit's EnvironmentFile). SBGH_API_INGEST_TOKEN must be the SAME value
# you generated for the handler's secrets.env in §5a — it's the shared token
# the daemon uses to authenticate the handler's webhook submissions.
sudo tee /etc/sbgh/daemon/secrets.env >/dev/null <<EOF
SBGH_API_INGEST_TOKEN=<same value as the handler's SBGH_API_INGEST_TOKEN>
EOF
sudo chmod 0600 /etc/sbgh/daemon/secrets.env
sudo chown sbgh:sbgh /etc/sbgh/daemon/secrets.env
```

> **Optional — concurrency.** `[runner].max_concurrent_jobs` (default `1`,
> sequential) caps how many benchmarks run at once. Each job is a full
> build+bench VM (vCPUs, memory, an LVM snapshot, a results tmpfs), so raise it
> only when the host can carry that many simultaneously — size against
> `[vm].build_vcpus`/`build_memory` × N and your VG free space. Leave it at `1`
> until you've measured headroom.

## 6. Run handler + smee + Postgres in Docker

The handler, smee, and Postgres run in containers via
[docker/docker-compose.yml](../docker/docker-compose.yml). The daemon
stays on the host (it needs LVM + libvirt + the golden image).

### One DB role (the owner)

Since roadmap-v3 Phase 6 there is a **single** Postgres role: the owner
`sbgh` (password `POSTGRES_OWNER_PASSWORD`). The daemon is the sole DB
client — it connects as the owner and, at startup, applies any pending
schema migrations before serving. There is **no** migrate one-shot and no
narrow `sbgh_handler` / `sbgh_orch` roles: the handler and CLI are `/api`
clients with no DB access. (A one-time forward migration drops the legacy
roles if a prior deploy created them.)

The `sbgh-cli` binary provides the operator admin + read commands. It is a
**pure `/api` client**: no DB credential, no GitHub access. They read the
daemon's admin cookie
(`/etc/sbgh/daemon/.cookie`, mode 0600, owned by `sbgh`) and target
the loopback `/api` (`http://127.0.0.1:8787`), so run them **as the `sbgh`
user**. The daemon resolves logins/repos server-side.

Use the host-built binary (`just build` → `target/release/sbgh-cli`, or
install it on `PATH`). Override the target with `--api-url` / `--cookie` if
needed.

```bash
# Allowlist a GH account (daemon resolves login → numeric id, upserts
# is_enabled=TRUE).
sudo -u sbgh sbgh-cli installer allow --login some-org
# Soft-disable (row preserved for audit; resolves login → id first).
sudo -u sbgh sbgh-cli installer disable --login some-org
# Dump the allowlist.
sudo -u sbgh sbgh-cli installer list

# Read-only visibility commands the /api enables:
sudo -u sbgh sbgh-cli installation list      # known App installations
sudo -u sbgh sbgh-cli webhook tail --limit 20 # recent inbox rows
sudo -u sbgh sbgh-cli jobs list               # benchmark runs
sudo -u sbgh sbgh-cli status                  # /api health + my cookie's scope
```

### One-time setup

```bash
# Docker if not installed.
sudo apt install -y docker.io docker-compose-v2
sudo usermod -a -G docker $USER     # log out + back in for this to take effect

# Runtime env. Required values (SMEE_CHANNEL + POSTGRES_OWNER_PASSWORD)
# have no sensible defaults; compose will refuse to start without them.
cp docker/.env.example docker/.env
$EDITOR docker/.env

# Generate the owner password (hex — URL-safe inside a Postgres DSN, unlike
# base64's `/`+`). Keep it out of shell history.
echo "POSTGRES_OWNER_PASSWORD=$(openssl rand -hex 32)" >> docker/.env

# The daemon's config.toml needs this same value in its
# [server].database_url (owner DSN). Same value, two places — the host
# daemon and the compose Postgres don't share a file.

# Prepare the host-side Postgres data directory. We bind-mount this into
# the container so the data survives `docker volume prune`. The container
# runs rootless as uid 900 (overriding the upstream image's 999 to dodge
# dnsmasq) and the entrypoint skips its usual chown when not running as
# root — so the host dir MUST already exist owned by uid 900 mode 0700
# before first start.
sudo install -d -m 0700 -o 900 -g 900 /var/lib/sbgh/postgres
```

### Bring up the stack

```bash
docker compose -f docker/docker-compose.yml up -d --build
```

What gets built + run:

| Service | Image | Listens | DB role | Talks to |
| ---- | ---- | ---- | ---- | ---- |
| `sbgh-postgres` | `postgres:18-trixie` (uid `${POSTGRES_UID:-900}`) | 127.0.0.1:5432 | — | — |
| `sbgh-handler` | local `handler` target (uid `${SBGH_UID:-901}`) | 127.0.0.1:8080 | — (no DB) | host daemon `/api` via `host.docker.internal:8787` |
| `sbgh-smee` | local `smee` target (uid `${SBGH_SMEE_UID:-903}`) | — | — | smee.io (SSE in), `handler:8080` (HTTP out) |

All three containers run rootless. The handler + smee in-container uid
must match the host `sbgh-handler` uid so the mode-0600 config bind-
mounted from `/etc/sbgh/handler` is readable. Defaults to 901; override
via `SBGH_UID` / `SBGH_GID` in `docker/.env` if your host uses a
different id (check with `id sbgh-handler`). Rebuild after changing
those:

```bash
docker compose -f docker/docker-compose.yml build --no-cache
```

### Tail + verify

```bash
# All logs
docker compose -f docker/docker-compose.yml logs -f

# Just the handler
docker compose -f docker/docker-compose.yml logs -f handler

# Quick health check
curl -i http://127.0.0.1:8080/health    # → 200 OK
```

The smee container picks up `SMEE_CHANNEL` from `docker/.env` and starts
forwarding to `http://handler:8080/webhook` over the docker network.

Schema migrations are no longer a separate step: the host daemon
applies any pending migrations at startup (it's the sole DB client). New
code that adds a SQL migration takes effect the next time the daemon
restarts (`sudo systemctl restart sbgh-daemon`, or the
`install-daemon.sh` re-run that does it for you).

### Why not the daemon too?

Three reasons it stays on the host:

- It calls `lvcreate`/`lvremove` via sudo — easy to wire from host, awkward from inside a container.
- It calls `virsh` — same, plus the libvirt socket is host-side.
- It needs read access to `/var/lib/libvirt/images/sbgh-golden-ubuntu24.qcow2` and write access under `/var/lib/sbgh/jobs/`, both of which are host paths the libvirt-qemu apparmor profile already knows about.

### Build + run the daemon (host-side)

```bash
# Build the binary.
just build
```

**For a long-running setup**, install as a systemd unit (recommended once
the first manual smoke test has succeeded — see below for the manual path):

```bash
# Installs the binary to /usr/local/bin/sbgh-daemon and the unit
# file to /etc/systemd/system/sbgh-daemon.service, then enables +
# starts the service. Idempotent — re-run after every `just build` to
# pick up new code (it'll restart the service automatically).
sudo ./scripts/install-daemon.sh

# Tail logs:
journalctl -u sbgh-daemon -f

# Status:
systemctl status sbgh-daemon
```

Unit lives at [systemd/sbgh-daemon.service](../systemd/sbgh-daemon.service)
in the repo. To override `RUST_LOG` or other env per host, use
`sudo systemctl edit sbgh-daemon` (creates a drop-in at
`/etc/systemd/system/sbgh-daemon.service.d/override.conf`).

> **Shutdown behavior.** `systemctl stop` (SIGTERM) **aborts** in-flight runs —
> the daemon cancels them, tears their VMs down, marks the jobs failed ("aborted
> by shutdown"), and exits. In a foreground/`tmux` run, **one** `Ctrl-C` (SIGINT)
> **drains** instead — it stops claiming new jobs and lets in-flight runs finish,
> then exits; a **second** `Ctrl-C` escalates to abort. The unit sets
> `KillMode=mixed` + `TimeoutStopSec=120s` so systemd lets the daemon run that
> teardown rather than SIGKILLing its `virsh`/`lvremove` children — raise the
> timeout if you run many concurrent jobs.

**For first-time debugging** before installing the unit (sees errors
immediately, easier to ctrl-C), foreground-run in a `tmux` session:

```bash
sudo -u sbgh \
  RUST_LOG=info,sbgh_daemon=debug,sqlx=warn \
  target/release/sbgh-daemon
```

Either way, successful boot logs:

```text
INFO sbgh_daemon: daemon started
```

It'll sit there polling the queue every 5 seconds. The handler in Docker verifies each webhook's HMAC and forwards it to the daemon's `/api`; the daemon records it to the Postgres inbox, the processor creates `job` rows from the inbox, and the runner picks them up.

## 7. First real run

1. Go to a PR on your fork.
2. Comment exactly `/benchmark` on its own line.
3. Within a few seconds you should see:
   - **sbgh-smee** logs `forwarded delivery status=200`.
   - **handler** logs the inbound POST (visible at `RUST_LOG=debug` via `tower_http::trace`) and a new comment appears on the PR: *"⏳ queued at position **1** (job `<uuid>`)…"*.
   - **daemon** (Linux only): claims the job, starts provisioning, defines + starts the domain. On a macOS dev machine without the daemon running, the row simply stays in `queued` — that's the expected handler-only mode for inbound-side validation.
4. The PR comment updates as phases change: `building → running → collecting → done`.
5. On success the comment becomes a ✅ with the summary JSON; on failure ❌ with the error + console tail.

To watch the VM serial console live during the run, before the job dir is cleaned up:

```bash
sudo tail -F /var/lib/sbgh/jobs/<job-id>/console.log
```

(Path is logged by the daemon when the job starts.)

To list domains while a job is running:

```bash
sudo virsh list --all
sudo virsh dominfo sbgh-<job-id>
```

After the job finishes, the quickest look is the CLI (no DB credential —
it's an `/api` client):

```bash
sudo -u sbgh sbgh-cli jobs list
```

For the full forensics, query the `job` family directly. The run output +
archive dir live in `job_result`; the phase/failure timeline in `job_event`:

```bash
# Latest jobs + their result blob.
psql "$DATABASE_URL" -c "
  SELECT j.id, j.status, j.job_kind, j.git_ref_display,
         r.archive_dir, r.run_json
  FROM job j
  LEFT JOIN job_result r ON r.job_id = j.id
  ORDER BY j.created_at DESC LIMIT 5;
"

# Per-job event timeline — phase transitions and failures land here
# (event_status + remark + detail JSONB).
psql "$DATABASE_URL" -c "
  SELECT job_id, event_kind, event_status, occurred_at, remark
  FROM job_event
  ORDER BY occurred_at DESC LIMIT 20;
"
```

And the archived SQLite output for the latest job:

```bash
ls -lh /var/lib/sbgh/results/
sqlite3 /var/lib/sbgh/results/<job-id>.sqlite '.tables'
```

## 8. Troubleshooting

### Webhook isn't reaching the handler

- Check `sbgh-smee` is still running and shows `connected to smee channel`.
- Re-deliver an old webhook from **App settings → Advanced → Recent Deliveries** to retry without re-typing `/benchmark`.
- `curl -v http://localhost:8080/health` should return 200.

### `sbgh-smee` is connected but only logs `event=ping`

You're subscribed to a smee.io channel that nobody is delivering to. GitHub's "Recent Deliveries" page may show successful 200s — that just means smee.io accepted the POST for *its* channel, not that any client received it. **The smee URL set on the App's Webhook URL field and the one passed to `sbgh-smee --channel` must be byte-identical.** Open both in a browser and compare; the App's channel will show the delivery, the other won't.

### Handler logs the inbound webhook but the bot can't post a comment (HTTP 401)

```text
http.method=POST http.url=https://api.github.com/repos/.../issues/N/comments
http.status_code=401 otel.status_code="ERROR"
failed to post initial PR comment
```

Two possible causes, in order:

1. **Permission isn't granted**: App settings → **Permissions & events → Repository permissions → Issues** must be **"Read & write"**. PR comments go through the issues endpoint, not the pull-request reviews endpoint.
2. **Permission was changed after install and the installation hasn't accepted it**: visit <https://github.com/settings/installations>, find the App, click **Configure** — there'll be a yellow banner *"This GitHub App is requesting new permissions. Review →"*. Accept it.

After fixing either, **restart the handler** (the in-memory installation token cache holds the old token for up to ~1 hour) and hit **Redeliver** on the GitHub delivery page.

### Signature verification fails (401)

- `SBGH_WEBHOOK_SECRET` in `/etc/sbgh/handler/secrets.env` must match exactly what you set in the App. No surrounding quotes, no trailing newline.
- If you copy-pasted, regenerate with `openssl rand -hex 32` and update both sides.

### `loading github app private key` error at daemon startup

(The handler never loads the PEM — if you see this error, it's the daemon on the host.)

- `sudo -u sbgh ls -l /etc/sbgh/daemon/github-app.private-key.pem` — must be readable as user `sbgh` (mode `0600`, owner `sbgh:sbgh`).
- PEM file must start with `-----BEGIN RSA PRIVATE KEY-----` (GitHub gives you PKCS#1; `jsonwebtoken` accepts both PKCS#1 and PKCS#8).

### Handler logs `failed to forward webhook to /api` / returns 502

The handler couldn't reach (or was rejected by) the daemon `/api`. The
handler maps a transport failure or daemon 5xx to **502** (GitHub redelivers)
and propagates a daemon **4xx** as-is. Check, in order:

- Is the host daemon running and is `/api` up? `systemctl status sbgh-daemon`; `curl http://127.0.0.1:8787/api/health`.
- Can the container reach the host? The daemon must bind the docker host-gateway IP — `[api].listen` must include `172.17.0.1:8787` (see §5b). `host.docker.internal:host-gateway` in compose resolves to that.
- A **401** (`webhook rejected by /api`) means the ingest token mismatched: `SBGH_API_INGEST_TOKEN` in the handler's `secrets.env` must equal the daemon's (§5a/§5b). Fix both, then restart the handler **and** the daemon.

### `password authentication failed for user "sbgh"`

The owner role's password in Postgres no longer matches the DSN the
daemon was given. Likely cause: someone changed
`POSTGRES_OWNER_PASSWORD` in `docker/.env` (so Postgres reset the role's
password on the next container init) without updating the daemon's
`[server].database_url`. Fix — make the two match, then restart:

```bash
# update [server].database_url in the daemon config to the new
# password, then:
sudo systemctl restart sbgh-daemon
```

### `installation token mint failed: 404`

- The App is registered but **not installed** on the target repo. Re-check step 4.9.

### `no base chainstate LV found in VG ... matching prefix mainnet-`

- `sudo lvs` — confirm at least one LV exists in your VG (whatever `[lvm].vg_name` is set to) with a name starting with `mainnet-`.
- The daemon uses `lvs --select 'lv_name=~^mainnet-'` (regex). If your LV is in a different VG or named differently, override `chainstate_base_prefix` in the config.

### `lvcreate` fails with "Snapshots of snapshots are not supported"

- You tried to snapshot from an already-snapshotted LV. The base LV must be the original thin volume, not another snapshot. Run `sudo lvs -a` and check the `Origin` column.

### VM boots but cloud-init never runs

- Confirm `cidata.iso` was attached: `sudo virsh dumpxml sbgh-<job-id> | grep -A1 cidata`.
- Inspect the ISO contents: `sudo isoinfo -i /var/lib/sbgh/jobs/<id>/cidata.iso -l`.
- In the VM console (`virsh console sbgh-<id>` while running), look for `cloud-init[...] Datasource detected: NoCloud`. If it falls back to `Datasource detected: None`, the ISO label is wrong — `cloud-localds` defaults to `cidata` which is what we want.

### `cargo build` fails inside the VM

- The console log will tell you which dep is missing. Add it to the `--install` list in [scripts/build-golden-image.sh](../scripts/build-golden-image.sh) and rebuild the golden image.
- Common stacks-core deps that aren't in `build-essential`: `libsqlite3-dev`, `protobuf-compiler`.

### Job dir was deleted before you could look at it

By default the daemon deletes the per-job dir on completion. The console tail and last phase are persisted to Postgres (`job_result.run_json` + the `job_event` timeline) regardless, and the SQLite output goes to `paths.results_archive_dir`. For first-time bringup debugging where you need to step through the artifacts:

```bash
# Stop teardown by setting an environment override
SBGH_DEBUG_KEEP_JOB_DIR=1 target/release/sbgh-daemon
```

(That flag isn't wired up yet — open a TODO. Until then, comment out the `remove_dir_all(job_dir)` line in [crates/sbgh-daemon/src/libvirt/driver.rs](../crates/sbgh-daemon/src/libvirt/driver.rs) during bringup.)

### Domain stuck in `paused` or `crashed`

```bash
sudo virsh dominfo sbgh-<job-id>
sudo virsh dumpxml sbgh-<job-id> | less
```

If the daemon died mid-job, you may have orphaned domains and LVs:

```bash
sudo virsh list --all | grep sbgh-
sudo lvs | grep sbgh-

# clean up an orphan
sudo virsh destroy sbgh-<job-id> 2>/dev/null
sudo virsh undefine sbgh-<job-id>
sudo lvremove --force <vg>/sbgh-<job-id>-chainstate   # <vg> = your [lvm].vg_name
sudo umount /run/sbgh/jobs/<job-id> 2>/dev/null
sudo rm -rf /var/lib/sbgh/jobs/<job-id> /run/sbgh/jobs/<job-id>
```

(A "sweep on startup" step on the daemon that reaps these is on the v2 list.)

## 9. Concurrent benchmarking & CPU pinning (optional)

By default the daemon runs one job at a time (`[runner].max_concurrent_jobs =
1`). Raising it lets benchmarks run in parallel — but on a single-socket host
the concurrent VMs contend for cores, the shared L3, and memory bandwidth, which
can inflate the measured `Execution+Commit` time past the serial noise floor.
**Measure before you trust concurrent numbers**, and pin the VMs to dedicated
cores so at least the scheduler isn't adding jitter.

This setup assumes a dedicated bench host where measurement accuracy matters.

### 9.1 Daemon config

In `config.toml` (`[runner]`):

```toml
[runner]
max_concurrent_jobs = 2
cpu_sets  = ["0-1", "2-3"]   # slot 0 → cores 0,1 ; slot 1 → cores 2,3
host_cpus = "4-5"            # pin the qemu emulator/I-O threads off the bench cores
```

The daemon emits `<vcpu placement='static' cpuset='…'>` + `<cputune>
<emulatorpin cpuset='…'/>` into each job's domain XML. `cpu_sets` must have at
least `max_concurrent_jobs` entries; omit it (or leave empty) to disable pinning.

Match the per-phase vCPU counts to the slot size — set **both**
`[vm].build_vcpus` and `[vm].bench_vcpus` to the cores per slot (`2` for a 2-core
slot). A VM's vCPUs are *confined* to its cpuset, so a larger count just
oversubscribes that slot's own cores: for `build_vcpus` that's harmless (it stays
contained to the slot, and the build isn't measured) but pointless — it can't use
more than the slot's cores. For `bench_vcpus` it matters — keep it ≤ slot cores
so the *measured* phase runs one vCPU per core, not oversubscribed. (The default
`build_vcpus = 4` targets the unpinned single-job case; drop it to the slot size
when pinning.)

### 9.2 Host-side isolation (matters as much as the pinning)

Pinning the VM is only half of it — keep the host kernel off the bench cores too,
or kernel threads + interrupts re-introduce the jitter. These are kernel
cmdline + IRQ-affinity changes (GRUB, editable over SSH — no BIOS needed).

**Disable SMT/hyperthreading** (siblings share execution units → variance):

```bash
# Runtime (non-persistent — reverts on reboot):
echo off | sudo tee /sys/devices/system/cpu/smt/control
lscpu -e=CPU,CORE,SOCKET,ONLINE     # confirm one online CPU per physical core
```

**Persist SMT-off + isolate the bench cores (incl. managed IRQs)** via the
kernel cmdline. Edit `/etc/default/grub` — append to `GRUB_CMDLINE_LINUX`
(applies to every entry; use `GRUB_CMDLINE_LINUX_DEFAULT` if you'd rather leave
a *recovery* boot un-isolated for debugging):

```text
nosmt=force isolcpus=domain,managed_irq,0-3 nohz_full=0-3 rcu_nocbs=0-3
```

then `sudo update-grub && sudo reboot`. The cpu numbers are the *bench* logical
CPUs; with SMT off on a 6-core box those are 0–3, leaving 4–5 for the host. After
this, re-establish your serial baselines — the CPU config changed. What each flag
buys you:

- **`nosmt=force`** — disable SMT *and* forbid re-enabling it at runtime (stricter
  than plain `nosmt`; `/sys/.../smt/control` reads `forceoff`).
- **`isolcpus=domain,managed_irq,0-3`** — `domain` keeps the scheduler off cores
  0–3 (no tasks unless explicitly pinned); **`managed_irq`** keeps *kernel-managed*
  interrupts off them too. This is the important one: NVMe (and multiqueue NIC)
  completion IRQs are managed — one queue per CPU, kernel-assigned, **not**
  movable via `/proc/irq/*/smp_affinity`. `managed_irq` is the only lever that
  steers them away from the isolated cores, and it must be set at boot. (This is
  why `irq-affinity.sh --apply` reported the NVMe IRQs as skipped — they're
  managed.)
- **`nohz_full=0-3`** — full tickless on the bench cores (drop the periodic timer
  interrupt). Needs a `CONFIG_NO_HZ_FULL` kernel (Ubuntu's stock kernel has it)
  and ≥1 housekeeping CPU, which 4,5 are.
- **`rcu_nocbs=0-3`** — offload RCU callbacks off the bench cores.

Between `managed_irq` (NVMe/NIC) and `nohz_full` (timers), the high-rate
interrupt sources are now handled at boot — so explicit IRQ steering is rarely
needed.

**(Optional) Steer any *non-managed* device IRQs off the bench cores.** A few
legacy single-vector device IRQs (not NVMe/NIC multiqueue) aren't covered by
`managed_irq` and can still target a bench core — but they're low-rate, so this
is belt-and-suspenders. The daemon's `<emulatorpin>` also puts the VM's qemu I/O
threads on the host cores. **Skip it for your first A/B; revisit only if results
show jitter that tracks with I/O.** If you do want it:

```bash
# Print the recommended commands (review, then run):
scripts/irq-affinity.sh --host-cpus 4-5
# …or apply directly (root):
sudo scripts/irq-affinity.sh --host-cpus 4-5 --apply
```

It disables `irqbalance` (which would re-spread IRQs) and writes each
*non-managed* device IRQ's allowed CPUs to `/proc/irq/<n>/smp_affinity_list`.
Managed IRQs (handled by `managed_irq` above) are reported as skipped; `/proc`
affinities reset on reboot, so the GRUB line is the durable mechanism.

### 9.3 The honest ceiling

Pinning + isolation remove scheduler jitter and core-sharing, but on a single
socket the concurrent jobs still share the **L3 cache and one memory
controller** — which no pinning can partition. Block replay (MARF + Clarity) is
cache/bandwidth-sensitive, so concurrent runs may still perturb each other.

**Validate it:** establish a serial `Execution+Commit` baseline per commit
(`max_concurrent_jobs = 1`, several runs → mean + CV), then run two *different*
commits concurrently and compare each to its own serial baseline. If the shift
stays within your noise floor, concurrency is safe for change detection; if not,
this host benchmarks accurately only serially, and `max > 1` is for throughput
when accuracy isn't the point.
