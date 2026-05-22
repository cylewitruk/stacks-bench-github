# Host bringup

Step-by-step for going from a fresh Linux host with libvirt installed to a working `/benchmark` flow on a real PR. Assumes you've already read [architecture.md](./architecture.md).

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
  qemu-system-x86 qemu-utils libvirt-daemon-system virtinst \
  cloud-image-utils libguestfs-tools \
  git lvm2 util-linux e2fsprogs xfsprogs
```

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
   - install `qemu-guest-agent`, `xfsprogs`, build toolchain (`git build-essential pkg-config libssl-dev libclang-dev cmake clang zstd`)
   - install rustup → `/opt/{rustup,cargo}` system-wide, with symlinks in `/usr/local/bin`
   - enable `qemu-guest-agent` to start on boot
   - truncate `/etc/machine-id` and `/var/lib/dbus/machine-id` so each VM gets a unique id on first boot
4. Drops the image at the destination path you passed.

Verify the image is bootable + has the toolchain:

```bash
sudo virt-customize -a /var/lib/libvirt/images/sbgh-golden-ubuntu24.qcow2 \
  --run-command 'cargo --version && rustc --version'
```

You should see `cargo 1.x` and `rustc 1.x`.

## 2. Configure the host LVM thin-pool

If you haven't already, set up the thin-pool and a base chainstate LV:

```bash
# one-time pool setup (use a real disk or LVM PV you can dedicate)
sudo pvcreate /dev/<disk>
sudo vgcreate sbgh-vg /dev/<disk>
sudo lvcreate -L 2T --thinpool thinpool sbgh-vg

# base chainstate LV — name format is `<chainstate_base_prefix><date>`
# refresh this LV out-of-band whenever you want a newer snapshot baseline
sudo lvcreate -V 500G --thin --name mainnet-2026-05-21 sbgh-vg/thinpool
sudo mkfs.xfs /dev/sbgh-vg/mainnet-2026-05-21

# populate it with a chainstate snapshot, e.g. by mounting and untarring
sudo mount /dev/sbgh-vg/mainnet-2026-05-21 /mnt
# ... rsync / curl / etc. ...
sudo umount /mnt

# deactivate so snapshots can be created
sudo lvchange -an sbgh-vg/mainnet-2026-05-21
```

The orchestrator will pick the lexicographically-newest LV matching `chainstate_base_prefix`, so dated suffixes are fine.

## 3. Service user + sudoers

```bash
sudo useradd --system --create-home --shell /usr/sbin/nologin sbgh
sudo usermod -a -G libvirt sbgh        # virsh access without sudo for read-only ops
sudo install -d -m 0755 -o sbgh -g sbgh /var/lib/sbgh
sudo install -d -m 0755 -o sbgh -g sbgh /var/lib/sbgh/jobs
sudo install -d -m 0755 -o sbgh -g sbgh /var/lib/sbgh/results
sudo install -d -m 0755 -o sbgh -g sbgh /var/lib/sbgh/git
sudo install -d -m 0755 -o sbgh -g sbgh /run/sbgh
```

Install the sudoers fragment:

```bash
sudo tee /etc/sudoers.d/sbgh >/dev/null <<'EOF'
sbgh ALL=(root) NOPASSWD: /usr/sbin/lvcreate, /usr/sbin/lvremove, /usr/sbin/lvs
sbgh ALL=(root) NOPASSWD: /usr/sbin/mkfs.ext4, /usr/sbin/losetup
sbgh ALL=(root) NOPASSWD: /usr/bin/mount, /usr/bin/umount, /usr/bin/chown
sbgh ALL=(root) NOPASSWD: /usr/bin/virsh
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

1. Go to **https://github.com/settings/apps/new** (or your org's developer settings).
2. Fill in:
   - **GitHub App name**: anything unique, e.g. `sbgh-dev-<your-handle>`
   - **Homepage URL**: anything (e.g. your repo URL)
   - **Webhook URL**: paste your smee.io URL here (from `smee.io` — just visit the page and it gives you one) **or** any placeholder if using `gh webhook forward`, then update later.
   - **Webhook secret**: paste a long random string. Generate one with:

     ```bash
     openssl rand -hex 32
     ```

     Save this — it's `SBGH_GH_WEBHOOK_SECRET`.
3. **Repository permissions**:

    | Permission | Access |
    | ---- | ---- |
    | Contents | Read-only |
    | Issues | Read & write |
    | Metadata | Read-only |
    | Pull requests | Read & write |

4. **Subscribe to events**: `Issue comment` (that's the only one we need; PR comments come through this event).
5. **Where can this GitHub App be installed?**: "Only on this account" (for dev).
6. Click **Create GitHub App**.
7. From the new app's settings page, copy the **App ID** — that's `SBGH_GH_APP_ID`.
8. Scroll down → **Generate a private key**. The browser downloads a `.pem` file. Move it somewhere safe and lock it down:

    ```bash
    sudo install -m 0600 -o sbgh -g sbgh \
      ~/Downloads/sbgh-dev-*.pem /etc/sbgh/github-app.private-key.pem
    ```

9. In the left sidebar of the app page, click **Install App** → install on your account → choose the fork of `stacks-core` you'll be testing against. Note the **installation ID** in the URL (`/settings/installations/<N>`).

## 5. Tunnel webhook deliveries to localhost

Pick one. `gh webhook forward` is the lowest-friction option if you already have `gh` installed.

### Option A: gh webhook forward

```bash
gh auth status                  # must be logged in
gh webhook forward \
  --repo=<your-handle>/stacks-core \
  --events=issue_comment \
  --url=http://localhost:8080/webhook
```

Leave this running. It prints each delivery. When the handler is running, you can verify by re-delivering a past webhook from the App settings page (**Advanced → Recent Deliveries → Redeliver**).

### Option B: smee.io

If you set the webhook URL on the App to your smee channel:

```bash
# pick one
npm install --global smee-client
# or
docker run --rm -p 0 --name smee deltaprojects/smee-client \
  --url https://smee.io/<your-channel> \
  --target http://localhost:8080/webhook
```

```bash
smee --url https://smee.io/<your-channel> --target http://localhost:8080/webhook
```

## 6. Configure + run the services

### Postgres

```bash
docker compose -f docker/docker-compose.yml up -d
```

### Environment

```bash
cp .env.example .env
chmod 0600 .env
```

Edit `.env` and set at minimum:

```bash
DATABASE_URL=postgres://sbgh:sbgh@127.0.0.1:5432/sbgh
SBGH_GH_APP_ID=<from step 4.7>
SBGH_GH_PRIVATE_KEY_PATH=/etc/sbgh/github-app.private-key.pem
SBGH_GH_WEBHOOK_SECRET=<from step 4.2>
SBGH_ALLOWED_REPOS=<your-handle>/stacks-core
SBGH_VM_GOLDEN_IMAGE=/var/lib/libvirt/images/sbgh-golden-ubuntu24.qcow2
SBGH_LVM_VG=sbgh-vg
SBGH_LVM_THINPOOL=thinpool
RUST_LOG=info,sbgh_handler=debug,sbgh_orchestrator=debug
```

Optionally drop a `config.toml` for non-secret settings:

```bash
sudo install -m 0644 -o sbgh -g sbgh config.example.toml /etc/sbgh/config.toml
sudo $EDITOR /etc/sbgh/config.toml
```

### Build + run

```bash
cargo build --release --workspace

# handler — apply migrations + listen for webhooks on :8080
SBGH_CONFIG=/etc/sbgh/config.toml \
  target/release/sbgh-handler &

# orchestrator — claim + execute jobs
sudo -u sbgh -E \
  SBGH_CONFIG=/etc/sbgh/config.toml \
  target/release/sbgh-orchestrator
```

(For a long-running setup, write systemd units; for first-run debugging, foreground is easier.)

## 7. First real run

1. Go to a PR on your fork.
2. Comment exactly `/benchmark` on its own line.
3. Within a few seconds you should see:
   - **gh webhook forward** prints the `issue_comment` delivery.
   - **handler** logs: `webhook → parsed command → enqueued`, and a new comment appears on the PR: *"⏳ queued at position #1…"*.
   - **orchestrator** logs: claims the job, starts provisioning, defines + starts the domain.
4. The PR comment updates as phases change: `building → running → collecting → done`.
5. On success the comment becomes a ✅ with the summary JSON; on failure ❌ with the error + console tail.

To watch the VM serial console live during the run, before the job dir is cleaned up:

```bash
sudo tail -F /var/lib/sbgh/jobs/<job-id>/console.log
```

(Path is logged by the orchestrator when the job starts.)

To list domains while a job is running:

```bash
sudo virsh list --all
sudo virsh dominfo sbgh-<job-id>
```

After the job finishes, query Postgres for the full forensics blob:

```bash
psql "$DATABASE_URL" -c "
  SELECT id, status, error,
         result->'finish_reason'   AS finish_reason,
         result->'last_phase'      AS last_phase,
         result->'sqlite_size_bytes' AS sqlite_size
  FROM jobs
  ORDER BY queued_at DESC LIMIT 5;
"
```

And the archived SQLite output for the latest job:

```bash
ls -lh /var/lib/sbgh/results/
sqlite3 /var/lib/sbgh/results/<job-id>.sqlite '.tables'
```

## 8. Troubleshooting

### Webhook isn't reaching the handler

- Check `gh webhook forward` is still running.
- Re-deliver an old webhook from **App settings → Advanced → Recent Deliveries** to retry without re-typing `/benchmark`.
- `curl -v http://localhost:8080/health` should return 200.

### Signature verification fails (401)

- The `SBGH_GH_WEBHOOK_SECRET` env value must match exactly what you set in the App. No surrounding quotes, no trailing newline.
- If you copy-pasted, regenerate with `openssl rand -hex 32` and update both sides.

### `loading github app private key` error at handler startup

- `ls -l $SBGH_GH_PRIVATE_KEY_PATH` — must be readable by the user running the handler (mode `0600`, owned by that user is ideal).
- PEM file must start with `-----BEGIN RSA PRIVATE KEY-----` (GitHub gives you PKCS#1; `jsonwebtoken` accepts both PKCS#1 and PKCS#8).

### `installation token mint failed: 404`

- The App is registered but **not installed** on the target repo. Re-check step 4.9.

### `no base chainstate LV found in VG ... matching prefix mainnet-`

- `sudo lvs` — confirm at least one LV exists in `sbgh-vg` with a name starting with `mainnet-`.
- The orchestrator uses `lvs --select 'lv_name=~^mainnet-'` (regex). If your LV is in a different VG or named differently, override `chainstate_base_prefix` in the config.

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

By default the orchestrator deletes the per-job dir on completion. The console tail and last phase are persisted to Postgres (`jobs.result`) regardless, and the SQLite output goes to `paths.results_archive_dir`. For first-time bringup debugging where you need to step through the artifacts:

```bash
# Stop teardown by setting an environment override
SBGH_DEBUG_KEEP_JOB_DIR=1 target/release/sbgh-orchestrator
```

(That flag isn't wired up yet — open a TODO. Until then, comment out the `remove_dir_all(job_dir)` line in [crates/sbgh-orchestrator/src/libvirt/driver.rs](../crates/sbgh-orchestrator/src/libvirt/driver.rs) during bringup.)

### Domain stuck in `paused` or `crashed`

```bash
sudo virsh dominfo sbgh-<job-id>
sudo virsh dumpxml sbgh-<job-id> | less
```

If the orchestrator died mid-job, you may have orphaned domains and LVs:

```bash
sudo virsh list --all | grep sbgh-
sudo lvs | grep sbgh-

# clean up an orphan
sudo virsh destroy sbgh-<job-id> 2>/dev/null
sudo virsh undefine sbgh-<job-id>
sudo lvremove --force sbgh-vg/sbgh-<job-id>-chainstate
sudo umount /run/sbgh/jobs/<job-id> 2>/dev/null
sudo rm -rf /var/lib/sbgh/jobs/<job-id> /run/sbgh/jobs/<job-id>
```

(A "sweep on startup" step on the orchestrator that reaps these is on the v2 list.)
