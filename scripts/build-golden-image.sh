#!/usr/bin/env bash
# Build the sbgh golden VM image (Ubuntu 24.04 + Rust toolchain + build deps).
#
# Approach: cloud-init driven. We seed a NoCloud ISO with apt + rustup
# install commands, boot the cloud image as a transient libvirt VM with that
# ISO attached, let cloud-init run, then power off. cloud-init lives inside
# the real VM and uses libvirt's networking (the same `default` network the
# daemon will use for benchmark VMs) — so any DNS / networking issues
# we hit here are exactly the ones the daemon would hit, which makes
# this both more reliable and a useful smoke test of host networking.
#
# This replaces an earlier `virt-customize` approach that broke on hosts
# where libguestfs's appliance VM can't get a working network (which is the
# case on Hetzner among others — the host's network restricts outbound DNS
# and the appliance can't reach anything useful).
#
# Output: a qcow2 image at the path passed as $1, ready to be referenced by
# `[vm].golden_image` in the daemon config.
#
# Requires (on the build host): qemu-utils, cloud-image-utils,
# libvirt-daemon-system, virtinst, libguestfs-tools (for virt-sysprep),
# curl, sha256sum.

set -euo pipefail

DEST="${1:-}"
if [[ -z "$DEST" ]]; then
    cat >&2 <<EOF
usage: $0 <output-qcow2-path>
  e.g. $0 /var/lib/libvirt/images/sbgh-golden-ubuntu24.qcow2

Env overrides:
  UBUNTU_RELEASE       codename to pull (default: noble)
  IMAGE_SIZE           resized image disk size (default: 32G)
  RUST_TOOLCHAIN       rustup toolchain (default: stable)
  CACHE_DIR            where to cache the upstream cloud image
                       (default: /var/cache/sbgh-images)
  BUILD_VM_VCPUS       cores for the build VM (default: 4)
  BUILD_VM_MEMORY_MIB  memory for the build VM (default: 4096)
  BUILD_VM_NETWORK     libvirt network to attach (default: default)
  BUILD_TIMEOUT_SECS   max wait for the build VM to power off (default: 1800)
EOF
    exit 2
fi

UBUNTU_RELEASE="${UBUNTU_RELEASE:-noble}"
IMAGE_SIZE="${IMAGE_SIZE:-32G}"
RUST_TOOLCHAIN="${RUST_TOOLCHAIN:-stable}"
CACHE_DIR="${CACHE_DIR:-/var/cache/sbgh-images}"
BUILD_VM_VCPUS="${BUILD_VM_VCPUS:-4}"
BUILD_VM_MEMORY_MIB="${BUILD_VM_MEMORY_MIB:-4096}"
BUILD_VM_NETWORK="${BUILD_VM_NETWORK:-default}"
BUILD_TIMEOUT_SECS="${BUILD_TIMEOUT_SECS:-1800}"

# ─── pre-flight ────────────────────────────────────────────────────────
for cmd in curl sha256sum qemu-img install mktemp awk \
           cloud-localds virsh virt-sysprep; do
    if ! command -v "$cmd" >/dev/null; then
        echo "missing required command: $cmd" >&2
        case "$cmd" in
            cloud-localds)  echo "  → sudo apt install cloud-image-utils" >&2 ;;
            virsh)          echo "  → sudo apt install libvirt-clients libvirt-daemon-system" >&2 ;;
            virt-sysprep)   echo "  → sudo apt install libguestfs-tools" >&2 ;;
            qemu-img)       echo "  → sudo apt install qemu-utils" >&2 ;;
        esac
        exit 1
    fi
done

# Confirm the chosen libvirt network is active.
if ! virsh net-info "$BUILD_VM_NETWORK" >/dev/null 2>&1; then
    echo "libvirt network '$BUILD_VM_NETWORK' does not exist" >&2
    echo "  → run: sudo virsh net-start $BUILD_VM_NETWORK && sudo virsh net-autostart $BUILD_VM_NETWORK" >&2
    exit 1
fi
if ! virsh net-info "$BUILD_VM_NETWORK" 2>/dev/null | awk '/^Active/{print $2}' | grep -q yes; then
    echo "libvirt network '$BUILD_VM_NETWORK' is not active; starting" >&2
    virsh net-start "$BUILD_VM_NETWORK" >&2 || { echo "failed to start network" >&2; exit 1; }
fi

SRC_URL="https://cloud-images.ubuntu.com/${UBUNTU_RELEASE}/current/${UBUNTU_RELEASE}-server-cloudimg-amd64.img"
SHA_URL="https://cloud-images.ubuntu.com/${UBUNTU_RELEASE}/current/SHA256SUMS"
CACHED_IMG="${CACHE_DIR}/${UBUNTU_RELEASE}-server-cloudimg-amd64.img"

mkdir -p "$CACHE_DIR"

# ─── 1. Resolve / verify cached cloud image ────────────────────────────
EXPECTED_SHA=$(curl --fail --silent --show-error --location "$SHA_URL" \
    | awk -v f="${UBUNTU_RELEASE}-server-cloudimg-amd64.img" '$2 == "*" f || $2 == f {print $1; exit}')

if [[ -z "$EXPECTED_SHA" ]]; then
    echo "==> Could not fetch SHA256SUMS; falling back to plain download (no cache verify)"
    if [[ ! -f "$CACHED_IMG" ]]; then
        echo "==> Downloading ${UBUNTU_RELEASE} cloud image -> $CACHED_IMG"
        curl --fail --location --output "$CACHED_IMG" "$SRC_URL"
    else
        echo "==> Using cached image at $CACHED_IMG (sha not verified — sums unreachable)"
    fi
else
    if [[ -f "$CACHED_IMG" ]] && [[ "$(sha256sum "$CACHED_IMG" | awk '{print $1}')" == "$EXPECTED_SHA" ]]; then
        echo "==> Cache hit (sha verified): $CACHED_IMG"
    else
        if [[ -f "$CACHED_IMG" ]]; then
            echo "==> Cached image is stale; re-downloading"
        else
            echo "==> Downloading ${UBUNTU_RELEASE} cloud image -> $CACHED_IMG"
        fi
        curl --fail --location --output "$CACHED_IMG" "$SRC_URL"
        ACTUAL_SHA=$(sha256sum "$CACHED_IMG" | awk '{print $1}')
        if [[ "$ACTUAL_SHA" != "$EXPECTED_SHA" ]]; then
            echo "SHA MISMATCH on downloaded cloud image (expected $EXPECTED_SHA, got $ACTUAL_SHA)" >&2
            rm -f "$CACHED_IMG"
            exit 1
        fi
    fi
fi

# ─── 2. Copy to destination + resize ───────────────────────────────────
echo "==> Copying to $DEST + resizing to ${IMAGE_SIZE}"
install -m 0644 "$CACHED_IMG" "$DEST"
qemu-img resize "$DEST" "$IMAGE_SIZE"

# ─── 3. Build cloud-init seed ISO ──────────────────────────────────────
PACKAGES=(
    qemu-guest-agent
    xfsprogs
    python3
    git
    build-essential
    pkg-config
    libssl-dev
    libclang-dev
    libsqlite3-dev
    cmake
    clang
    zstd
    protobuf-compiler
    # sccache is guest-local on each disposable boot overlay. Persistent
    # cross-attempt reuse is limited to the host-mediated binary cache.
    sccache
)

# Workdir for the seed ISO + console log + domain XML must be readable by
# the libvirt-qemu user under apparmor confinement. /tmp doesn't satisfy
# both constraints:
#   - `mktemp -d` defaults to mode 0700 (only the creating user can
#     traverse), so libvirt-qemu can't enter the dir to read seed.iso.
#   - The default libvirt-qemu apparmor profile on Debian/Ubuntu doesn't
#     whitelist /tmp anyway.
# /var/lib/libvirt/images/ IS in the apparmor profile and libvirt's dynamic
# per-domain rules cover files inside it. Plus chmod 0755 so libvirt-qemu
# can actually traverse our subdirectory.
mkdir -p /var/lib/libvirt/images
WORKDIR=$(mktemp -d -p /var/lib/libvirt/images sbgh-build-XXXXXX)
chmod 0755 "$WORKDIR"
trap 'rm -rf "$WORKDIR"' EXIT

USER_DATA="$WORKDIR/user-data"
META_DATA="$WORKDIR/meta-data"
SEED_ISO="$WORKDIR/seed.iso"
CONSOLE_LOG="$WORKDIR/console.log"

cat > "$META_DATA" <<EOF
instance-id: sbgh-golden-build-$$
local-hostname: sbgh-golden-build
EOF

# user-data: enumerate apt packages, install rustup, enable guest-agent,
# zero out machine-id (so VMs spawned from this image get unique ones), and
# poweroff so virt-install's --wait can detect "done".
{
    echo '#cloud-config'
    echo 'package_update: true'
    echo 'package_upgrade: false'
    echo 'packages:'
    for p in "${PACKAGES[@]}"; do
        echo "  - $p"
    done
    cat <<EOF
runcmd:
  - env RUSTUP_HOME=/opt/rustup CARGO_HOME=/opt/cargo sh -c 'curl --proto =https --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path --default-toolchain ${RUST_TOOLCHAIN} --profile minimal'
  - ln -sf /opt/cargo/bin/cargo  /usr/local/bin/cargo
  - ln -sf /opt/cargo/bin/rustc  /usr/local/bin/rustc
  - ln -sf /opt/cargo/bin/rustup /usr/local/bin/rustup
  - sh -c "echo 'export PATH=/opt/cargo/bin:\$PATH' > /etc/profile.d/rust.sh"
  - systemctl enable qemu-guest-agent
  - truncate -s 0 /etc/machine-id
  - truncate -s 0 /var/lib/dbus/machine-id || true
  # Sentinel so we can verify the script ran to completion from the console log.
  - sh -c 'cargo --version && rustc --version && echo "SBGH-BUILD-OK"'

power_state:
  mode: poweroff
  condition: True
  delay: now
EOF
} > "$USER_DATA"

echo "==> Building cloud-init seed ISO"
cloud-localds "$SEED_ISO" "$USER_DATA" "$META_DATA"

# ─── 4. Boot the build VM ──────────────────────────────────────────────
BUILD_VM="sbgh-golden-build-$$"
DOMAIN_XML="$WORKDIR/domain.xml"

cat > "$DOMAIN_XML" <<EOF
<domain type='kvm'>
  <name>$BUILD_VM</name>
  <memory unit='MiB'>$BUILD_VM_MEMORY_MIB</memory>
  <vcpu>$BUILD_VM_VCPUS</vcpu>
  <os>
    <type arch='x86_64' machine='q35'>hvm</type>
    <boot dev='hd'/>
  </os>
  <features><acpi/><apic/></features>
  <cpu mode='host-passthrough' check='none'/>
  <clock offset='utc'/>
  <on_poweroff>destroy</on_poweroff>
  <on_reboot>destroy</on_reboot>
  <on_crash>destroy</on_crash>
  <devices>
    <emulator>/usr/bin/qemu-system-x86_64</emulator>
    <disk type='file' device='disk'>
      <driver name='qemu' type='qcow2'/>
      <source file='$DEST'/>
      <target dev='vda' bus='virtio'/>
    </disk>
    <disk type='file' device='cdrom'>
      <driver name='qemu' type='raw'/>
      <source file='$SEED_ISO'/>
      <target dev='sda' bus='sata'/>
      <readonly/>
    </disk>
    <interface type='network'>
      <source network='$BUILD_VM_NETWORK'/>
      <model type='virtio'/>
    </interface>
    <serial type='file'>
      <source path='$CONSOLE_LOG' append='off'/>
      <target type='isa-serial' port='0'/>
    </serial>
    <console type='file'>
      <source path='$CONSOLE_LOG' append='off'/>
      <target type='serial' port='0'/>
    </console>
    <channel type='unix'>
      <target type='virtio' name='org.qemu.guest_agent.0'/>
    </channel>
  </devices>
</domain>
EOF

# Make the console log world-writable so qemu (running as libvirt-qemu)
# can append to it.
touch "$CONSOLE_LOG"
chmod 0666 "$CONSOLE_LOG"
# Same for the disk + seed — libvirt-qemu needs access during the boot.
chmod 0644 "$DEST" "$SEED_ISO"

echo "==> Booting build VM '$BUILD_VM' (transient — will be undefined on shutdown)"
virsh create "$DOMAIN_XML" >/dev/null

# Best-effort cleanup if we get killed mid-boot.
trap '
    rm -rf "$WORKDIR"
    virsh list --name 2>/dev/null | grep -qx "'"$BUILD_VM"'" \
        && virsh destroy "'"$BUILD_VM"'" >/dev/null 2>&1
' EXIT INT TERM

# ─── 5. Wait for cloud-init to finish (VM powers itself off) ───────────
echo "==> Build VM running. Tail of cloud-init progress (this takes ~5-15 min):"
echo "    ---"
# Tail the console log in the background while we wait for the VM to exit.
tail -F "$CONSOLE_LOG" 2>/dev/null \
    | sed -u 's/^/    /' &
TAIL_PID=$!

start=$(date +%s)
while virsh list --name 2>/dev/null | grep -qx "$BUILD_VM"; do
    elapsed=$(( $(date +%s) - start ))
    if [[ $elapsed -gt $BUILD_TIMEOUT_SECS ]]; then
        kill "$TAIL_PID" 2>/dev/null || true
        echo "ERROR: build VM did not shut down within ${BUILD_TIMEOUT_SECS}s" >&2
        echo "Console log tail:" >&2
        tail -50 "$CONSOLE_LOG" >&2
        virsh destroy "$BUILD_VM" >/dev/null 2>&1 || true
        exit 1
    fi
    sleep 5
done
sleep 2  # give tail a moment to drain the final lines
kill "$TAIL_PID" 2>/dev/null || true
wait "$TAIL_PID" 2>/dev/null || true
echo "    ---"

# Sentinel check — cloud-init prints SBGH-BUILD-OK after cargo/rustc verify.
if ! grep -q 'SBGH-BUILD-OK' "$CONSOLE_LOG"; then
    echo "ERROR: build VM finished but did not print the success sentinel." >&2
    echo "Last 80 lines of console log:" >&2
    tail -80 "$CONSOLE_LOG" >&2
    exit 1
fi

# ─── 6. Sysprep cleanup ───────────────────────────────────────────────
# Removes machine-id, cloud-init state, ssh host keys, bash history, logs —
# anything that should be unique per booted VM and not baked into the image.
# Runs offline (no network needed), so libguestfs's broken-network issue
# doesn't apply.
echo "==> Running virt-sysprep cleanup"
virt-sysprep -a "$DEST" \
    --operations \
defaults,-ssh-userdir,-customize \
    --quiet

echo
echo "Golden image ready at: $DEST"
echo "Size on disk: $(du -h "$DEST" | cut -f1)"
