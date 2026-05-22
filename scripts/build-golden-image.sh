#!/usr/bin/env bash
# Build the sbgh golden VM image (Ubuntu 24.04 + Rust toolchain + build deps).
#
# Output: a qcow2 image at the path passed as $1, ready to be referenced by
# `[vm].golden_image` in the orchestrator config.
#
# Requires (on the build host): qemu-utils, libguestfs-tools, curl.

set -euo pipefail

DEST="${1:-}"
if [[ -z "$DEST" ]]; then
    echo "usage: $0 <output-qcow2-path>" >&2
    echo "  e.g. $0 /var/lib/libvirt/images/sbgh-golden-ubuntu24.qcow2" >&2
    exit 2
fi

UBUNTU_RELEASE="${UBUNTU_RELEASE:-noble}"
IMAGE_SIZE="${IMAGE_SIZE:-32G}"
RUST_TOOLCHAIN="${RUST_TOOLCHAIN:-stable}"

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

SRC_URL="https://cloud-images.ubuntu.com/${UBUNTU_RELEASE}/current/${UBUNTU_RELEASE}-server-cloudimg-amd64.img"
SRC="$WORKDIR/base.img"

echo "==> Downloading ${UBUNTU_RELEASE} cloud image"
curl --fail --location --output "$SRC" "$SRC_URL"

echo "==> Copying to destination + resizing to ${IMAGE_SIZE}"
install -m 0644 "$SRC" "$DEST"
qemu-img resize "$DEST" "$IMAGE_SIZE"

# Build deps for stacks-core. Add to this list if `cargo build -p stacks-bench`
# fails inside a VM with a "missing X.h" error.
PACKAGES=(
    qemu-guest-agent
    xfsprogs
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
)
PKG_LIST="$(IFS=,; echo "${PACKAGES[*]}")"

echo "==> Customizing image (apt update, install toolchain, install rustup)"
virt-customize -a "$DEST" \
    --update \
    --install "$PKG_LIST" \
    --run-command "\
        env RUSTUP_HOME=/opt/rustup CARGO_HOME=/opt/cargo \
            sh -c 'curl --proto =https --tlsv1.2 -sSf https://sh.rustup.rs \
                   | sh -s -- -y --no-modify-path --default-toolchain ${RUST_TOOLCHAIN} --profile minimal'" \
    --run-command "\
        ln -sf /opt/cargo/bin/cargo  /usr/local/bin/cargo && \
        ln -sf /opt/cargo/bin/rustc  /usr/local/bin/rustc && \
        ln -sf /opt/cargo/bin/rustup /usr/local/bin/rustup" \
    --run-command "echo 'export PATH=/opt/cargo/bin:\$PATH' > /etc/profile.d/rust.sh" \
    --run-command "systemctl enable qemu-guest-agent" \
    --truncate /etc/machine-id \
    --truncate /var/lib/dbus/machine-id

echo "==> Verifying toolchain is callable inside the image"
virt-customize -a "$DEST" \
    --run-command "cargo --version && rustc --version"

echo
echo "Golden image ready at: $DEST"
echo "Size on disk: $(du -h "$DEST" | cut -f1)"
