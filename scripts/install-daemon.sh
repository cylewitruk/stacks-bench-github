#!/usr/bin/env bash
# Install the daemon binary + systemd unit into their canonical
# system paths. Idempotent — safe to re-run after every change.
#
# Usage: from the repo root:
#   sudo ./scripts/install-daemon.sh           # build + install
#   sudo ./scripts/install-daemon.sh --no-build # use existing binary
#
# What it does:
#   1. Builds target/release/sbgh-daemon as the invoking user
#      (skipped with --no-build)
#   2. Copies it to /usr/local/bin/
#   3. Installs systemd/sbgh-daemon.service to /etc/systemd/system/
#   4. systemctl daemon-reload
#   5. systemctl enable + start (only on first install; subsequent runs
#      just restart so the new binary takes effect)

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
BINARY_SRC="$REPO_ROOT/target/release/sbgh-daemon"
BINARY_DEST=/usr/local/bin/sbgh-daemon
UNIT_SRC="$REPO_ROOT/systemd/sbgh-daemon.service"
UNIT_DEST=/etc/systemd/system/sbgh-daemon.service

DO_BUILD=1
for arg in "$@"; do
    case "$arg" in
        --no-build) DO_BUILD=0 ;;
        *) echo "Unknown argument: $arg" >&2; exit 1 ;;
    esac
done

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    echo "Must run as root (use sudo)." >&2
    exit 1
fi

if [[ ! -f "$UNIT_SRC" ]]; then
    echo "Unit file not found at $UNIT_SRC." >&2
    exit 1
fi

if [[ $DO_BUILD -eq 1 ]]; then
    # Build as the invoking user so target/ stays owned by them — building
    # as root would break subsequent `just build` runs with permission errors.
    BUILD_USER="${SUDO_USER:-}"
    if [[ -z "$BUILD_USER" || "$BUILD_USER" == "root" ]]; then
        echo "Refusing to cargo build as root (would clobber target/ ownership)." >&2
        echo "Either run via sudo from a non-root shell, or pass --no-build." >&2
        exit 1
    fi
    echo "[1/5] Building release binary as $BUILD_USER..."
    sudo -u "$BUILD_USER" -H sh -c \
        "cd '$REPO_ROOT' && cargo build --locked --release -p sbgh-daemon"
else
    echo "[1/5] Skipping build (--no-build)."
fi

if [[ ! -x "$BINARY_SRC" ]]; then
    echo "Binary not found at $BINARY_SRC after build step." >&2
    exit 1
fi

echo "[2/5] Installing binary to $BINARY_DEST..."
install -m 0755 "$BINARY_SRC" "$BINARY_DEST"

echo "[3/5] Installing unit file to $UNIT_DEST..."
install -m 0644 "$UNIT_SRC" "$UNIT_DEST"

echo "[4/5] Reloading systemd..."
systemctl daemon-reload

# First-install vs upgrade: if the unit isn't enabled yet, enable + start.
# Otherwise just restart so the new binary picks up.
if ! systemctl is-enabled --quiet sbgh-daemon.service; then
    echo "[5/5] First install — enabling + starting service..."
    systemctl enable --now sbgh-daemon.service
else
    echo "[5/5] Restarting service to pick up the new binary..."
    systemctl restart sbgh-daemon.service
fi

echo
echo "Done. Tail logs with:  journalctl -u sbgh-daemon -f"
echo "Status:                systemctl status sbgh-daemon"
