#!/usr/bin/env bash
# Install the orchestrator binary + systemd unit into their canonical
# system paths. Idempotent — safe to re-run after every `just build`.
#
# Usage: from the repo root, after `just build`:
#   sudo ./scripts/install-orchestrator.sh
#
# What it does:
#   1. Copies target/release/sbgh-orchestrator to /usr/local/bin/
#   2. Installs systemd/sbgh-orchestrator.service to /etc/systemd/system/
#   3. systemctl daemon-reload
#   4. systemctl enable + start (only on first install; subsequent runs
#      just restart so the new binary takes effect)

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
BINARY_SRC="$REPO_ROOT/target/release/sbgh-orchestrator"
BINARY_DEST=/usr/local/bin/sbgh-orchestrator
UNIT_SRC="$REPO_ROOT/systemd/sbgh-orchestrator.service"
UNIT_DEST=/etc/systemd/system/sbgh-orchestrator.service

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    echo "Must run as root (use sudo)." >&2
    exit 1
fi

if [[ ! -x "$BINARY_SRC" ]]; then
    echo "Binary not found at $BINARY_SRC — run \`just build\` first." >&2
    exit 1
fi

if [[ ! -f "$UNIT_SRC" ]]; then
    echo "Unit file not found at $UNIT_SRC." >&2
    exit 1
fi

echo "[1/4] Installing binary to $BINARY_DEST..."
install -m 0755 "$BINARY_SRC" "$BINARY_DEST"

echo "[2/4] Installing unit file to $UNIT_DEST..."
install -m 0644 "$UNIT_SRC" "$UNIT_DEST"

echo "[3/4] Reloading systemd..."
systemctl daemon-reload

# First-install vs upgrade: if the unit isn't enabled yet, enable + start.
# Otherwise just restart so the new binary picks up.
if ! systemctl is-enabled --quiet sbgh-orchestrator.service; then
    echo "[4/4] First install — enabling + starting service..."
    systemctl enable --now sbgh-orchestrator.service
else
    echo "[4/4] Restarting service to pick up the new binary..."
    systemctl restart sbgh-orchestrator.service
fi

echo
echo "Done. Tail logs with:  journalctl -u sbgh-orchestrator -f"
echo "Status:                systemctl status sbgh-orchestrator"
