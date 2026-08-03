#!/usr/bin/env bash
# Install the control-plane binary, operator CLI, and daemon systemd unit.
# Idempotent and intentionally independent of execution-worker installation.

set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
# shellcheck source=install-service-common.sh
source "$repo_root/scripts/install-service-common.sh"

do_build=1
start_service=1
for arg in "$@"; do
    case "$arg" in
        --no-build) do_build=0 ;;
        --no-start) start_service=0 ;;
        -h|--help)
            echo "usage: sudo $0 [--no-build] [--no-start]"
            echo "       DESTDIR=/staging/root $0 --no-build --no-start"
            exit 0
            ;;
        *) echo "Unknown argument: $arg" >&2; exit 2 ;;
    esac
done

sbgh_install_init "$repo_root"

daemon_src="$repo_root/target/release/sbgh-daemon"
cli_src="$repo_root/target/release/sbgh-cli"
unit_src="$repo_root/systemd/sbgh-daemon.service"

sbgh_require_file "$unit_src" "Daemon unit"

echo "[1/5] Preparing control-plane release..."
sbgh_build_release "$do_build" sbgh-daemon sbgh-cli
sbgh_require_executable "$daemon_src" "Daemon binary"
sbgh_require_executable "$cli_src" "Operator CLI"

echo "[2/5] Installing control-plane artifacts..."
sbgh_install_file 0755 "$daemon_src" /usr/local/bin/sbgh-daemon
sbgh_install_file 0755 "$cli_src" /usr/local/bin/sbgh-cli

echo "[3/5] Installing daemon unit..."
sbgh_install_file 0644 "$unit_src" /etc/systemd/system/sbgh-daemon.service

echo "[4/5] Reloading systemd..."
sbgh_reload_systemd

if [[ -n "$SBGH_INSTALL_DESTDIR" ]]; then
    echo "[5/5] Staging install complete; no service action was taken."
elif [[ "$start_service" -eq 0 ]]; then
    echo "[5/5] Skipping service enable/restart (--no-start)."
elif ! systemctl is-enabled --quiet sbgh-daemon.service; then
    echo "[5/5] Enabling and starting daemon service..."
    systemctl enable --now sbgh-daemon.service
else
    echo "[5/5] Restarting daemon service..."
    systemctl restart sbgh-daemon.service
fi

echo
echo "Done. Tail logs with: journalctl -u sbgh-daemon -f"
echo "Status:               systemctl status sbgh-daemon"
echo "Operator CLI:         sudo -u sbgh sbgh-cli status"
