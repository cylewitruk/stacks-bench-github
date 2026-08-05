#!/usr/bin/env bash
# Install the fleet worker binary and systemd template. This command never
# selects, enables, starts, or restarts an instance: config validation,
# preflight, enrollment, and explicit operator activation come first.

set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
# shellcheck source=install-service-common.sh
source "$repo_root/scripts/install-service-common.sh"

do_build=1
for arg in "$@"; do
    case "$arg" in
        --no-build) do_build=0 ;;
        -h|--help)
            echo "usage: sudo $0 [--no-build]"
            echo "       DESTDIR=/staging/root $0 --no-build"
            exit 0
            ;;
        *) echo "Unknown argument: $arg" >&2; exit 2 ;;
    esac
done

sbgh_install_init "$repo_root"

binary_src="$repo_root/target/release/sbgh-worker"
unit_src="$repo_root/systemd/sbgh-worker@.service"
hardening_src="$repo_root/systemd/sbgh-worker-hardening.conf"

sbgh_require_file "$unit_src" "Worker unit template"
sbgh_require_file "$hardening_src" "Worker hardening drop-in"

echo "[1/4] Preparing worker release..."
sbgh_build_release "$do_build" sbgh-worker
sbgh_require_executable "$binary_src" "Worker binary"

echo "[2/4] Installing worker-owned artifacts..."
# LVM writes its standard pre-mutation VG backups here. The unit exposes only
# these root-owned directories through ProtectSystem=strict.
sbgh_install_directory 0700 /etc/lvm/archive
sbgh_install_directory 0700 /etc/lvm/backup
sbgh_install_file 0755 "$binary_src" /usr/local/bin/sbgh-worker
sbgh_install_file 0644 "$unit_src" /etc/systemd/system/sbgh-worker@.service
sbgh_install_file 0644 "$hardening_src" \
    /etc/systemd/system/sbgh-worker@.service.d/hardening.conf

echo "[3/4] Reloading systemd..."
sbgh_reload_systemd

echo "[4/4] Worker remains stopped pending config, preflight, and enrollment."
echo
echo "Next: sudo -u sbgh-worker sbgh-worker --config /etc/sbgh/worker/PROFILE.toml --preflight-only"
echo "Then: sudo systemctl enable --now sbgh-worker@PROFILE.service"
