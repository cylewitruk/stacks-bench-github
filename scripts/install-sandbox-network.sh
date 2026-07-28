#!/usr/bin/env bash
# Install the versioned sandbox-egress policy and its root-owned helpers.
set -euo pipefail
PATH=/usr/sbin:/usr/bin:/sbin:/bin
export PATH

repo_root=$(cd "$(dirname "$0")/.." && pwd)
install_only=0
refresh=0

usage() {
    local status=${1:-2}
    cat >&2 <<'EOF'
usage: install-sandbox-network.sh [--install-only] [--refresh]

  --install-only  Install assets/unit without applying or enabling the policy.
  --refresh       Back up and replace existing /etc/sbgh/network policy assets.

Existing operator-protected CIDRs are always preserved.
EOF
    exit "$status"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --install-only) install_only=1 ;;
        --refresh) refresh=1 ;;
        -h|--help) usage 0 ;;
        *) usage ;;
    esac
    shift
done

[[ $EUID -eq 0 ]] || { echo "sandbox network install must run as root" >&2; exit 2; }

"$repo_root/scripts/check-sandbox-network-assets.py" \
    --xml "$repo_root/network/sandbox-egress.xml" \
    --nft "$repo_root/network/sandbox-egress.nft" \
    --protected-ipv4 "$repo_root/network/protected-ipv4.conf.example"

install -d -m 0755 /usr/local/libexec /usr/local/share/sbgh/network
install -d -m 0755 /etc/sbgh/network
install -m 0755 "$repo_root/scripts/apply-sandbox-network.sh" \
    /usr/local/libexec/sbgh-apply-sandbox-network
install -m 0755 "$repo_root/scripts/check-sandbox-network.sh" \
    /usr/local/libexec/sbgh-check-sandbox-network
install -m 0755 "$repo_root/scripts/check-sandbox-network-assets.py" \
    /usr/local/libexec/sbgh-check-sandbox-network-assets
install -m 0644 "$repo_root/network/sandbox-egress.xml" \
    /usr/local/share/sbgh/network/sandbox-egress.xml
install -m 0644 "$repo_root/network/sandbox-egress.nft" \
    /usr/local/share/sbgh/network/sandbox-egress.nft

install_policy() {
    local name=$1
    local source="/usr/local/share/sbgh/network/$name"
    local target="/etc/sbgh/network/$name"
    if [[ ! -e $target ]]; then
        install -m 0644 "$source" "$target"
    elif [[ $refresh -eq 1 ]]; then
        cp -a "$target" "$target.previous"
        install -m 0644 "$source" "$target"
    fi
}
install_policy sandbox-egress.xml
install_policy sandbox-egress.nft

if [[ ! -e /etc/sbgh/network/protected-ipv4.conf ]]; then
    install -m 0644 "$repo_root/network/protected-ipv4.conf.example" \
        /etc/sbgh/network/protected-ipv4.conf
fi

install -m 0644 "$repo_root/systemd/sbgh-sandbox-egress.service" \
    /etc/systemd/system/sbgh-sandbox-egress.service
systemctl daemon-reload

if [[ $install_only -eq 1 ]]; then
    echo "installed sandbox-egress assets without applying them"
else
    systemctl enable --now sbgh-sandbox-egress.service
    echo "installed and applied sandbox-egress policy"
fi
