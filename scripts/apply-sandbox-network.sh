#!/usr/bin/env bash
# Apply the root-owned sandbox-egress libvirt and nftables policy.
set -euo pipefail
PATH=/usr/sbin:/usr/bin:/sbin:/bin
export PATH

network_name=sandbox-egress
network_dir=/etc/sbgh/network
xml="$network_dir/sandbox-egress.xml"
nft_policy="$network_dir/sandbox-egress.nft"
protected_ipv4="$network_dir/protected-ipv4.conf"
validator=/usr/local/libexec/sbgh-check-sandbox-network-assets
checker=/usr/local/libexec/sbgh-check-sandbox-network

[[ $EUID -eq 0 ]] || { echo "sandbox network apply must run as root" >&2; exit 2; }
for command in virsh nft python3 sysctl mktemp; do
    command -v "$command" >/dev/null ||
        { echo "required command is unavailable: $command" >&2; exit 2; }
done
for path in "$xml" "$nft_policy" "$protected_ipv4" "$validator" "$checker"; do
    [[ -e $path ]] || { echo "required sandbox network asset is missing: $path" >&2; exit 2; }
done

python3 -I "$validator" --xml "$xml" --nft "$nft_policy" \
    --protected-ipv4 "$protected_ipv4"

work=$(mktemp -d /tmp/sbgh-sandbox-egress.apply.XXXXXX)
cleanup() {
    rm -rf "$work"
}
trap cleanup EXIT

if virsh net-info "$network_name" >/dev/null 2>&1; then
    active=$(virsh net-info "$network_name" | awk '/^Active:/{print $2}')
else
    active=no
fi

if [[ $active == yes ]]; then
    virsh net-dumpxml "$network_name" >"$work/live.xml"
    if ! python3 -I "$validator" --xml "$work/live.xml" --nft "$nft_policy" \
        --protected-ipv4 "$protected_ipv4" >/dev/null
    then
        echo "active $network_name differs from the installed policy" >&2
        echo "drain workers, destroy the network, then re-run this service" >&2
        exit 1
    fi
else
    virsh net-define "$xml" >/dev/null
    virsh net-start "$network_name" >/dev/null
fi
virsh net-autostart "$network_name" >/dev/null

if nft list table inet sbgh_sandbox_egress >/dev/null 2>&1; then
    printf 'delete table inet sbgh_sandbox_egress\n' >"$work/policy.nft"
fi
cat "$nft_policy" >>"$work/policy.nft"
nft -f "$work/policy.nft"

while IFS= read -r raw || [[ -n $raw ]]; do
    cidr=${raw%%#*}
    cidr=${cidr//[[:space:]]/}
    [[ -n $cidr ]] || continue
    nft add element inet sbgh_sandbox_egress operator_protected_ipv4 "{ $cidr }"
done <"$protected_ipv4"

[[ $(sysctl -n net.ipv4.ip_forward) == 1 ]] ||
    { echo "net.ipv4.ip_forward is disabled after starting the NAT network" >&2; exit 1; }

"$checker"
