#!/usr/bin/env bash
# Read-only structural verifier used by worker preflight.
set -euo pipefail
PATH=/usr/sbin:/usr/bin:/sbin:/bin
export PATH

network_name=sandbox-egress
network_dir=/etc/sbgh/network
xml="$network_dir/sandbox-egress.xml"
nft_policy="$network_dir/sandbox-egress.nft"
protected_ipv4="$network_dir/protected-ipv4.conf"
validator=/usr/local/libexec/sbgh-check-sandbox-network-assets

[[ $EUID -eq 0 ]] || { echo "sandbox network check must run as root" >&2; exit 2; }
for command in virsh nft python3 sysctl mktemp grep; do
    command -v "$command" >/dev/null ||
        { echo "required command is unavailable: $command" >&2; exit 2; }
done
for path in "$xml" "$nft_policy" "$protected_ipv4" "$validator"; do
    [[ -e $path ]] || { echo "required sandbox network asset is missing: $path" >&2; exit 2; }
done

python3 -I "$validator" --xml "$xml" --nft "$nft_policy" \
    --protected-ipv4 "$protected_ipv4" >/dev/null

work=$(mktemp -d /tmp/sbgh-sandbox-egress.check.XXXXXX)
cleanup() {
    rm -rf "$work"
}
trap cleanup EXIT

info=$(virsh net-info "$network_name")
[[ $(awk '/^Active:/{print $2}' <<<"$info") == yes ]] ||
    { echo "$network_name is inactive" >&2; exit 1; }
[[ $(awk '/^Autostart:/{print $2}' <<<"$info") == yes ]] ||
    { echo "$network_name is not configured for autostart" >&2; exit 1; }

virsh net-dumpxml "$network_name" >"$work/live.xml"
nft -j list set inet sbgh_sandbox_egress protected_ipv4 >"$work/protected.json"
nft -j list set inet sbgh_sandbox_egress operator_protected_ipv4 >"$work/operator.json"
python3 -I "$validator" --xml "$work/live.xml" --nft "$nft_policy" \
    --protected-ipv4 "$protected_ipv4" \
    --live-protected-json "$work/protected.json" \
    --live-operator-json "$work/operator.json" >/dev/null

nft -s list table inet sbgh_sandbox_egress >"$work/live.nft"
[[ $(grep -Ec '^[[:space:]]*chain (input_guard|forward_guard) \{' "$work/live.nft") -eq 2 ]] ||
    { echo "live nftables policy must contain exactly the two managed chains" >&2; exit 1; }
[[ $(grep -Ec '^[[:space:]]*set (protected_ipv4|operator_protected_ipv4) \{' "$work/live.nft") -eq 2 ]] ||
    { echo "live nftables policy must contain exactly the two managed sets" >&2; exit 1; }
[[ $(grep -Ec 'comment "sbgh:' "$work/live.nft") -eq 6 ]] ||
    { echo "live nftables policy must contain exactly six managed rules" >&2; exit 1; }
for marker in \
    sbgh:allow-dhcp-dns-v1 \
    sbgh:allow-dns-tcp-v1 \
    sbgh:deny-host-v1 \
    sbgh:deny-all-ipv6-v1 \
    sbgh:deny-protected-ipv4-v1 \
    sbgh:deny-operator-ipv4-v1
do
    grep -Fq "comment \"$marker\"" "$work/live.nft" ||
        { echo "live nftables policy is missing $marker" >&2; exit 1; }
done
for rule in \
    'iifname "virbr-sbgh" udp dport { 53, 67 } counter accept comment "sbgh:allow-dhcp-dns-v1"' \
    'iifname "virbr-sbgh" tcp dport 53 counter accept comment "sbgh:allow-dns-tcp-v1"' \
    'iifname "virbr-sbgh" counter drop comment "sbgh:deny-host-v1"' \
    'iifname "virbr-sbgh" meta nfproto ipv6 counter drop comment "sbgh:deny-all-ipv6-v1"' \
    'iifname "virbr-sbgh" ip daddr @protected_ipv4 counter drop comment "sbgh:deny-protected-ipv4-v1"' \
    'iifname "virbr-sbgh" ip daddr @operator_protected_ipv4 counter drop comment "sbgh:deny-operator-ipv4-v1"'
do
    grep -Fq "$rule" "$work/live.nft" ||
        { echo "live nftables rule differs from the managed policy: $rule" >&2; exit 1; }
done

[[ $(sysctl -n net.ipv4.ip_forward) == 1 ]] ||
    { echo "net.ipv4.ip_forward is disabled" >&2; exit 1; }

echo "sandbox-egress structural policy check passed"
