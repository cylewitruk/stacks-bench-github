#!/usr/bin/env bash
# Active v26 sandbox-egress qualification. Dry-run is the default.
set -euo pipefail
PATH=/usr/sbin:/usr/bin:/sbin:/bin
export PATH

usage() {
    local status=${1:-2}
    cat >&2 <<'EOF'
usage:
  qualify-sandbox-network.sh [--execute] [--deny-tcp IP:PORT]...
      REPORT.md GOLDEN_IMAGE [ALLOW_URL]

The active ceremony boots one disposable VM on sandbox-egress. It proves:
  - HTTPS dependency egress succeeds;
  - a controlled host service is unreachable;
  - controlled RFC1918 and metadata-like endpoints behind the host are
    unreachable;
  - every optional operator endpoint is reachable from the host but
    unreachable from the guest.

ALLOW_URL defaults to https://index.crates.io/config.json.
--deny-tcp may be repeated. Each IPv4 address must belong to an
operator-protected CIDR in /etc/sbgh/network/protected-ipv4.conf.
Domain XML separately enforces libvirt port isolation. Dry-run is the default
and changes no host state.
EOF
    exit "$status"
}

execute=0
deny_tcp=()
positionals=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --execute)
            execute=1
            ;;
        --deny-tcp)
            shift
            [[ $# -gt 0 ]] || usage
            deny_tcp+=("$1")
            ;;
        -h|--help)
            usage 0
            ;;
        --)
            shift
            positionals+=("$@")
            break
            ;;
        -*)
            usage
            ;;
        *)
            positionals+=("$1")
            ;;
    esac
    shift
done
[[ ${#positionals[@]} -ge 2 && ${#positionals[@]} -le 3 ]] || usage

report=${positionals[0]}
golden=${positionals[1]}
allow_url=${positionals[2]:-https://index.crates.io/config.json}

[[ $report == /* ]] || { echo "REPORT.md must be absolute" >&2; exit 2; }
[[ -f $golden ]] || { echo "GOLDEN_IMAGE is not a regular file: $golden" >&2; exit 2; }
[[ ! -e $report ]] || { echo "refusing to overwrite report: $report" >&2; exit 2; }
python3 - "$allow_url" "${deny_tcp[@]}" <<'PY'
import ipaddress
import sys
import urllib.parse

url = urllib.parse.urlparse(sys.argv[1])
if (
    url.scheme != "https"
    or not url.hostname
    or url.username
    or url.password
    or len(sys.argv[1]) > 2048
    or any(ord(character) < 32 or ord(character) == 127 for character in sys.argv[1])
):
    raise SystemExit("ALLOW_URL must be an HTTPS URL without credentials")

for endpoint in sys.argv[2:]:
    try:
        address, raw_port = endpoint.rsplit(":", 1)
        ipaddress.IPv4Address(address)
        port = int(raw_port)
    except ValueError as error:
        raise SystemExit(f"--deny-tcp must be IPv4:PORT: {endpoint}: {error}")
    if not 1 <= port <= 65535:
        raise SystemExit(f"--deny-tcp port is outside 1..65535: {endpoint}")
PY

if [[ $execute -eq 0 ]]; then
    cat <<EOF
DRY RUN — no VM, namespace, listener, route, or firewall command was executed.
report:        $report
golden_image:  $golden
network:       sandbox-egress
allow_url:     $allow_url
deny_host:     http://192.168.254.1:18081/
deny_private:  http://10.255.255.2:18080/
deny_metadata: http://169.254.169.254:18080/
EOF
    if [[ ${#deny_tcp[@]} -eq 0 ]]; then
        echo "operator_tcp:  none"
    else
        for endpoint in "${deny_tcp[@]}"; do
            echo "operator_tcp:  $endpoint"
        done
    fi
    echo
    echo "Re-run with --execute after draining workers and reviewing these values."
    exit 0
fi

[[ $EUID -eq 0 ]] || { echo "--execute must run as root" >&2; exit 2; }
for command in virsh qemu-img cloud-localds ip python3 nft base64 install; do
    command -v "$command" >/dev/null ||
        { echo "required command is unavailable: $command" >&2; exit 2; }
done
/usr/local/libexec/sbgh-check-sandbox-network

python3 - /etc/sbgh/network/protected-ipv4.conf "${deny_tcp[@]}" <<'PY'
import ipaddress
import pathlib
import sys

networks = []
for raw in pathlib.Path(sys.argv[1]).read_text(encoding="utf-8").splitlines():
    value = raw.split("#", 1)[0].strip()
    if value:
        networks.append(ipaddress.IPv4Network(value, strict=True))

for endpoint in sys.argv[2:]:
    address = ipaddress.IPv4Address(endpoint.rsplit(":", 1)[0])
    if not any(address in network for network in networks):
        raise SystemExit(
            f"--deny-tcp address is not operator-protected: {address}"
        )
PY

check_operator_endpoints_from_host() {
    local endpoint
    for endpoint in "${deny_tcp[@]}"; do
        python3 - "$endpoint" <<'PY'
import socket
import sys

address, raw_port = sys.argv[1].rsplit(":", 1)
try:
    with socket.create_connection((address, int(raw_port)), timeout=3):
        pass
except OSError as error:
    raise SystemExit(
        f"operator endpoint is not reachable from the host; "
        f"refusing a vacuous deny probe: {sys.argv[1]}: {error}"
    )
PY
    done
}
check_operator_endpoints_from_host

[[ -z $(ip route show 10.255.255.0/30) ]] ||
    { echo "qualification subnet 10.255.255.0/30 is already routed" >&2; exit 2; }
[[ -z $(ip route show 169.254.169.254/32) ]] ||
    { echo "qualification metadata route already exists" >&2; exit 2; }

run_id="$$"
namespace="sbghnt${run_id}"
host_veth="snh${run_id}"
peer_veth="snn${run_id}"
domain="sbgh-net-qualify-${run_id}"
work=$(mktemp -d /var/lib/libvirt/images/sbgh-network-qualify.XXXXXX)
chmod 0755 "$work"
overlay="$work/boot.qcow2"
seed="$work/seed.iso"
console="$work/console.log"
xml="$work/domain.xml"
host_server_pid=
namespace_server_pid=
created_namespace=0
created_domain=0

cleanup() {
    local status=$?
    set +e
    if [[ $created_domain -eq 1 ]]; then
        virsh destroy "$domain" >/dev/null 2>&1
    fi
    [[ -n $host_server_pid ]] && kill "$host_server_pid" >/dev/null 2>&1
    [[ -n $namespace_server_pid ]] && kill "$namespace_server_pid" >/dev/null 2>&1
    ip route del 169.254.169.254/32 via 10.255.255.2 dev "$host_veth" >/dev/null 2>&1
    if [[ $created_namespace -eq 1 ]]; then
        ip netns del "$namespace" >/dev/null 2>&1
        ip link del "$host_veth" >/dev/null 2>&1
    fi
    rm -rf "$work"
    exit "$status"
}
trap cleanup EXIT INT TERM

ip netns add "$namespace"
created_namespace=1
ip link add "$host_veth" type veth peer name "$peer_veth"
ip link set "$peer_veth" netns "$namespace"
ip addr add 10.255.255.1/30 dev "$host_veth"
ip link set "$host_veth" up
ip -n "$namespace" addr add 10.255.255.2/30 dev "$peer_veth"
ip -n "$namespace" addr add 169.254.169.254/32 dev lo
ip -n "$namespace" link set lo up
ip -n "$namespace" link set "$peer_veth" up
ip -n "$namespace" route add default via 10.255.255.1
ip route add 169.254.169.254/32 via 10.255.255.2 dev "$host_veth"

ip netns exec "$namespace" python3 -m http.server 18080 --bind 0.0.0.0 \
    --directory "$work" \
    >"$work/namespace-server.log" 2>&1 &
namespace_server_pid=$!
python3 -m http.server 18081 --bind 192.168.254.1 --directory "$work" \
    >"$work/host-server.log" 2>&1 &
host_server_pid=$!

for url in \
    http://10.255.255.2:18080/ \
    http://169.254.169.254:18080/ \
    http://192.168.254.1:18081/
do
    for _ in 1 2 3 4 5; do
        if python3 - "$url" <<'PY'
import sys
import urllib.request
urllib.request.urlopen(sys.argv[1], timeout=1).read(1)
PY
        then
            break
        fi
        sleep 1
    done
    python3 - "$url" <<'PY'
import sys
import urllib.request
urllib.request.urlopen(sys.argv[1], timeout=2).read(1)
PY
done

probe="$work/probe.py"
python3 - "$probe" "$allow_url" "${deny_tcp[@]}" <<'PY'
import ipaddress
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
allow = sys.argv[2]
deny_tcp = []
for endpoint in sys.argv[3:]:
    address, raw_port = endpoint.rsplit(":", 1)
    deny_tcp.append((str(ipaddress.IPv4Address(address)), int(raw_port)))
deny = [
    "http://192.168.254.1:18081/",
    "http://10.255.255.2:18080/",
    "http://169.254.169.254:18080/",
]
source = f"""#!/usr/bin/env python3
import json
import socket
import urllib.error
import urllib.request

allow = {allow!r}
deny = {deny!r}
deny_tcp = {deny_tcp!r}
result = {{"allow": allow, "denied": [], "denied_tcp": []}}

def emit(prefix, payload):
    message = prefix + json.dumps(payload, sort_keys=True)
    print(message, flush=True)
    with open("/dev/ttyS0", "w", encoding="utf-8") as console:
        console.write(message + "\\n")

try:
    with urllib.request.urlopen(allow, timeout=30) as response:
        if not 200 <= response.status < 400:
            raise RuntimeError(f"dependency egress returned HTTP {{response.status}}")
        response.read(1)

    for url in deny:
        try:
            urllib.request.urlopen(url, timeout=3).read(1)
        except (OSError, TimeoutError, urllib.error.URLError, socket.timeout):
            result["denied"].append(url)
        else:
            raise RuntimeError(f"protected destination was reachable: {{url}}")

    for address, port in deny_tcp:
        try:
            with socket.create_connection((address, port), timeout=3):
                pass
        except (OSError, TimeoutError, socket.timeout):
            result["denied_tcp"].append(f"{{address}}:{{port}}")
        else:
            raise RuntimeError(
                f"operator-protected endpoint was reachable: {{address}}:{{port}}"
            )
except Exception as error:
    emit(
        "SBGH_NETWORK_QUALIFICATION_FAILURE=",
        {{"error_type": type(error).__name__, "message": str(error)}},
    )
    raise
else:
    emit("SBGH_NETWORK_QUALIFICATION=", result)
"""
path.write_text(source, encoding="utf-8")
PY
probe_b64=$(base64 <"$probe" | tr -d '\n')

cat >"$work/meta-data" <<EOF
instance-id: $domain
local-hostname: $domain
EOF
cat >"$work/user-data" <<EOF
#cloud-config
write_files:
  - path: /usr/local/bin/sbgh-network-probe
    permissions: '0755'
    encoding: b64
    content: $probe_b64
runcmd:
  - [ sh, -c, '/usr/local/bin/sbgh-network-probe; status=\$?; shutdown -h now; exit \$status' ]
EOF
cloud-localds "$seed" "$work/user-data" "$work/meta-data"
qemu-img create -f qcow2 -F qcow2 -b "$golden" "$overlay" >/dev/null
touch "$console"
chmod 0644 "$overlay" "$seed"
chmod 0666 "$console"

cat >"$xml" <<EOF
<domain type='kvm'>
  <name>$domain</name>
  <memory unit='MiB'>2048</memory>
  <vcpu>2</vcpu>
  <os><type arch='x86_64' machine='q35'>hvm</type></os>
  <features><acpi/><apic/></features>
  <on_poweroff>destroy</on_poweroff>
  <on_reboot>destroy</on_reboot>
  <on_crash>destroy</on_crash>
  <devices>
    <disk type='file' device='disk'>
      <driver name='qemu' type='qcow2'/>
      <source file='$overlay'/>
      <target dev='vda' bus='virtio'/>
    </disk>
    <disk type='file' device='cdrom'>
      <driver name='qemu' type='raw'/>
      <source file='$seed'/>
      <target dev='sda' bus='sata'/>
      <readonly/>
    </disk>
    <interface type='network'>
      <source network='sandbox-egress'/>
      <model type='virtio'/>
      <port isolated='yes'/>
    </interface>
    <serial type='file'>
      <source path='$console' append='on'/>
      <target type='isa-serial' port='0'/>
    </serial>
    <console type='file'>
      <source path='$console' append='on'/>
      <target type='serial' port='0'/>
    </console>
  </devices>
</domain>
EOF

virsh create "$xml" >/dev/null
created_domain=1
deadline=$(( $(date +%s) + 300 ))
while virsh list --name | grep -qx "$domain"; do
    (( $(date +%s) < deadline )) ||
        { echo "qualification VM did not stop within 300 seconds" >&2; exit 1; }
    sleep 2
done

# A transient domain can leave the active list just before libvirt finishes
# releasing its file security metadata. Wait for the domain object to vanish,
# then allow the final asynchronous cleanup to finish before removing files.
release_deadline=$(( $(date +%s) + 30 ))
while virsh dominfo "$domain" >/dev/null 2>&1; do
    (( $(date +%s) < release_deadline )) ||
        { echo "qualification VM resources were not released within 30 seconds" >&2; exit 1; }
    sleep 1
done
created_domain=0
sleep 1

result=$(
    sed -n 's/^.*\(SBGH_NETWORK_QUALIFICATION={.*}\).*$/\1/p' "$console" |
        tail -1
)
[[ -n $result ]] || {
    echo "qualification VM emitted no success record" >&2
    failure=$(
        sed -n 's/^.*\(SBGH_NETWORK_QUALIFICATION_FAILURE={.*}\).*$/\1/p' "$console" |
            tail -1
    )
    [[ -z $failure ]] || echo "$failure" >&2
    failure_console="$report.failed-console.$(date -u +%Y%m%dT%H%M%SZ).log"
    mkdir -p "$(dirname "$failure_console")"
    install -m 0644 "$console" "$failure_console"
    echo "preserved guest console at $failure_console" >&2
    tail -100 "$console" >&2
    exit 1
}
check_operator_endpoints_from_host

mkdir -p "$(dirname "$report")"
{
    echo "# v26 sandbox-egress qualification"
    echo
    echo "- captured_at_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "- host: $(hostname -f 2>/dev/null || hostname)"
    echo "- network: sandbox-egress"
    echo "- allow_url: $allow_url"
    echo "- dependency_egress: passed"
    echo "- host_service_denial: passed"
    echo "- private_forward_denial: passed"
    echo "- metadata_denial: passed"
    echo "- operator_tcp_denials: ${#deny_tcp[@]} passed"
    for endpoint in "${deny_tcp[@]}"; do
        echo "- operator_tcp_endpoint: $endpoint"
    done
    echo "- guest_port_isolation: configured"
    echo "- result: \`${result#SBGH_NETWORK_QUALIFICATION=}\`"
    echo
    echo "## Policy counters"
    echo
    echo '```text'
    nft list table inet sbgh_sandbox_egress
    echo '```'
} >"$report"

echo "wrote $report"
