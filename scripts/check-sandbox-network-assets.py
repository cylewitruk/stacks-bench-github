#!/usr/bin/env python3
"""Validate the versioned sandbox-egress policy assets."""

from __future__ import annotations

import argparse
import ipaddress
import json
import pathlib
import re
import sys
import xml.etree.ElementTree as ET

NETWORK_NAME = "sandbox-egress"
BRIDGE_NAME = "virbr-sbgh"
SUBNET = ipaddress.ip_network("192.168.254.0/24")
GATEWAY = ipaddress.ip_address("192.168.254.1")
DHCP_START = ipaddress.ip_address("192.168.254.10")
DHCP_END = ipaddress.ip_address("192.168.254.250")
POLICY_NAMESPACE = "urn:sbgh:network-policy"
POLICY_VERSION = "1"

BASE_PROTECTED = {
    ipaddress.ip_network(value)
    for value in (
        "0.0.0.0/8",
        "10.0.0.0/8",
        "100.64.0.0/10",
        "127.0.0.0/8",
        "169.254.0.0/16",
        "172.16.0.0/12",
        "192.0.0.0/24",
        "192.168.0.0/16",
        "198.18.0.0/15",
        "224.0.0.0/4",
        "240.0.0.0/4",
    )
}

REQUIRED_RULE_MARKERS = {
    "sbgh:allow-dhcp-dns-v1",
    "sbgh:allow-dns-tcp-v1",
    "sbgh:deny-host-v1",
    "sbgh:deny-all-ipv6-v1",
    "sbgh:deny-protected-ipv4-v1",
    "sbgh:deny-operator-ipv4-v1",
}

EXPECTED_CHAIN_RULES = {
    "input_guard": {
        'iifname "virbr-sbgh" udp dport { 53, 67 } counter accept comment "sbgh:allow-dhcp-dns-v1"',
        'iifname "virbr-sbgh" tcp dport 53 counter accept comment "sbgh:allow-dns-tcp-v1"',
        'iifname "virbr-sbgh" counter drop comment "sbgh:deny-host-v1"',
    },
    "forward_guard": {
        'iifname "virbr-sbgh" meta nfproto ipv6 counter drop comment "sbgh:deny-all-ipv6-v1"',
        'iifname "virbr-sbgh" ip daddr @protected_ipv4 counter drop comment "sbgh:deny-protected-ipv4-v1"',
        'iifname "virbr-sbgh" ip daddr @operator_protected_ipv4 counter drop comment "sbgh:deny-operator-ipv4-v1"',
    },
}


def fail(message: str) -> None:
    raise ValueError(message)


def validate_xml(path: pathlib.Path) -> None:
    root = ET.parse(path).getroot()
    if root.tag != "network":
        fail("network XML root must be <network>")
    if root.findtext("name") != NETWORK_NAME:
        fail(f"network name must be {NETWORK_NAME}")

    forward = root.find("forward")
    if forward is None or forward.get("mode") != "nat":
        fail("sandbox network must use IPv4 NAT")
    bridge = root.find("bridge")
    if bridge is None or bridge.get("name") != BRIDGE_NAME:
        fail(f"sandbox bridge must be {BRIDGE_NAME}")

    ips = root.findall("ip")
    if len(ips) != 1:
        fail("sandbox network must define exactly one IPv4 subnet and no IPv6 subnet")
    ip = ips[0]
    network = ipaddress.ip_network(f"{ip.get('address')}/{ip.get('netmask')}", strict=False)
    if network != SUBNET or ipaddress.ip_address(ip.get("address", "")) != GATEWAY:
        fail(f"sandbox IPv4 network must be {SUBNET} with gateway {GATEWAY}")
    dhcp = ip.find("dhcp/range")
    if dhcp is None:
        fail("sandbox network must define its DHCP range")
    if (
        ipaddress.ip_address(dhcp.get("start", "")) != DHCP_START
        or ipaddress.ip_address(dhcp.get("end", "")) != DHCP_END
    ):
        fail(f"sandbox DHCP range must be {DHCP_START}-{DHCP_END}")

    metadata = root.find(f"metadata/{{{POLICY_NAMESPACE}}}network-policy")
    if metadata is None:
        fail("sandbox network is missing sbgh policy metadata")
    expected = {
        "version": POLICY_VERSION,
        "bridge": BRIDGE_NAME,
        "ipv4-subnet": str(SUBNET),
    }
    for name, value in expected.items():
        if metadata.get(name) != value:
            fail(f"sandbox policy metadata {name} must be {value}")


def validate_nft(path: pathlib.Path) -> None:
    text = path.read_text(encoding="utf-8")
    required_fragments = {
        "table inet sbgh_sandbox_egress",
        'iifname "virbr-sbgh"',
        "type filter hook input priority -20; policy accept;",
        "type filter hook forward priority -20; policy accept;",
        "ip daddr @protected_ipv4",
        "ip daddr @operator_protected_ipv4",
    }
    missing = sorted(fragment for fragment in required_fragments if fragment not in text)
    if missing:
        fail(f"nftables policy is missing: {', '.join(missing)}")

    markers = set(re.findall(r'comment "(sbgh:[^"]+)"', text))
    if markers != REQUIRED_RULE_MARKERS:
        fail(
            "nftables rule markers differ: "
            f"missing={sorted(REQUIRED_RULE_MARKERS - markers)} "
            f"extra={sorted(markers - REQUIRED_RULE_MARKERS)}"
        )

    lines = text.splitlines()
    for chain, expected_rules in EXPECTED_CHAIN_RULES.items():
        start = next(
            (
                index
                for index, line in enumerate(lines)
                if line.strip() == f"chain {chain} {{"
            ),
            None,
        )
        if start is None:
            fail(f"nftables policy has no {chain} chain")
        rules: list[str] = []
        depth = 1
        for line in lines[start + 1 :]:
            depth += line.count("{") - line.count("}")
            stripped = line.strip()
            if depth == 0:
                break
            if stripped and not stripped.startswith("type filter hook"):
                rules.append(stripped)
        actual_rules = set(rules)
        if actual_rules != expected_rules or len(rules) != len(expected_rules):
            fail(
                f"nftables {chain} rules differ: "
                f"missing={sorted(expected_rules - actual_rules)} "
                f"extra={sorted(actual_rules - expected_rules)}"
            )

    match = re.search(
        r"set protected_ipv4\s*\{.*?elements\s*=\s*\{(?P<elements>.*?)\}",
        text,
        re.DOTALL,
    )
    if match is None:
        fail("nftables policy has no protected_ipv4 elements")
    actual = {
        ipaddress.ip_network(value.strip())
        for value in match.group("elements").split(",")
        if value.strip()
    }
    if actual != BASE_PROTECTED:
        fail(
            "nftables protected IPv4 set differs: "
            f"missing={sorted(map(str, BASE_PROTECTED - actual))} "
            f"extra={sorted(map(str, actual - BASE_PROTECTED))}"
        )


def read_protected(path: pathlib.Path) -> set[ipaddress.IPv4Network]:
    networks: set[ipaddress.IPv4Network] = set()
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        value = raw.split("#", 1)[0].strip()
        if not value:
            continue
        try:
            network = ipaddress.ip_network(value, strict=True)
        except ValueError as error:
            fail(f"{path}:{line_number}: invalid CIDR: {error}")
        if network.version != 4:
            fail(f"{path}:{line_number}: only IPv4 CIDRs are supported")
        networks.add(network)
    return networks


def validate_protected(path: pathlib.Path) -> None:
    read_protected(path)


def read_live_ipv4_set(
    path: pathlib.Path, expected_name: str
) -> set[ipaddress.IPv4Network]:
    document = json.loads(path.read_text(encoding="utf-8"))
    objects = document.get("nftables") if isinstance(document, dict) else None
    if not isinstance(objects, list):
        fail(f"{path}: nft JSON has no nftables object list")
    sets = [item["set"] for item in objects if isinstance(item, dict) and "set" in item]
    if len(sets) != 1 or not isinstance(sets[0], dict):
        fail(f"{path}: nft JSON must contain exactly one set")
    live_set = sets[0]
    expected_fields = {
        "family": "inet",
        "table": "sbgh_sandbox_egress",
        "name": expected_name,
        "type": "ipv4_addr",
    }
    for field, expected in expected_fields.items():
        if live_set.get(field) != expected:
            fail(f"{path}: live {expected_name} {field} must be {expected}")
    if set(live_set.get("flags", [])) != {"interval"}:
        fail(f"{path}: live {expected_name} must be an interval set")

    elements = live_set.get("elem", [])
    if not isinstance(elements, list):
        fail(f"{path}: live {expected_name} elements must be a list")
    networks: set[ipaddress.IPv4Network] = set()
    for element in elements:
        if isinstance(element, str):
            value = element
        elif isinstance(element, dict) and set(element) == {"prefix"}:
            prefix = element["prefix"]
            if not isinstance(prefix, dict) or set(prefix) != {"addr", "len"}:
                fail(f"{path}: malformed prefix in live {expected_name}")
            value = f"{prefix['addr']}/{prefix['len']}"
        else:
            fail(f"{path}: unsupported element in live {expected_name}: {element!r}")
        try:
            network = ipaddress.ip_network(value, strict=True)
        except ValueError as error:
            fail(f"{path}: invalid element in live {expected_name}: {error}")
        if network.version != 4:
            fail(f"{path}: live {expected_name} contains a non-IPv4 element")
        networks.add(network)
    if len(networks) != len(elements):
        fail(f"{path}: live {expected_name} contains duplicate elements")
    return networks


def validate_live_sets(
    protected_json: pathlib.Path,
    operator_json: pathlib.Path,
    operator_config: pathlib.Path,
) -> None:
    actual_base = read_live_ipv4_set(protected_json, "protected_ipv4")
    if actual_base != BASE_PROTECTED:
        fail(
            "live protected IPv4 set differs: "
            f"missing={sorted(map(str, BASE_PROTECTED - actual_base))} "
            f"extra={sorted(map(str, actual_base - BASE_PROTECTED))}"
        )
    expected_operator = read_protected(operator_config)
    actual_operator = read_live_ipv4_set(operator_json, "operator_protected_ipv4")
    if actual_operator != expected_operator:
        fail(
            "live operator-protected IPv4 set differs: "
            f"missing={sorted(map(str, expected_operator - actual_operator))} "
            f"extra={sorted(map(str, actual_operator - expected_operator))}"
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--xml", type=pathlib.Path, required=True)
    parser.add_argument("--nft", type=pathlib.Path, required=True)
    parser.add_argument("--protected-ipv4", type=pathlib.Path)
    parser.add_argument("--live-protected-json", type=pathlib.Path)
    parser.add_argument("--live-operator-json", type=pathlib.Path)
    args = parser.parse_args()

    try:
        validate_xml(args.xml)
        validate_nft(args.nft)
        if args.protected_ipv4 is not None:
            validate_protected(args.protected_ipv4)
        live_paths = (args.live_protected_json, args.live_operator_json)
        if any(path is not None for path in live_paths):
            if args.protected_ipv4 is None or any(path is None for path in live_paths):
                fail(
                    "live set validation requires --protected-ipv4, "
                    "--live-protected-json, and --live-operator-json"
                )
            validate_live_sets(
                args.live_protected_json,
                args.live_operator_json,
                args.protected_ipv4,
            )
    except (OSError, ET.ParseError, ValueError) as error:
        print(f"sandbox network policy invalid: {error}", file=sys.stderr)
        return 1
    print("sandbox network policy assets are consistent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
