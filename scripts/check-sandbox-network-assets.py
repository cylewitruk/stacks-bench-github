#!/usr/bin/env python3
"""Validate the versioned sandbox-egress policy assets."""

from __future__ import annotations

import argparse
import ipaddress
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


def validate_protected(path: pathlib.Path) -> None:
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--xml", type=pathlib.Path, required=True)
    parser.add_argument("--nft", type=pathlib.Path, required=True)
    parser.add_argument("--protected-ipv4", type=pathlib.Path)
    args = parser.parse_args()

    try:
        validate_xml(args.xml)
        validate_nft(args.nft)
        if args.protected_ipv4 is not None:
            validate_protected(args.protected_ipv4)
    except (OSError, ET.ParseError, ValueError) as error:
        print(f"sandbox network policy invalid: {error}", file=sys.stderr)
        return 1
    print("sandbox network policy assets are consistent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
