#!/usr/bin/env python3
"""Regression tests for the sandbox network policy validator."""

from __future__ import annotations

import importlib.util
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True

ROOT = pathlib.Path(__file__).resolve().parent.parent
VALIDATOR_PATH = ROOT / "scripts/check-sandbox-network-assets.py"
SPEC = importlib.util.spec_from_file_location("sandbox_network_assets", VALIDATOR_PATH)
assert SPEC is not None and SPEC.loader is not None
validator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(validator)


class SandboxNetworkAssetsTest(unittest.TestCase):
    @staticmethod
    def write_live_set(
        path: pathlib.Path, name: str, elements: list[object]
    ) -> None:
        path.write_text(
            json.dumps(
                {
                    "nftables": [
                        {
                            "metainfo": {
                                "version": "1.0.9",
                                "json_schema_version": 1,
                            }
                        },
                        {
                            "set": {
                                "family": "inet",
                                "name": name,
                                "table": "sbgh_sandbox_egress",
                                "type": "ipv4_addr",
                                "flags": ["interval"],
                                "elem": elements,
                            }
                        },
                    ]
                }
            ),
            encoding="utf-8",
        )

    def test_checked_in_assets_are_valid(self) -> None:
        validator.validate_xml(ROOT / "network/sandbox-egress.xml")
        validator.validate_nft(ROOT / "network/sandbox-egress.nft")
        validator.validate_protected(ROOT / "network/protected-ipv4.conf.example")

    def test_renamed_network_is_rejected(self) -> None:
        text = (ROOT / "network/sandbox-egress.xml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "network.xml"
            path.write_text(
                text.replace("<name>sandbox-egress</name>", "<name>default</name>"),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "network name"):
                validator.validate_xml(path)

    def test_missing_private_deny_range_is_rejected(self) -> None:
        text = (ROOT / "network/sandbox-egress.nft").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "policy.nft"
            path.write_text(text.replace("            10.0.0.0/8,\n", ""), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "protected IPv4 set differs"):
                validator.validate_nft(path)

    def test_missing_live_rule_marker_is_rejected(self) -> None:
        text = (ROOT / "network/sandbox-egress.nft").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "policy.nft"
            path.write_text(
                text.replace("sbgh:deny-host-v1", "sbgh:unexpected"),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "rule markers differ"):
                validator.validate_nft(path)

    def test_extra_allow_rule_is_rejected(self) -> None:
        text = (ROOT / "network/sandbox-egress.nft").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "policy.nft"
            path.write_text(
                text.replace(
                    '        iifname "virbr-sbgh" counter drop',
                    '        iifname "virbr-sbgh" accept\n'
                    '        iifname "virbr-sbgh" counter drop',
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "input_guard rules differ"):
                validator.validate_nft(path)

    def test_drop_changed_to_accept_with_same_marker_is_rejected(self) -> None:
        text = (ROOT / "network/sandbox-egress.nft").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "policy.nft"
            path.write_text(
                text.replace(
                    'counter drop comment "sbgh:deny-host-v1"',
                    'counter accept comment "sbgh:deny-host-v1"',
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "input_guard rules differ"):
                validator.validate_nft(path)

    def test_policy_helpers_use_private_tmp_for_scratch_state(self) -> None:
        apply = (ROOT / "scripts/apply-sandbox-network.sh").read_text(encoding="utf-8")
        check = (ROOT / "scripts/check-sandbox-network.sh").read_text(encoding="utf-8")
        unit = (ROOT / "systemd/sbgh-sandbox-egress.service").read_text(
            encoding="utf-8"
        )

        self.assertIn("/tmp/sbgh-sandbox-egress.apply.", apply)
        self.assertIn("/tmp/sbgh-sandbox-egress.check.", check)
        self.assertNotIn("mktemp -d /run/", apply)
        self.assertNotIn("mktemp -d /run/", check)
        self.assertIn("ProtectSystem=strict", unit)
        self.assertIn("PrivateTmp=true", unit)

    def test_policy_unit_does_not_start_the_host_nftables_loader(self) -> None:
        unit = (ROOT / "systemd/sbgh-sandbox-egress.service").read_text(
            encoding="utf-8"
        )

        self.assertIn("Wants=libvirtd.service\n", unit)
        self.assertIn("After=libvirtd.service\n", unit)
        self.assertNotIn("nftables.service", unit)

    def test_live_sets_accept_nft_1_0_9_prefix_and_address_forms(self) -> None:
        base_elements = [
            {"prefix": {"addr": str(network.network_address), "len": network.prefixlen}}
            for network in sorted(validator.BASE_PROTECTED)
        ]
        self.assertIn(
            {"prefix": {"addr": "240.0.0.0", "len": 4}}, base_elements
        )
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            protected_json = root / "protected.json"
            operator_json = root / "operator.json"
            operator_config = root / "operator.conf"
            self.write_live_set(protected_json, "protected_ipv4", base_elements)
            self.write_live_set(
                operator_json, "operator_protected_ipv4", ["144.76.56.188"]
            )
            operator_config.write_text("144.76.56.188/32\n", encoding="utf-8")

            validator.validate_live_sets(
                protected_json, operator_json, operator_config
            )

    def test_live_sets_reject_missing_terminal_protected_range(self) -> None:
        base_elements = [
            {"prefix": {"addr": str(network.network_address), "len": network.prefixlen}}
            for network in sorted(validator.BASE_PROTECTED)
            if str(network) != "240.0.0.0/4"
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            protected_json = root / "protected.json"
            operator_json = root / "operator.json"
            operator_config = root / "operator.conf"
            self.write_live_set(protected_json, "protected_ipv4", base_elements)
            self.write_live_set(operator_json, "operator_protected_ipv4", [])
            operator_config.write_text("", encoding="utf-8")

            with self.assertRaisesRegex(
                ValueError, "missing=\\['240.0.0.0/4'\\]"
            ):
                validator.validate_live_sets(
                    protected_json, operator_json, operator_config
                )

    def test_installer_reports_success_only_after_active_check(self) -> None:
        installer = (ROOT / "scripts/install-sandbox-network.sh").read_text(
            encoding="utf-8"
        )

        active_check = installer.index(
            "systemctl is-active --quiet sbgh-sandbox-egress.service"
        )
        success = installer.index("installed and applied sandbox-egress policy")
        self.assertLess(active_check, success)

    def test_noncanonical_operator_cidr_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "protected.conf"
            path.write_text("203.0.113.4/24\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "invalid CIDR"):
                validator.validate_protected(path)

    def test_qualification_dry_run_accepts_operator_tcp_probe(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = pathlib.Path(directory) / "report.md"
            result = subprocess.run(
                [
                    ROOT / "scripts/qualify-sandbox-network.sh",
                    "--deny-tcp",
                    "203.0.113.10:443",
                    report,
                    ROOT / "config.example.worker-benchmark.toml",
                ],
                check=True,
                capture_output=True,
                text=True,
            )

        self.assertIn("operator_tcp:  203.0.113.10:443", result.stdout)
        self.assertIn("DRY RUN", result.stdout)

    def test_qualification_rejects_hostname_tcp_probe(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = pathlib.Path(directory) / "report.md"
            result = subprocess.run(
                [
                    ROOT / "scripts/qualify-sandbox-network.sh",
                    "--deny-tcp",
                    "orchestrator.example:443",
                    report,
                    ROOT / "config.example.worker-benchmark.toml",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--deny-tcp must be IPv4:PORT", result.stderr)

    def test_qualification_preserves_console_when_success_marker_is_missing(
        self,
    ) -> None:
        script = (ROOT / "scripts/qualify-sandbox-network.sh").read_text(
            encoding="utf-8"
        )

        self.assertIn(
            "grep '^SBGH_NETWORK_QUALIFICATION=' \"$console\" | tail -1 || true",
            script,
        )
        self.assertIn("$report.failed-console.", script)
        self.assertIn('install -m 0644 "$console" "$failure_console"', script)


if __name__ == "__main__":
    unittest.main()
