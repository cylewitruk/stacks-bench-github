#!/usr/bin/env python3
"""Regression tests for the sandbox network policy validator."""

from __future__ import annotations

import importlib.util
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


if __name__ == "__main__":
    unittest.main()
