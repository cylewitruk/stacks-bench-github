#!/usr/bin/env python3
"""Check the combined-host updater's operator interface and safety ordering."""

from __future__ import annotations

from pathlib import Path
import subprocess
import unittest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/update-combined-host.sh"


class CombinedHostUpdaterTest(unittest.TestCase):
    def test_help_documents_safe_scope_and_defaults(self) -> None:
        result = subprocess.run(
            [SCRIPT, "--help"],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertIn("default: combined", result.stdout)
        self.assertIn("exactly one registered", result.stdout)
        self.assertIn("--skip-backup", result.stdout)

    def test_mutating_steps_keep_fail_closed_order(self) -> None:
        source = SCRIPT.read_text(encoding="utf-8")
        milestones = (
            'operator fleet drain --worker-id "$worker_id"',
            "wait_for_quiescence",
            'systemctl stop "$worker_unit"',
            '"$repo_root/scripts/install-daemon.sh"',
            "wait_for_daemon",
            "post_registration=$(worker_command fleet check)",
            'operator fleet undrain --worker-id "$worker_id"',
            'systemctl restart "$worker_unit"',
            'wait_for_worker "$worker_id"',
        )
        positions = [source.rindex(milestone) for milestone in milestones]
        self.assertEqual(positions, sorted(positions))
        self.assertIn("trap recover_on_failure EXIT", source)
        self.assertIn("UPDATE FAILED — leaving worker fail-closed", source)


if __name__ == "__main__":
    unittest.main()
