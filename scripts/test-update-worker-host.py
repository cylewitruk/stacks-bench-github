#!/usr/bin/env python3
"""Check the worker updater's operator interface and fail-closed ordering."""

from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/update-worker-host.sh"


class WorkerHostUpdaterTest(unittest.TestCase):
    def test_help_documents_scope_and_defaults(self) -> None:
        result = subprocess.run(
            [SCRIPT, "--help"],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertIn("default: combined", result.stdout)
        self.assertIn("wire-compatible worker-only", result.stdout)
        self.assertIn("authenticated sbgh-cli operator access", result.stdout)
        self.assertNotIn("backup", result.stdout.lower())

    def test_mutating_steps_keep_fail_closed_order(self) -> None:
        source = SCRIPT.read_text(encoding="utf-8")
        milestones = (
            'operator fleet drain --worker-id "$worker_id"',
            'wait_for_worker_quiescence "$worker_id"',
            'systemctl stop "$worker_unit"',
            '"$repo_root/scripts/install-worker.sh"',
            '"$worker" --config "$worker_config" --preflight-only',
            "post_registration=$(worker_command fleet check)",
            'operator fleet undrain --worker-id "$worker_id"',
            'systemctl restart "$worker_unit"',
            'wait_for_worker "$worker_id"',
        )
        positions = [source.rindex(milestone) for milestone in milestones]
        self.assertEqual(positions, sorted(positions))
        self.assertIn("trap recover_on_failure EXIT", source)
        self.assertIn("UPDATE FAILED — leaving worker fail-closed", source)
        self.assertNotIn("install-daemon.sh", source)


if __name__ == "__main__":
    unittest.main()
