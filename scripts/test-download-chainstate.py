#!/usr/bin/env python3
"""Check chainstate downloader options and fail-closed publication ordering."""

from __future__ import annotations

from pathlib import Path
import subprocess
import unittest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/download-chainstate.sh"


class DownloadChainstateTest(unittest.TestCase):
    def test_help_documents_ripcat_and_safe_default_size(self) -> None:
        result = subprocess.run(
            [SCRIPT, "--help"],
            check=True,
            capture_output=True,
            text=True,
        )

        self.assertIn("--connections", result.stdout)
        self.assertIn("--window-mib", result.stdout)
        self.assertIn("--retries", result.stdout)
        self.assertIn("--spool-dir", result.stdout)
        self.assertNotIn("--stream", result.stdout)
        self.assertIn("Default: 1T", result.stdout)
        self.assertIn("Failed ranges retry", result.stdout)
        self.assertIn("partial base LV", result.stdout)

    def test_publication_is_fail_closed(self) -> None:
        source = SCRIPT.read_text(encoding="utf-8")
        execution = source[source.index("# ─── 1. expected SHA") :]
        milestones = (
            "EXPECTED_SHA=$(curl",
            "sudo lvcreate -V \"$BASE_SIZE\"",
            "mkfifo \"$DOWNLOAD_DIR/archive.sha256.fifo\"",
            'ripcat "${ripcat_args[@]}" "$URL"',
            "wait \"$HASH_PID\"",
            'if [[ ${ACTUAL_SHA,,} != "${EXPECTED_SHA,,}" ]]',
            'sudo umount "$MOUNT_BASE"',
            'sudo lvchange --permission r "$VG/$BASE_LV"',
            "BASE_POPULATED=1",
        )
        positions = [execution.index(milestone) for milestone in milestones]

        self.assertEqual(positions, sorted(positions))
        self.assertIn("set -euo pipefail", source)
        self.assertIn("required_commands=(ripcat", source)
        self.assertIn("if [[ $BASE_CREATED -eq 1 && $BASE_POPULATED -eq 0 ]]", source)
        self.assertIn('sudo lvremove -y "$VG/$BASE_LV"', source)

    def test_removed_modes_are_rejected(self) -> None:
        for option in ("--stream", "--scratch-size", "--stream-connections"):
            with self.subTest(option=option):
                result = subprocess.run(
                    [SCRIPT, option],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, 2)
                self.assertIn(f"unknown arg: {option}", result.stderr)

        source = SCRIPT.read_text(encoding="utf-8")
        self.assertNotIn("aria2c", source)
        self.assertNotIn("MOUNT_SCRATCH", source)


if __name__ == "__main__":
    unittest.main()
