#!/usr/bin/env python3
"""Exercise installer ownership and idempotency against a staging root."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent


def digest_tree(root: Path) -> dict[str, tuple[int, str]]:
    result: dict[str, tuple[int, str]] = {}
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        result[str(path.relative_to(root))] = (
            stat.S_IMODE(path.stat().st_mode),
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
    return result


class InstallerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "repo"
        for directory in ("scripts", "systemd", "target/release"):
            (self.root / directory).mkdir(parents=True, exist_ok=True)

        for name in (
            "install-service-common.sh",
            "install-daemon.sh",
            "install-worker.sh",
        ):
            shutil.copy2(ROOT / "scripts" / name, self.root / "scripts" / name)
        for name in (
            "sbgh-daemon.service",
            "sbgh-worker@.service",
            "sbgh-worker-hardening.conf",
        ):
            shutil.copy2(ROOT / "systemd" / name, self.root / "systemd" / name)
        for name in ("sbgh-daemon", "sbgh-cli", "sbgh-worker"):
            binary = self.root / "target/release" / name
            binary.write_text(f"#!/bin/sh\necho {name}\n", encoding="utf-8")
            binary.chmod(0o755)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def run_installer(self, name: str, destination: Path, *arguments: str) -> None:
        environment = os.environ.copy()
        environment["DESTDIR"] = str(destination)
        subprocess.run(
            [self.root / "scripts" / name, "--no-build", *arguments],
            check=True,
            capture_output=True,
            text=True,
            env=environment,
        )

    def test_daemon_installer_owns_only_control_plane_artifacts(self) -> None:
        destination = Path(self.temporary.name) / "daemon-root"
        self.run_installer("install-daemon.sh", destination, "--no-start")

        self.assertEqual(
            set(digest_tree(destination)),
            {
                "etc/systemd/system/sbgh-daemon.service",
                "usr/local/bin/sbgh-cli",
                "usr/local/bin/sbgh-daemon",
            },
        )
        installed = digest_tree(destination)
        self.assertEqual(installed["usr/local/bin/sbgh-daemon"][0], 0o755)
        self.assertEqual(installed["usr/local/bin/sbgh-cli"][0], 0o755)
        self.assertEqual(installed["etc/systemd/system/sbgh-daemon.service"][0], 0o644)

        before = installed
        self.run_installer("install-daemon.sh", destination, "--no-start")
        self.assertEqual(digest_tree(destination), before)

    def test_worker_installer_owns_only_worker_artifacts(self) -> None:
        destination = Path(self.temporary.name) / "worker-root"
        self.run_installer("install-worker.sh", destination)

        self.assertEqual(
            set(digest_tree(destination)),
            {
                "etc/systemd/system/sbgh-worker@.service",
                "etc/systemd/system/sbgh-worker@.service.d/hardening.conf",
                "usr/local/bin/sbgh-worker",
            },
        )
        installed = digest_tree(destination)
        self.assertEqual(installed["usr/local/bin/sbgh-worker"][0], 0o755)
        self.assertEqual(installed["etc/systemd/system/sbgh-worker@.service"][0], 0o644)
        self.assertEqual(
            installed["etc/systemd/system/sbgh-worker@.service.d/hardening.conf"][0],
            0o644,
        )
        self.assertEqual(
            stat.S_IMODE((destination / "etc/lvm/archive").stat().st_mode),
            0o700,
        )
        self.assertEqual(
            stat.S_IMODE((destination / "etc/lvm/backup").stat().st_mode),
            0o700,
        )
        hardening = (
            destination
            / "etc/systemd/system/sbgh-worker@.service.d/hardening.conf"
        ).read_text(encoding="utf-8")
        self.assertIn("/etc/lvm/archive", hardening)
        self.assertIn("/etc/lvm/backup", hardening)

        before = installed
        self.run_installer("install-worker.sh", destination)
        self.assertEqual(digest_tree(destination), before)

    def test_invalid_staging_root_is_rejected(self) -> None:
        environment = os.environ.copy()
        environment["DESTDIR"] = "relative"
        result = subprocess.run(
            [self.root / "scripts/install-worker.sh", "--no-build"],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("absolute staging directory", result.stderr)


if __name__ == "__main__":
    unittest.main()
