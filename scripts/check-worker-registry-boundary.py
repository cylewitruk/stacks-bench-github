#!/usr/bin/env python3
"""Reject static worker-policy authority outside PostgreSQL."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN = (
    "SBGH_FLEET_CONFIG",
    "ConfiguredWorker",
    "disable_workers_except",
)

errors: list[str] = []
for path in sorted((ROOT / "crates").glob("*/src/**/*.rs")):
    text = path.read_text()
    for marker in FORBIDDEN:
        if marker in text:
            errors.append(f"{path.relative_to(ROOT)}: forbidden static registry marker {marker!r}")

for path in [
    ROOT / "crates/sbgh-core/src/config.rs",
    *sorted((ROOT / "crates/sbgh-daemon/src/fleet").glob("*.rs")),
]:
    text = path.read_text()
    for marker in ("workers: Vec<", "RawWorker", "certificate_sha256: Vec<"):
        if marker in text:
            errors.append(
                f"{path.relative_to(ROOT)}: forbidden static worker-policy shape {marker!r}"
            )

legacy_example = ROOT / "config.example.fleet.toml"
if legacy_example.exists():
    errors.append("config.example.fleet.toml: static fleet registry must not be restored")

if errors:
    raise SystemExit("\n".join(errors))

print("worker registry boundary check passed")
