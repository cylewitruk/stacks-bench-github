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
    for marker in ("workers: Vec<", "RawWorker", "client_ca_certificate"):
        if marker in text:
            errors.append(
                f"{path.relative_to(ROOT)}: forbidden static worker-policy shape {marker!r}"
            )

legacy_example = ROOT / "config.example.fleet.toml"
if legacy_example.exists():
    errors.append("config.example.fleet.toml: static fleet registry must not be restored")

worker_config = (ROOT / "crates/sbgh-worker/src/config.rs").read_text()
for marker in (
    "pub worker_id:",
    "pub capabilities:",
    "pub client_certificate:",
    "pub server_ca_certificate:",
    "pub libvirt:",
):
    if marker in worker_config:
        errors.append(f"worker config: forbidden legacy field {marker!r}")

for path in sorted(ROOT.glob("config.example.worker-*.toml")):
    text = path.read_text()
    for marker in ("worker_id =", "capabilities =", "client_certificate =", "[libvirt"):
        if marker in text:
            errors.append(f"{path.name}: forbidden legacy worker setting {marker!r}")

if errors:
    raise SystemExit("\n".join(errors))

print("worker registry boundary check passed")
