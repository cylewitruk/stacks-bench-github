#!/usr/bin/env python3
"""Keep generated fleet messages at the daemon/worker transport edges."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ALLOWED = {
    Path("crates/sbgh-daemon/src/fleet/grpc.rs"),
    Path("crates/sbgh-worker/src/transport.rs"),
}
GENERATED_IMPORT = re.compile(r"\bsbgh_proto(?:::|;)")
LEGACY_ROUTE = re.compile(
    r'"/v1/(?:register|poll|accept|repository-credential|heartbeat|events|'
    r'progress|artifacts/grant|complete|cleanup|deregister)'
)


def main() -> int:
    errors: list[str] = []
    for source in sorted((ROOT / "crates").glob("*/src/**/*.rs")):
        relative = source.relative_to(ROOT)
        text = source.read_text(encoding="utf-8")
        if GENERATED_IMPORT.search(text) and relative not in ALLOWED:
            errors.append(f"{relative}: generated fleet messages crossed the transport boundary")
        if LEGACY_ROUTE.search(text):
            errors.append(f"{relative}: legacy JSON fleet route remains")

    if errors:
        print("protobuf boundary check failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("protobuf boundary check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
