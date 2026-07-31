#!/usr/bin/env python3
"""Keep shared intent and Slack intake task-aware."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
errors: list[str] = []

for path in sorted((ROOT / "crates").glob("*/src/**/*.rs")):
    text = path.read_text()
    for marker in ("trait BenchmarkQueue", "SlackBenchmarkQueue", "GitHubBlockValidationConfig"):
        if marker in text:
            errors.append(f"{path.relative_to(ROOT)}: legacy intake marker {marker!r}")

for path in [
    ROOT / "config.example.daemon.toml",
    *sorted((ROOT / "docs").glob("*.md")),
]:
    if "[github.block_validation]" in path.read_text():
        errors.append(f"{path.relative_to(ROOT)}: validation policy remains GitHub-owned")

intent = (ROOT / "crates/sbgh-intent/src/intent.rs").read_text()
for marker in ("enum UserIntent", "enum TaskCreationIntent", "BlockValidation(BlockValidationIntent)"):
    if marker not in intent:
        errors.append(f"intent contract is missing exhaustive marker {marker!r}")
for marker in ("Resolved(BenchmarkRequest)", "Resolved(Box<BenchmarkRequest>)"):
    if marker in intent:
        errors.append(f"intent contract regressed to benchmark-only shape {marker!r}")

slack = (ROOT / "crates/sbgh-slack/src/connector.rs").read_text()
for marker in ("trait TaskSubmissionPort", "TaskSubmissionRequest::BlockValidation"):
    if marker not in slack:
        errors.append(f"Slack intake is missing task-aware marker {marker!r}")

if errors:
    raise SystemExit("\n".join(errors))

print("task-aware intake boundary check passed")
