#!/usr/bin/env python3
"""Reject legacy names for the universal task-submission aggregate."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TEXT_SUFFIXES = {".md", ".rs", ".sh", ".sql", ".tmpl", ".toml", ".yaml", ".yml"}

LEGACY = re.compile(
    r"(?<![A-Za-z0-9])(?:"
    r"benchmark_group(?:_id)?|"
    r"benchmark_spec(?:_id)?|"
    r"benchmark_run_index|"
    r"benchmark_workflow_step|"
    r"benchmark_step_kind|"
    r"BenchmarkGroup|BenchmarkSpec|BenchmarkWorkflowStep|BenchmarkStepKind|"
    r"GroupRecovery|RecoverGroup|recover_group|recover-group|"
    r"recovery_of_group_id|prior_group_id|new_group_id|"
    r"/api/fleet/groups(?:/|\b)"
    r")(?![A-Za-z0-9])"
)

EXCLUDED_PREFIXES = (
    Path("planning/archive"),
    Path("target"),
)
EXCLUDED_FILES = {
    Path("planning/iterations/v27.1-task-neutral-submission-model.md"),
    Path("scripts/pg-restore-check.sh"),
}

# These are deliberate continuity/negative-contract assertions, not live model
# vocabulary. Keep the allowlist narrow so any new occurrence fails lint.
ALLOWED_FILES = {
    Path("crates/sbgh-postgres/tests/postgres_task_submission_migration.rs"),
}
ALLOWED_LINES = {
    Path("crates/sbgh-api/src/dto.rs"): (
        re.compile(r'\.get\("(?:prior|new)_group_id"\)'),
    ),
    Path("crates/sbgh-cli/src/main.rs"): (
        re.compile(r'"recover-group"'),
        re.compile(r'"--group-id"'),
    ),
    Path("crates/sbgh-daemon/src/api/mod.rs"): (
        re.compile(r'\.uri\("/api/fleet/groups/'),
    ),
}


def excluded(path: Path) -> bool:
    if path in EXCLUDED_FILES:
        return True
    if path.parts[:1] == ("migrations",) and path.name[:1].isdigit():
        return True
    return any(path.is_relative_to(prefix) for prefix in EXCLUDED_PREFIXES)


def allowed(path: Path, line: str) -> bool:
    if path in ALLOWED_FILES:
        return True
    return any(pattern.search(line) for pattern in ALLOWED_LINES.get(path, ()))


def main() -> int:
    errors: list[str] = []
    for absolute in sorted(ROOT.rglob("*")):
        if not absolute.is_file() or absolute.suffix not in TEXT_SUFFIXES:
            continue
        relative = absolute.relative_to(ROOT)
        if excluded(relative):
            continue
        try:
            lines = absolute.read_text().splitlines()
        except UnicodeDecodeError:
            continue
        for line_number, line in enumerate(lines, start=1):
            if LEGACY.search(line) and not allowed(relative, line):
                errors.append(f"{relative}:{line_number}: {line.strip()}")

    if errors:
        print("legacy task-submission naming check failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    bypass = re.compile(
        r"\.\s*(?:create_job_with_links|create_unlinked_job|"
        r"create_unlinked_benchmark_submission|enqueue_prepared_job)\s*\("
        r"|\b(?:NewJob|JobCreationRequest)\s*\{"
        r"|\bNewBenchmarkSpec\s*::"
    )
    for absolute in sorted((ROOT / "crates/sbgh-daemon/src").rglob("*.rs")):
        relative = absolute.relative_to(ROOT)
        production = absolute.read_text().split("#[cfg(test)]", maxsplit=1)[0]
        if bypass.search(production):
            print(
                f"task-submission kernel bypass in production module: {relative}",
                file=sys.stderr,
            )
            return 1

    print("task-submission naming check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
