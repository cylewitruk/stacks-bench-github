#!/usr/bin/env python3
"""Keep task-aware reporting on the shared typed boundary."""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
DAEMON = ROOT / "crates/sbgh-daemon/src"

errors: list[str] = []
for relative in ("report.rs", "reporter.rs", "slack_report.rs"):
    text = (DAEMON / relative).read_text()
    if 'get("task")' in text or "get('task')" in text:
        errors.append(f"{relative}: infers task kind from untyped summary JSON")
    if "block_validation_result" in text:
        errors.append(f"{relative}: renderer contains persistence SQL/table knowledge")

reporter = (DAEMON / "reporter.rs").read_text()
if "const CHECK_NAME" in reporter:
    errors.append("reporter.rs: global check-name constant bypasses exhaustive task policy")
terminal_projection = reporter.partition("pub(crate) async fn report_fleet_terminal")[2].partition(
    "/// The reporter task body"
)[0]
for required in ("match &snapshot.task", "TaskReport::Benchmark"):
    if required not in terminal_projection:
        errors.append(
            f"reporter.rs: terminal projection is missing benchmark-gated token {required!r}"
        )
if terminal_projection.find("TaskReport::Benchmark") > terminal_projection.find(
    ".multi_variant_comparison()"
):
    errors.append(
        "reporter.rs: benchmark comparison read appears before exhaustive task dispatch"
    )

report = (DAEMON / "report.rs").read_text()
for required in ("stacks-bench", "stacks-block-validation", "TaskKind::BuildOnly => None"):
    if required not in report:
        errors.append(f"report.rs: missing task-aware check policy token {required!r}")
for relative in ("report.rs", "slack_report.rs"):
    text = (DAEMON / relative).read_text()
    if "task_kind == TaskKind::BlockValidation" in text:
        errors.append(f"{relative}: terminal renderer uses non-exhaustive validation dispatch")
    if "TaskReport::BlockValidation" not in text:
        errors.append(f"{relative}: terminal renderer does not consume the canonical task view")

if errors:
    print("reporting boundary check failed:", file=sys.stderr)
    for error in errors:
        print(f"- {error}", file=sys.stderr)
    raise SystemExit(1)

print("reporting boundary check passed")
