#!/usr/bin/env python3
"""Validate repository-local Markdown links and the planning registry."""

from __future__ import annotations

import re
import subprocess
import sys
import unicodedata
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parent.parent
MARKDOWN = [ROOT / "README.md", *sorted((ROOT / "docs").rglob("*.md")), *sorted((ROOT / "planning").rglob("*.md"))]
INLINE_LINK = re.compile(
    r"!?\[[^\]]*]\(\s*(?P<target><[^>]+>|[^\s)]+)(?:\s+[^)]*)?\)"
)
REFERENCE_LINK = re.compile(
    r"!?\[(?P<text>[^\]]+)]\[(?P<label>[^\]]*)]"
)
SHORTCUT_REFERENCE = re.compile(r"\[(?P<label>[^\]\n]+)](?![\[(])")
REFERENCE_DEFINITION = re.compile(
    r"^\s{0,3}\[(?P<label>[^\]^][^\]]*)]:\s*(?P<target><[^>]+>|\S+)"
)
ATX_HEADING = re.compile(r"^\s{0,3}#{1,6}\s+(?P<heading>.*)$")
SETEXT_HEADING = re.compile(r"^\s{0,3}(?:=+|-+)\s*$")
HTML_ANCHOR = re.compile(
    r"<[^>]+\s(?:id|name)=[\"'](?P<anchor>[^\"']+)[\"'][^>]*>",
    re.IGNORECASE,
)
ROW = re.compile(
    r"^\| `(?P<id>[^`]+)` \| .* \| `(?P<status>[^`]+)` \| "
    r"\[[^]]+]\((?P<location>[^)]+)\) \|$"
)
ITEM_ROW = re.compile(
    r"^\| `(?P<id>\d{4}-[^`]+)` \| .* \| (?P<status>[a-z_]+) \|$"
)
STATUS = re.compile(r"^> \*\*Status:\*\* (?P<status>[a-z_]+)\b")
HISTORICAL_MARKER = re.compile(
    r"roadmap-v\d+|\bslice\s+\d+|\bphase\s+\d+|review fix", re.IGNORECASE
)
PROVIDER_INSTALL = re.compile(
    r"let\s+_\s*=\s*rustls\s*::\s*crypto\s*::\s*ring\s*::\s*default_provider\s*"
    r"\(\s*\)\s*\.\s*install_default\s*\(\s*\)"
)
MAIN_FUNCTION = re.compile(
    r"(?:async\s+)?fn\s+main\s*\([^)]*\)\s*(?:->\s*[^{]+)?\{"
)
# Existing production-comment debt is budgeted per file. Counts may decrease,
# but a new marker or an increase in any file fails the check. Current phase
# names that are part of a runtime protocol remain budgeted intentionally.
HISTORICAL_MARKER_BUDGET = {
    "crates/sbgh-api/src/dto.rs": 1,
    "crates/sbgh-postgres/src/admin/mod.rs": 2,
    "crates/sbgh-postgres/src/admin/policy.rs": 2,
    "crates/sbgh-postgres/src/admin/repo.rs": 1,
    "crates/sbgh-postgres/src/admin/user.rs": 2,
    "crates/sbgh-core/src/bench_args.rs": 1,
    "crates/sbgh-core/src/db/ingest.rs": 2,
    "crates/sbgh-core/src/db/installation.rs": 6,
    "crates/sbgh-core/src/db/jobs.rs": 32,
    "crates/sbgh-core/src/db/policy.rs": 4,
    "crates/sbgh-postgres/src/stores/installation.rs": 4,
    "crates/sbgh-postgres/src/stores/jobs.rs": 2,
    "crates/sbgh-postgres/src/stores/policy.rs": 1,
    "crates/sbgh-postgres/src/stores/pull_request.rs": 1,
    "crates/sbgh-postgres/src/stores/user.rs": 1,
    "crates/sbgh-postgres/src/stores/webhook.rs": 1,
    "crates/sbgh-core/src/db/pull_request.rs": 8,
    "crates/sbgh-core/src/db/repo.rs": 1,
    "crates/sbgh-core/src/db/user.rs": 3,
    "crates/sbgh-core/src/db/webhook.rs": 3,
    "crates/sbgh-github/src/auth.rs": 1,
    "crates/sbgh-github/src/client.rs": 1,
    "crates/sbgh-github/src/api.rs": 11,
    "crates/sbgh-core/src/models.rs": 38,
    "crates/sbgh-daemon/src/api/mod.rs": 4,
    "crates/sbgh-daemon/src/api/state.rs": 1,
    "crates/sbgh-daemon/src/bench_summary.rs": 2,
    "crates/sbgh-daemon/src/comparison.rs": 2,
    "crates/sbgh-driver/src/events.rs": 4,
    "crates/sbgh-daemon/src/job_source.rs": 10,
    "crates/sbgh-libvirt/src/libvirt/domain.rs": 1,
    "crates/sbgh-libvirt/src/libvirt/driver.rs": 2,
    "crates/sbgh-libvirt/src/libvirt/git_mirror.rs": 1,
    "crates/sbgh-libvirt/src/libvirt/progress.rs": 1,
    "crates/sbgh-daemon/src/pin_manager.rs": 1,
    "crates/sbgh-daemon/src/pin_resolver.rs": 1,
    "crates/sbgh-worker/src/recipe.rs": 1,
    "crates/sbgh-daemon/src/reporter.rs": 5,
    "crates/sbgh-daemon/src/shutdown.rs": 2,
    "crates/sbgh-daemon/src/slack/connector.rs": 1,
    "crates/sbgh-daemon/src/slack/session.rs": 1,
    "crates/sbgh-daemon/src/slack/timeline.rs": 1,
    "crates/sbgh-daemon/src/webhook_processor.rs": 73,
    "crates/sbgh-core/src/workload.rs": 1,
}


def markdown_prose(text: str) -> str:
    """Remove fenced code so examples are not treated as links or headings."""
    lines: list[str] = []
    fence: tuple[str, int] | None = None
    for line in text.splitlines():
        match = re.match(r"^\s{0,3}(`{3,}|~{3,})", line)
        if match:
            marker = match.group(1)
            if fence is None:
                fence = (marker[0], len(marker))
            elif marker[0] == fence[0] and len(marker) >= fence[1]:
                fence = None
            lines.append("")
        elif fence is None:
            lines.append(line)
        else:
            lines.append("")
    return "\n".join(lines)


def heading_slug(heading: str) -> str:
    """Approximate GitHub's generated heading-id algorithm."""
    heading = re.sub(r"\s+#+\s*$", "", heading).strip()
    heading = re.sub(r"<[^>]*>", "", heading)
    heading = re.sub(r"!\[([^\]]*)]\([^)]*\)", r"\1", heading)
    heading = re.sub(r"\[([^\]]+)]\([^)]*\)", r"\1", heading)
    heading = re.sub(r"[`*_~]", "", heading).lower()
    # GitHub preserves hyphens and underscores while dropping ASCII
    # punctuation and Unicode punctuation such as em dashes. Each whitespace
    # character becomes one hyphen.
    heading = re.sub(r"""[!\"#$%&'()*+,./:;<=>?@\[\\\]^`{|}]""", "", heading)
    heading = "".join(
        character
        for character in heading
        if character in "-_" or not unicodedata.category(character).startswith("P")
    )
    return re.sub(r"\s", "-", heading)


def markdown_anchors(text: str) -> set[str]:
    anchors = set(HTML_ANCHOR.findall(text))
    duplicates: dict[str, int] = {}
    lines = markdown_prose(text).splitlines()

    def add(heading: str) -> None:
        base = heading_slug(heading)
        suffix = duplicates.get(base, 0)
        duplicates[base] = suffix + 1
        anchors.add(base if suffix == 0 else f"{base}-{suffix}")

    for index, line in enumerate(lines):
        match = ATX_HEADING.match(line)
        if match is not None:
            add(match["heading"])
        elif (
            SETEXT_HEADING.match(line)
            and index > 0
            and lines[index - 1].strip()
        ):
            add(lines[index - 1].strip())
    return anchors


def normalize_reference_label(label: str) -> str:
    return " ".join(label.split()).casefold()


def markdown_targets(source: Path, text: str, errors: list[str]) -> list[str]:
    prose = markdown_prose(text)
    definitions: dict[str, str] = {}
    for line in prose.splitlines():
        if match := REFERENCE_DEFINITION.match(line):
            definitions[normalize_reference_label(match["label"])] = match["target"]

    targets = [match["target"] for match in INLINE_LINK.finditer(prose)]
    for match in REFERENCE_LINK.finditer(prose):
        label = match["label"] or match["text"]
        target = definitions.get(normalize_reference_label(label))
        if target is None:
            errors.append(
                f"{source.relative_to(ROOT)}: undefined Markdown reference {label!r}"
            )
        else:
            targets.append(target)
    for match in SHORTCUT_REFERENCE.finditer(prose):
        if target := definitions.get(normalize_reference_label(match["label"])):
            targets.append(target)
    # Validate definitions even when currently unused; leaving a latent broken
    # target makes a future reference silently bad.
    targets.extend(definitions.values())
    return targets


def local_target(source: Path, raw: str) -> tuple[Path, str | None] | None:
    target = raw.strip()
    if target.startswith("<") and target.endswith(">"):
        target = target[1:-1]
    parsed = urlsplit(target)
    if parsed.scheme or parsed.netloc:
        return None
    path = unquote(parsed.path)
    resolved = source if not path else (source.parent / path).resolve()
    fragment = unquote(parsed.fragment) if parsed.fragment else None
    return resolved, fragment


def check_link_parser_contract(errors: list[str]) -> None:
    sample = """\
## Transport & format
## Transport & format
```md
## Not a heading
```
"""
    sample += "Reference links\n---\n"
    expected = {"transport--format", "transport--format-1", "reference-links"}
    if markdown_anchors(sample) != expected:
        errors.append("check-docs parser: heading anchors or duplicate suffixes regressed")

    reference_errors: list[str] = []
    targets = markdown_targets(
        ROOT / "README.md",
        "[Statuses][status-ref]\n[status-ref]: README.md#statuses\n",
        reference_errors,
    )
    if reference_errors or "README.md#statuses" not in targets:
        errors.append("check-docs parser: reference-style links regressed")


def check_links(errors: list[str]) -> None:
    anchors: dict[Path, set[str]] = {}
    for source in MARKDOWN:
        for raw in markdown_targets(source, source.read_text(), errors):
            resolved = local_target(source, raw)
            if resolved is None:
                continue
            target, fragment = resolved
            if not target.exists():
                errors.append(
                    f"{source.relative_to(ROOT)}: broken local link {raw!r}"
                )
                continue
            if fragment is None:
                continue
            if not target.is_file() or target.suffix.lower() not in {".md", ".markdown"}:
                errors.append(
                    f"{source.relative_to(ROOT)}: fragment link {raw!r} does not target Markdown"
                )
                continue
            target_anchors = anchors.setdefault(
                target, markdown_anchors(target.read_text())
            )
            if fragment not in target_anchors:
                errors.append(
                    f"{source.relative_to(ROOT)}: broken heading fragment {raw!r}"
                )


def registry_section(lines: list[str], start: str, end: str) -> dict[str, tuple[str, str]]:
    active = False
    entries: dict[str, tuple[str, str]] = {}
    for line in lines:
        if line == start:
            active = True
            continue
        if active and line == end:
            break
        if active and (match := ROW.match(line)):
            entries[match["id"]] = (match["status"], match["location"])
    return entries


def check_registry(errors: list[str]) -> None:
    index = ROOT / "planning/index.md"
    lines = index.read_text().splitlines()
    items = registry_section(lines, "## Items", "## Iterations")
    iterations = registry_section(lines, "## Iterations", "## Decisions")

    for entry_id, (_, location) in [*items.items(), *iterations.items()]:
        if not (index.parent / location).exists():
            errors.append(f"planning/index.md: {entry_id} points to missing {location}")

    for path in sorted((ROOT / "planning/iterations").glob("v*.md")):
        relative = path.relative_to(ROOT / "planning")
        iteration_id = path.stem
        registered = iterations.get(iteration_id)
        if registered is None:
            errors.append(f"{relative}: iteration is missing from planning/index.md")
            continue

        status = next(
            (match["status"] for line in path.read_text().splitlines() if (match := STATUS.match(line))),
            None,
        )
        if status is None:
            errors.append(f"{relative}: missing iteration status")
        elif registered != (status, relative.as_posix()):
            errors.append(
                f"{relative}: registry has {registered}, document has "
                f"({status!r}, {relative.as_posix()!r})"
            )

        for line in path.read_text().splitlines():
            match = ITEM_ROW.match(line)
            if match is None:
                continue
            item = items.get(match["id"])
            expected = (match["status"], relative.as_posix())
            if item != expected:
                errors.append(
                    f"{relative}: {match['id']} registry has {item}, document has {expected}"
                )


def cargo_metadata() -> dict:
    result = subprocess.run(
        ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    import json

    return json.loads(result.stdout)


def check_workspace_inventory(errors: list[str], metadata: dict) -> None:
    packages = {package["name"] for package in metadata["packages"]}
    binaries = {
        target["name"]
        for package in metadata["packages"]
        for target in package["targets"]
        if "bin" in target["kind"]
    }
    readme = (ROOT / "README.md").read_text()
    architecture = (ROOT / "docs/architecture.md").read_text()
    documented = set(re.findall(r"`(sbgh-[a-z-]+)`", readme + architecture))
    missing = sorted((packages | binaries) - documented)
    if missing:
        errors.append(f"workspace packages/targets missing from docs: {', '.join(missing)}")

    for package in metadata["packages"]:
        if package["publish"] != []:
            errors.append(f"{package['name']}: internal workspace crate must set publish = false")

    recipes = subprocess.run(
        ["just", "--list", "--unsorted"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    available = {
        match.group(1)
        for line in recipes.splitlines()
        if (match := re.match(r"\s{4}([a-z][a-z0-9-]*)\b", line))
    }
    documented_commands: set[str] = set()
    for path in [ROOT / "AGENTS.md", ROOT / "README.md", *sorted((ROOT / "docs").glob("*.md"))]:
        text = path.read_text()
        documented_commands.update(
            re.findall(r"`just ([a-z][a-z0-9-]*)[^`]*`", text)
        )
        documented_commands.update(
            re.findall(r"(?m)^\s*just ([a-z][a-z0-9-]*)\b", text)
        )
    unknown = sorted(documented_commands - available)
    if unknown:
        errors.append(f"documented just recipes do not exist: {', '.join(unknown)}")

    for recipe in ("build", "lint", "test"):
        help_result = subprocess.run(
            ["just", recipe, "--help"],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        if help_result.returncode != 0:
            errors.append(f"just {recipe} --help failed: {help_result.stderr.strip()}")


def provider_bootstrap_error(source: str) -> str | None:
    installs = list(PROVIDER_INSTALL.finditer(source))
    if len(installs) != 1:
        return f"expected exactly one ring provider installation, found {len(installs)}"
    main = MAIN_FUNCTION.search(source)
    if main is None:
        return "could not locate main function"
    install = installs[0]
    if install.start() < main.end():
        return "provider installation is outside the main body"

    prefix = source[main.end() : install.start()]
    prefix = re.sub(r"//[^\n]*|/\*.*?\*/", "", prefix, flags=re.DOTALL)
    if prefix.strip():
        return "provider installation must be the first statement in main"
    return None


def check_crypto_bootstrap(errors: list[str], metadata: dict) -> None:
    binaries = [
        (package["name"], target)
        for package in metadata["packages"]
        for target in package["targets"]
        if "bin" in target["kind"]
    ]
    for package, target in binaries:
        source = Path(target["src_path"])
        if error := provider_bootstrap_error(source.read_text()):
            errors.append(f"{package}/{target['name']}: {error}")


def check_crypto_parser_contract(errors: list[str]) -> None:
    install = "let _ = rustls::crypto::ring::default_provider().install_default();"
    good = f"async fn main() -> anyhow::Result<()> {{\n{install}\nstart_tls();\n}}"
    bad = f"async fn main() -> anyhow::Result<()> {{\nstart_tls();\n{install}\n}}"
    if provider_bootstrap_error(good) is not None or provider_bootstrap_error(bad) is None:
        errors.append("check-docs parser: crypto bootstrap ordering check regressed")


def check_historical_comment_ratchet(errors: list[str]) -> None:
    test_only_names = {"tests.rs", "test_support.rs"}
    for path in sorted((ROOT / "crates").glob("*/src/**/*.rs")):
        if path.name in test_only_names or path.name.endswith("_tests.rs"):
            continue
        relative = path.relative_to(ROOT).as_posix()
        production = path.read_text().split("#[cfg(test)]\nmod tests", maxsplit=1)[0]
        count = sum(
            1
            for line in production.splitlines()
            if line.lstrip().startswith("//") and HISTORICAL_MARKER.search(line)
        )
        budget = HISTORICAL_MARKER_BUDGET.get(relative, 0)
        if count > budget:
            errors.append(
                f"{relative}: {count} historical planning markers exceed budget {budget}"
            )


def main() -> int:
    errors: list[str] = []
    check_link_parser_contract(errors)
    check_crypto_parser_contract(errors)
    check_links(errors)
    check_registry(errors)
    metadata = cargo_metadata()
    check_workspace_inventory(errors, metadata)
    check_crypto_bootstrap(errors, metadata)
    check_historical_comment_ratchet(errors)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("documentation links and planning registry are consistent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
