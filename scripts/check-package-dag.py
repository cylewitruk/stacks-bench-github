#!/usr/bin/env python3
"""Enforce the workspace's compiler-owned package boundaries."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

ALLOWED_INTERNAL = {
    "sbgh-api": set(),
    "sbgh-cli": {"sbgh-api"},
    "sbgh-core": set(),
    "sbgh-daemon": {
        "sbgh-api",
        "sbgh-core",
        "sbgh-driver",
        "sbgh-github",
        "sbgh-libvirt",
        "sbgh-postgres",
        "sbgh-worker",
    },
    "sbgh-driver": set(),
    "sbgh-github": {"sbgh-core"},
    "sbgh-handler": {"sbgh-api"},
    "sbgh-libvirt": {"sbgh-driver"},
    "sbgh-postgres": {"sbgh-core"},
    "sbgh-smee": set(),
    "sbgh-worker": {"sbgh-driver", "sbgh-libvirt"},
}

EXECUTION_FORBIDDEN = {
    "axum",
    "jsonwebtoken",
    "octocrab",
    "reqwest",
    "sbgh-core",
    "sbgh-daemon",
    "slack-messaging",
    "slack-morphism",
    "sqlx",
}
CORE_FORBIDDEN = {"jsonwebtoken", "octocrab", "reqwest", "sqlx"}


def metadata() -> dict:
    environment = os.environ.copy()
    environment["RUSTC_WRAPPER"] = ""
    result = subprocess.run(
        ["cargo", "metadata", "--locked", "--all-features", "--format-version", "1"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    return json.loads(result.stdout)


def main() -> int:
    data = metadata()
    packages = {package["id"]: package["name"] for package in data["packages"]}
    workspace_ids = set(data["workspace_members"])
    node_by_id = {node["id"]: node for node in data["resolve"]["nodes"]}
    architecture_edges: dict[str, set[str]] = defaultdict(set)

    for package_id, node in node_by_id.items():
        for dependency in node["deps"]:
            if any(kind["kind"] in (None, "normal", "build") for kind in dependency["dep_kinds"]):
                architecture_edges[package_id].add(dependency["pkg"])

    errors: list[str] = []
    actual_workspace_names = {packages[package_id] for package_id in workspace_ids}
    if actual_workspace_names != set(ALLOWED_INTERNAL):
        errors.append(
            "workspace packages differ from the documented DAG: "
            f"expected {sorted(ALLOWED_INTERNAL)}, got {sorted(actual_workspace_names)}"
        )

    ids_by_name = {packages[package_id]: package_id for package_id in workspace_ids}
    for package_name, allowed in ALLOWED_INTERNAL.items():
        package_id = ids_by_name.get(package_name)
        if package_id is None:
            continue
        actual = {
            packages[dependency_id]
            for dependency_id in architecture_edges[package_id]
            if dependency_id in workspace_ids
        }
        if actual != allowed:
            errors.append(
                f"{package_name}: internal runtime/build dependencies must be "
                f"{sorted(allowed)}, got {sorted(actual)}"
            )

    def transitive_names(root_name: str) -> set[str]:
        root_id = ids_by_name[root_name]
        seen: set[str] = set()
        pending = list(architecture_edges[root_id])
        while pending:
            package_id = pending.pop()
            if package_id in seen:
                continue
            seen.add(package_id)
            pending.extend(architecture_edges[package_id])
        return {packages[package_id] for package_id in seen}

    for package_name in ("sbgh-driver", "sbgh-libvirt", "sbgh-worker"):
        forbidden = transitive_names(package_name) & EXECUTION_FORBIDDEN
        if forbidden:
            errors.append(
                f"{package_name}: forbidden runtime/build dependency closure: {sorted(forbidden)}"
            )

    forbidden = transitive_names("sbgh-core") & CORE_FORBIDDEN
    if forbidden:
        errors.append(f"sbgh-core: forbidden runtime/build dependency closure: {sorted(forbidden)}")

    if errors:
        print("package DAG check failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    print("package DAG check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
