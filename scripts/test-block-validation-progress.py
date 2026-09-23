#!/usr/bin/env python3
"""Focused tests for block-validation guest planning and progress."""

import ast
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True
SOURCE = (
    ROOT
    / "crates"
    / "sbgh-libvirt"
    / "src"
    / "libvirt"
    / "templates"
    / "block_validation_progress.py"
)
SPEC = spec_from_file_location("block_validation_progress", SOURCE)
assert SPEC is not None and SPEC.loader is not None
PROGRESS = module_from_spec(SPEC)
SPEC.loader.exec_module(PROGRESS)


def guest_python():
    """Return the rendered Python body from the block-validation guest script."""
    template = (
        SOURCE.with_name("sbgh-block-validation.sh.tmpl")
        .read_text(encoding="utf-8")
        .replace("{{ block_validation_progress }}", SOURCE.read_text(encoding="utf-8"))
    )
    heredoc = template.split("python3 - <<'PY'", 1)[1]
    return heredoc.split("\n", 1)[1].split("\nPY\n", 1)[0]


def plan_ranges(selection, pre, naka):
    """Execute the guest's pure selection/partition slice with probe fixtures."""
    tree = ast.parse(guest_python())
    start = next(
        index
        for index, node in enumerate(tree.body)
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "pre" for target in node.targets)
    )
    end = next(
        index
        for index, node in enumerate(tree.body[start:], start)
        if isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == "phase"
    )
    fragment = ast.Module(body=tree.body[start:end], type_ignores=[])
    namespace = {
        "probe": lambda kind: pre if kind == "index-range" else naka,
        "plan": {
            "selection": selection,
            "target_blocks_per_shard": 2,
            "max_shards": 4,
            "devices": [{"shard": index} for index in range(4)],
        },
    }
    exec(compile(fragment, "sbgh-guest-planning.py", "exec"), namespace)
    return namespace


class ProgressParserTests(unittest.TestCase):
    def test_rendered_guest_python_compiles(self):
        compile(guest_python(), "sbgh-block-validation.py", "exec")

    def test_recent_uses_one_height_ordered_last_command(self):
        result = plan_ranges({"kind": "recent", "block_count": 1}, 185630, 8865006)
        self.assertEqual(result["shard_count"], 1)
        self.assertEqual(
            result["ranges"],
            [(0, 9050635, 9050635, [("last", 8865005, 8865005)])],
        )

    def test_cross_epoch_range_keeps_two_index_commands(self):
        result = plan_ranges(
            {"kind": "range", "range": {"start": 99, "end": 100}}, 100, 900
        )
        self.assertEqual(
            result["ranges"],
            [(0, 99, 100, [("index-range", 99, 99), ("naka-index-range", 0, 0)])],
        )

    def test_fragmented_carriage_return_record_is_parsed(self):
        pending, current = PROGRESS.parse_progress_chunk(
            b"", b"\rValidating: 4", 100
        )
        self.assertIsNone(current)
        pending, current = PROGRESS.parse_progress_chunk(
            pending, b"2% (42/100)\r", 100
        )
        self.assertEqual(current, 42)
        self.assertLessEqual(len(pending), PROGRESS.MAX_PROGRESS_PENDING_BYTES)

    def test_greatest_valid_counter_wins(self):
        _, current = PROGRESS.parse_progress_chunk(
            b"",
            b"Validating: 10% (10/100)\rValidating: 47% (47/100)\r",
            100,
        )
        self.assertEqual(current, 47)

    def test_untrusted_or_mismatched_records_are_ignored(self):
        _, current = PROGRESS.parse_progress_chunk(
            b"",
            (
                b"Validating: 101% (10/100)\r"
                b"Validating: 50% (50/99)\r"
                b"Validating: 50% (101/100)\r"
                b"Validating: 999999999999999999999% (1/100)\r"
            ),
            100,
        )
        self.assertIsNone(current)

    def test_aggregate_is_block_weighted_and_bounded(self):
        self.assertEqual(
            PROGRESS.aggregate_block_progress(
                [(0, 9)], [20, 5], trusted_total=110
            ),
            35,
        )
        self.assertEqual(
            PROGRESS.aggregate_block_progress(
                [(0, 99)], [50], trusted_total=110
            ),
            110,
        )


if __name__ == "__main__":
    unittest.main()
