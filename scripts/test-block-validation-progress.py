#!/usr/bin/env python3
"""Focused tests for block-validation guest planning and progress."""

import ast
import io
import json
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys
import unittest
from unittest.mock import mock_open, patch


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


def guest_function(name, namespace):
    """Load a production guest function without executing its VM setup."""
    node = next(
        node for node in ast.parse(guest_python()).body
        if isinstance(node, ast.FunctionDef) and node.name == name
    )
    exec(compile(ast.Module(body=[node], type_ignores=[]), "guest.py", "exec"), namespace)
    return namespace[name]


def active_command(progress, ordinal=0, pending=None):
    """Return the progress-facing state of one active guest command."""
    return {
        "current_command_progress": progress,
        "current_command": {"ordinal": ordinal},
        "pending_commands": pending or [],
        "completed_command_blocks": 0,
    }


class ValidationStatusTests(unittest.TestCase):
    def test_initialization_is_not_processed_progress(self):
        rate = PROGRESS.ValidationRate()
        for now in (0, 3600, 7200):
            view = PROGRESS.validation_progress(0, 100, 0, 1, {0: active_command(0)}, [], rate, now)
            self.assertEqual(view["phase"], "initialize")
            self.assertEqual(view["current"], 0)
            self.assertIn("waiting for processed-block progress", view["message"])
            self.assertNotIn("ETA", view["message"])
        self.assertEqual(len(rate.samples), 0)

    def test_estimate_excludes_initialization_and_requires_stable_samples(self):
        rate = PROGRESS.ValidationRate()
        self.assertIsNone(rate.estimate(0, 0, 100, (0,), False))
        for offset in range(10):
            self.assertIsNone(rate.estimate(7200 + offset, offset + 1, 100, (0,), True))
        self.assertEqual(rate.estimate(7210, 11, 100, (0,), True), 89)

    def test_sample_window_is_bounded_and_no_faster_than_one_second(self):
        rate = PROGRESS.ValidationRate()
        for index in range(1000):
            rate.estimate(index / 5, index, 10000, (0,), True)
        self.assertEqual(len(rate.samples), 61)
        self.assertGreaterEqual(rate.samples[-1][0] - rate.samples[0][0], 60)

    def test_withholds_estimate_on_stall_cohort_change_and_regression(self):
        rate = PROGRESS.ValidationRate()
        for now in range(11):
            eta = rate.estimate(now, now + 1, 100, (0,), True)
        self.assertEqual(eta, 89)
        self.assertIsNone(rate.estimate(20, 11, 100, (0,), True))
        self.assertIsNone(rate.estimate(21, 12, 100, (1,), True))
        self.assertEqual(len(rate.samples), 1)
        self.assertIsNone(rate.estimate(22, 1, 100, (1,), True))
        self.assertEqual(len(rate.samples), 1)

    def test_unstable_rates_do_not_produce_eta(self):
        rate = PROGRESS.ValidationRate()
        for now in range(11):
            current = now + 1 if now <= 5 else 6 + (now - 5) * 10
            eta = rate.estimate(now, current, 1000, (0,), True)
        self.assertIsNone(eta)

    def test_unstarted_commands_and_mixed_initialization_withhold_eta(self):
        for active, pending in (
            ({0: active_command(10), 1: active_command(0)}, []),
            ({0: active_command(10)}, [1]),
            ({0: active_command(10, pending=[("naka-index-range", 0, 100)])}, []),
        ):
            rate = PROGRESS.ValidationRate()
            for now in range(20):
                view = PROGRESS.validation_progress(10, 100, 0, 2, active, pending, rate, now)
                self.assertEqual(view["phase"], "validate")
                self.assertNotIn("ETA", view["message"])
            self.assertEqual(len(rate.samples), 0)

    def test_silent_completion_does_not_create_eta(self):
        view = PROGRESS.validation_progress(100, 100, 1, 1, {}, [], PROGRESS.ValidationRate(), 7200)
        self.assertEqual(view["phase"], "validate")
        self.assertEqual(view["current"], 100)
        self.assertNotIn("ETA", view["message"])

    def test_guest_debounces_status_and_publishes_stalled_eta_removal(self):
        clock = type("Clock", (), {"now": 100.0, "monotonic": lambda self: self.now})()
        emitted = []
        namespace = {
            "time": clock, "last_progress": None, "last_progress_emit": 0,
            "progress_emit_interval": 5, "progress_rate": PROGRESS.ValidationRate(),
            "finished": [], "active": {0: active_command(0)}, "pending": [],
            "ranges": [0], "count": 100,
            "aggregate_block_progress": PROGRESS.aggregate_block_progress,
            "validation_progress": PROGRESS.validation_progress,
            "write_progress": lambda view: emitted.append((clock.now, view)),
        }
        emit = guest_function("emit_progress", namespace)
        emit()
        self.assertEqual(emitted[-1][1]["phase"], "initialize")
        for offset in range(1, 21):
            clock.now = 100 + offset
            namespace["active"][0]["current_command_progress"] = offset
            emit()
        self.assertIn("ETA", emitted[-1][1]["message"])
        for offset in range(21, 31):
            clock.now = 100 + offset
            emit()
        self.assertNotIn("ETA", emitted[-1][1]["message"])
        self.assertTrue(all(right[0] - left[0] >= 5 for left, right in zip(emitted, emitted[1:])))

    def test_progress_io_failure_does_not_fail_validation(self):
        write = guest_function("write_progress", {"RESULTS": "/unused", "json": json})
        with patch("builtins.open", side_effect=OSError("disk full")), patch("sys.stdout", new_callable=io.StringIO) as output:
            write({"phase": "initialize", "current": 0})
        self.assertIn("SBGH_BLOCK_VALIDATION_PROGRESS_WARNING", output.getvalue())

    def test_probe_announces_discovery_before_running_command(self):
        calls = []
        class Process:
            """Return a successful probe and record its launch ordering."""
            PIPE = -1

            def run(self, *args, **kwargs):
                """Record invocation and return a minimal successful result."""
                calls.append("run")
                return type("Result", (), {"returncode": 0, "stdout": "count 10", "stderr": ""})()

        namespace = {
            "write_progress": lambda view: calls.append(view), "probe_records": [],
            "BIN": "/unused", "database_paths": {0: "/unused"}, "RESULTS": "/unused",
            "json": json, "subprocess": Process(),
            "os": type("OS", (), {"fsync": lambda fd: None}),
        }
        probe = guest_function("probe", namespace)
        with patch("builtins.open", mock_open()), patch("sys.stdout", new_callable=io.StringIO):
            self.assertEqual(probe("index-range"), 10)
        self.assertEqual(calls[0]["phase"], "initialize")
        self.assertNotIn("total", calls[0])
        self.assertEqual(calls[1], "run")


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
