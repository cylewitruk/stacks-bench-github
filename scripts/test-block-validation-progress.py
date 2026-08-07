#!/usr/bin/env python3
"""Focused tests for the guest block-validation progress parser."""

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


class ProgressParserTests(unittest.TestCase):
    def test_rendered_guest_python_compiles(self):
        template = (
            SOURCE.with_name("sbgh-block-validation.sh.tmpl")
            .read_text(encoding="utf-8")
            .replace("{{ block_validation_progress }}", SOURCE.read_text(encoding="utf-8"))
        )
        heredoc = template.split("python3 - <<'PY'", 1)[1]
        embedded = heredoc.split("\n", 1)[1].split("\nPY\n", 1)[0]
        compile(embedded, "sbgh-block-validation.py", "exec")

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
