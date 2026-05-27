#!/usr/bin/env python3
"""
Compute per-metric variance statistics across recent stacks-bench runs.

Walks `<results-dir>/<job-id>/run.json`, sorts by mtime newest-first,
takes the most recent N, and prints mean / stddev / CV% / min / max /
range% for each metric present in the envelope and `data.summary`.

Typical use: trigger several /benchmark runs on the SAME commit, then
run this to characterise the noise floor across separate VM boots.
The CV% column is the headline number for setting an auto-regression
threshold honestly (rather than picking one out of the air).

NOTE: the script does NOT verify the runs are all on the same SHA — it
just takes the N most recent. Trigger your variance runs back-to-back
with nothing else in flight, or filter the results dir to match.

Usage:
    scripts/variance-stats.py                      # last 5 from /var/lib/sbgh/results
    scripts/variance-stats.py --last 10
    scripts/variance-stats.py --results-dir /custom/path --last 8
"""

import argparse
import json
import statistics
import sys
from pathlib import Path

# (display label, JSON pointer into run.json)
METRICS = [
    ("Run duration (s)",       ("duration_secs",)),
    ("Replay duration (s)",    ("data", "duration_secs")),
    ("Total bench (µs)",       ("data", "summary", "total_duration_us")),
    ("Setup (µs)",             ("data", "summary", "setup_duration_us")),
    ("Execution (µs)",         ("data", "summary", "execution_duration_us")),
    ("Commit (µs)",            ("data", "summary", "commit_duration_us")),
    ("Clarity runtime",        ("data", "summary", "clarity_runtime")),
    ("Transactions",           ("data", "summary", "transactions")),
    ("Read length (B)",        ("data", "summary", "read_length")),
    ("Write length (B)",       ("data", "summary", "write_length")),
]


def dig(obj, path):
    for key in path:
        if not isinstance(obj, dict) or key not in obj:
            return None
        obj = obj[key]
    return obj


def find_recent_runs(results_dir: Path, n: int):
    candidates = []
    for run_json in results_dir.glob("*/run.json"):
        try:
            candidates.append((run_json.stat().st_mtime, run_json))
        except OSError:
            continue
    candidates.sort(reverse=True)
    return [p for _, p in candidates[:n]]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--last", type=int, default=5,
                    help="number of most-recent runs to include (default: 5)")
    ap.add_argument("--results-dir", type=Path,
                    default=Path("/var/lib/sbgh/results"),
                    help="archive root (default: /var/lib/sbgh/results)")
    args = ap.parse_args()

    if not args.results_dir.is_dir():
        print(f"results dir not found: {args.results_dir}", file=sys.stderr)
        return 1

    paths = find_recent_runs(args.results_dir, args.last)
    runs = []
    for p in paths:
        try:
            runs.append((p.parent.name, json.loads(p.read_text())))
        except (OSError, json.JSONDecodeError) as e:
            print(f"skipping {p}: {e}", file=sys.stderr)

    if len(runs) < 2:
        print(f"need >=2 parseable runs for variance, got {len(runs)}", file=sys.stderr)
        return 1

    print(f"# {len(runs)} run(s) from {args.results_dir}")
    for job_id, _ in runs:
        print(f"#   {job_id}")
    print()

    header = ("Metric", "n", "mean", "stddev", "CV%", "min", "max", "range%")
    widths = (24, 3, 18, 18, 7, 18, 18, 8)
    row = lambda cells: "  ".join(
        f"{c:<{w}}" if i < 2 else f"{c:>{w}}"
        for i, (c, w) in enumerate(zip(cells, widths))
    )
    print(row(header))
    print("-" * (sum(widths) + 2 * (len(widths) - 1)))

    for label, path in METRICS:
        values = []
        for _, run in runs:
            v = dig(run, path)
            if v is None:
                values = []  # any missing → skip metric (partial stats mislead)
                break
            values.append(float(v))
        if not values:
            continue
        mean = statistics.fmean(values)
        sd = statistics.stdev(values) if len(values) >= 2 else 0.0
        cv = (sd / mean * 100.0) if mean else 0.0
        lo, hi = min(values), max(values)
        rng_pct = ((hi - lo) / mean * 100.0) if mean else 0.0
        print(row((
            label,
            str(len(values)),
            f"{mean:,.2f}",
            f"{sd:,.2f}",
            f"{cv:.2f}",
            f"{lo:,.2f}",
            f"{hi:,.2f}",
            f"{rng_pct:.2f}",
        )))

    return 0


if __name__ == "__main__":
    sys.exit(main())
