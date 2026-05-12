#!/usr/bin/env python
"""CLI entry point for the benchmark harness.

Usage:
    python benchmarks/run_benchmarks.py              # run all scenarios
    python benchmarks/run_benchmarks.py filter join  # run named scenarios
    python benchmarks/run_benchmarks.py --list       # list available scenarios
    python benchmarks/run_benchmarks.py --repeats 10 # override repeat count
"""
from __future__ import annotations
import argparse
import sys
import time
from pathlib import Path

# Make the src tree importable when run directly
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))

from benchmarks import ALL_SCENARIOS, BenchmarkRunner, render_all, render_table


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark Volcano vs Push vs Adaptive execution"
    )
    parser.add_argument(
        "scenarios",
        nargs="*",
        help="Scenario name substrings to run (default: all)",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=5,
        help="Timing repetitions per data point (default: 5)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available scenarios and exit",
    )
    args = parser.parse_args()

    if args.list:
        print("\nAvailable scenarios:")
        for s in ALL_SCENARIOS:
            print(f"  {s.name}")
            if s.description:
                print(f"    {s.description}")
        return

    # Select scenarios
    selected = ALL_SCENARIOS
    if args.scenarios:
        selected = [
            s for s in ALL_SCENARIOS
            if any(kw.lower() in s.name.lower() for kw in args.scenarios)
        ]
        if not selected:
            print(f"No scenarios matched: {args.scenarios}")
            print("Run with --list to see available scenarios.")
            sys.exit(1)

    runner = BenchmarkRunner(repeats=args.repeats)
    all_results: dict[str, list] = {}

    t_total = time.perf_counter()
    for scenario in selected:
        print(f"\nRunning: {scenario.name} ({len(scenario.param_values)} points × {args.repeats} reps)…")
        results = runner.run(scenario)
        all_results[scenario.name] = results
        print(render_table(results, title=scenario.name, description=scenario.description))

    elapsed = time.perf_counter() - t_total
    print(f"\nTotal benchmark time: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
