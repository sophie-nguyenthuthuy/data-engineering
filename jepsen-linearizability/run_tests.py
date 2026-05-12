#!/usr/bin/env python3
"""CLI entry point for the Jepsen linearizability test harness.

Usage:
  python run_tests.py                          # default: 3 nodes, 5 clients, 10s
  python run_tests.py --nodes 5 --clients 8 --duration 30
  python run_tests.py --no-crashes             # disable process crash nemesis
  python run_tests.py --no-partitions --no-clock-skew  # network-only calm run
  python run_tests.py --output-dir /tmp/reports
"""

import sys
import click
from rich.console import Console

from jepsen.core.runner import TestConfig, run

console = Console()


@click.command()
@click.option("--nodes",       default=3,    show_default=True, help="Number of cluster nodes")
@click.option("--clients",     default=5,    show_default=True, help="Number of concurrent clients")
@click.option("--duration",    default=10.0, show_default=True, help="Test duration in seconds")
@click.option("--keys",        default="x,y,z", show_default=True, help="Comma-separated key names")
@click.option("--timeout",     default=1.5,  show_default=True, help="Per-request timeout (s)")
@click.option("--partitions/--no-partitions", default=True,  help="Enable network partition nemesis")
@click.option("--clock-skew/--no-clock-skew", default=True,  help="Enable clock skew nemesis")
@click.option("--crashes/--no-crashes",       default=True,  help="Enable process crash nemesis")
@click.option("--max-skew",    default=3.0,  show_default=True, help="Max clock skew in seconds")
@click.option("--output-dir",  default="reports", show_default=True, help="Report output directory")
def main(nodes, clients, duration, keys, timeout, partitions, clock_skew, crashes, max_skew, output_dir):
    """Run a Jepsen-style linearizability test against a replicated pipeline."""
    config = TestConfig(
        node_count=nodes,
        client_count=clients,
        test_duration_s=duration,
        keys=keys.split(","),
        request_timeout_s=timeout,
        enable_network_partitions=partitions,
        enable_clock_skew=clock_skew,
        enable_process_crashes=crashes,
        max_clock_skew_s=max_skew,
        output_dir=output_dir,
    )

    result = run(config)

    sys.exit(0 if result.linearizable else 1)


if __name__ == "__main__":
    main()
