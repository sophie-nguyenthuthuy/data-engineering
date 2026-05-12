"""
CLI entry point for the self-healing ETL framework.

Usage examples:
    # Run a CSV file through the pipeline
    python main.py --source orders.csv --dest output.csv

    # Run with strict schema mode (quarantine on any drift, no healing)
    python main.py --source orders.csv --dest output.csv --strict

    # Run the interactive demo
    python main.py --demo
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from config import ETLConfig, SchemaRegistryConfig, QuarantineConfig, HealingConfig, AlertConfig
from pipeline.orchestrator import etl_flow


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="etl",
        description="Self-Healing ETL — Prefect pipeline with schema drift detection",
    )
    p.add_argument("--demo", action="store_true", help="Run the built-in demo scenario")
    p.add_argument("--source", help="Source file path (CSV or JSONL)")
    p.add_argument("--source-type", default="csv", choices=["csv", "jsonl"],
                   help="Source format (default: csv)")
    p.add_argument("--dest", help="Destination file path")
    p.add_argument("--dest-type", default="csv", choices=["csv", "jsonl", "db", "memory"],
                   help="Destination format (default: csv)")
    p.add_argument("--source-name", default="source", help="Logical source name (used in registry)")
    p.add_argument("--pipeline-name", default="etl_pipeline")
    p.add_argument("--strict", action="store_true",
                   help="Strict schema mode: quarantine any drift rather than healing")
    p.add_argument("--no-coercion", action="store_true", help="Disable type coercion")
    p.add_argument("--no-backfill", action="store_true", help="Disable column backfill")
    p.add_argument("--no-evolution", action="store_true", help="Disable schema evolution")
    p.add_argument("--slack-webhook", default=None, help="Slack webhook URL for alerts")
    p.add_argument("--verbose", "-v", action="store_true")
    return p


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    if args.demo:
        from demo import main as demo_main
        demo_main()
        return

    if not args.source:
        print("Error: --source is required (or use --demo)")
        sys.exit(1)

    cfg = ETLConfig(
        pipeline_name=args.pipeline_name,
        schema_registry=SchemaRegistryConfig(strict_mode=args.strict),
        healing=HealingConfig(
            enable_type_coercion=not args.no_coercion,
            enable_column_backfill=not args.no_backfill,
            enable_schema_evolution=not args.no_evolution,
        ),
        alerts=AlertConfig(slack_webhook_url=args.slack_webhook),
    )

    summary = etl_flow(
        source_name=args.source_name,
        source_type=args.source_type,
        destination_type=args.dest_type,
        config=cfg,
        source_path=args.source,
        destination_path=args.dest,
    )

    print("\nPipeline completed:")
    for k, v in summary.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
