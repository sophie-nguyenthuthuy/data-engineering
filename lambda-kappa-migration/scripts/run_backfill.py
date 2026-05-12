#!/usr/bin/env python3
"""Trigger the backfill job: historical data → Kafka replay topic."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import os

parser = argparse.ArgumentParser(description="Run the backfill job")
parser.add_argument("--local", action="store_true", help="Use local file mode instead of Kafka")
parser.add_argument("--rate", type=int, default=None, help="Events per second (0=unlimited)")
parser.add_argument("--dry-run", action="store_true", help="Count events without publishing")
args = parser.parse_args()

if args.local:
    os.environ["LOCAL_MODE"] = "true"

from src.config import config
from src.migration.backfill import BackfillJob

logging.basicConfig(
    level=getattr(logging, config.log_level),
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("backfill")


def main() -> None:
    job = BackfillJob(local_mode=config.local_mode, rate=args.rate)
    if args.dry_run:
        stats = job.dry_run()
        print(f"Dry run: {stats}")
    else:
        stats = job.run()
        print(f"Backfill complete: {stats}")


if __name__ == "__main__":
    main()
