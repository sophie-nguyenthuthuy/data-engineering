#!/usr/bin/env python3
"""Run the correctness validator and print the comparison report."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

parser = argparse.ArgumentParser(description="Run correctness validation")
parser.add_argument("--local", action="store_true", help="Use local file mode (no Kafka)")
parser.add_argument("--output", type=Path, default=None, help="Write JSON report to this path")
parser.add_argument("--tolerance", type=float, default=None, help="Override relative tolerance (e.g. 0.001)")
args = parser.parse_args()

if args.local:
    os.environ["LOCAL_MODE"] = "true"

from src.config import HISTORICAL_DIR, config
from src.validator.correctness_validator import CorrectnessValidator

logging.basicConfig(
    level=getattr(logging, config.log_level),
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("validation")


def main() -> None:
    # Ensure seed data exists
    json_files = list(HISTORICAL_DIR.glob("*.json"))
    if not json_files:
        logger.info("No historical data found — running seed_data.py first...")
        from scripts.seed_data import main as seed_main
        seed_main()

    validator = CorrectnessValidator(
        local_mode=config.local_mode,
        amount_rel_tolerance=args.tolerance,
    )
    report = validator.run()
    report.print_rich()

    if args.output:
        report.save_json(args.output)

    sys.exit(0 if report.passed else 1)


if __name__ == "__main__":
    main()
