#!/usr/bin/env python3
"""
Entry-point for the full historical migration.

Usage:
    python scripts/run_migration.py --env dev [--dry-run]
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.spark_session import get_spark
from src.ingestion.full_load import run_full_load
from src.transformation.silver_to_gold import run_gold

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Full lakehouse migration")
    parser.add_argument("--env", default="dev", choices=["dev", "staging", "prod"])
    parser.add_argument("--dry-run", action="store_true", help="Validate config without writing data")
    args = parser.parse_args()

    config_path = f"config/env.{args.env}.yaml" if args.env != "dev" else "config/env.yaml"
    logger.info("Starting full migration  env=%s  dry_run=%s", args.env, args.dry_run)

    if args.dry_run:
        logger.info("Dry-run mode: SparkSession created, no data written.")
        get_spark(config_path)
        return

    spark = get_spark(config_path)
    run_full_load(spark, config_path)
    run_gold(spark, config_path)
    logger.info("Migration complete.")


if __name__ == "__main__":
    main()
