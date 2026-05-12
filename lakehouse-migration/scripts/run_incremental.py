#!/usr/bin/env python3
"""
Entry-point for watermark-based incremental ingestion.

Usage:
    python scripts/run_incremental.py --env dev --table transactions
    python scripts/run_incremental.py --env dev  # runs all tables
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.spark_session import get_spark
from src.ingestion.incremental import run_incremental
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Incremental lakehouse ingestion")
    parser.add_argument("--env", default="dev")
    parser.add_argument("--table", default=None, help="Run only this table (omit for all)")
    args = parser.parse_args()

    config_path = "config/env.yaml"
    spark = get_spark(config_path)

    if args.table:
        with open(config_path) as f:
            cfg = yaml.safe_load(f)
        cfg["tables"] = [t for t in cfg["tables"] if t["name"] == args.table]
        if not cfg["tables"]:
            logger.error("Table '%s' not found in config.", args.table)
            sys.exit(1)
        import tempfile, json
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
        import yaml as _yaml
        _yaml.dump(cfg, tmp)
        tmp.close()
        run_incremental(spark, tmp.name)
    else:
        run_incremental(spark, config_path)

    logger.info("Incremental run complete.")


if __name__ == "__main__":
    main()
