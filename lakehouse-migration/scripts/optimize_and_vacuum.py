#!/usr/bin/env python3
"""
Delta Lake maintenance: OPTIMIZE (Z-ORDER) + VACUUM.
Run on a schedule (e.g. nightly) to keep file sizes healthy and remove stale data.

Usage:
    python scripts/optimize_and_vacuum.py --env dev --layer silver
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.spark_session import get_spark
from delta import DeltaTable
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger(__name__)


def optimize_table(spark, path: str, z_order_cols: list[str]) -> None:
    dt = DeltaTable.forPath(spark, path)
    if z_order_cols:
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({', '.join(z_order_cols)})")
    else:
        spark.sql(f"OPTIMIZE delta.`{path}`")
    logger.info("OPTIMIZE complete: %s", path)


def vacuum_table(spark, path: str, retention_hours: int = 168) -> None:
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")
    logger.info("VACUUM complete: %s  retention=%dh", path, retention_hours)


LAYER_TABLES = {
    "bronze": [
        ("transactions", []),
        ("customers", []),
        ("products", []),
    ],
    "silver": [
        ("transactions", ["customer_id", "event_date"]),
        ("customers", ["customer_id"]),
        ("products", ["category"]),
    ],
    "gold": [
        ("fact_transactions", ["event_date"]),
        ("daily_revenue", ["event_date"]),
    ],
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev")
    parser.add_argument("--layer", default="silver", choices=["bronze", "silver", "gold", "all"])
    parser.add_argument("--vacuum-retention-hours", type=int, default=168)
    args = parser.parse_args()

    with open("config/env.yaml") as f:
        cfg = yaml.safe_load(f)

    spark = get_spark()
    layers = list(LAYER_TABLES.keys()) if args.layer == "all" else [args.layer]

    for layer in layers:
        base = cfg["layers"][layer]
        for table_name, z_cols in LAYER_TABLES[layer]:
            path = f"{base}/{table_name}"
            optimize_table(spark, path, z_cols)
            vacuum_table(spark, path, args.vacuum_retention_hours)


if __name__ == "__main__":
    main()
