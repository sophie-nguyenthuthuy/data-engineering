"""
Background compaction service entry point.

Usage:
    python scripts/run_service.py --config config/default_config.yaml \
        --tables db.events:delta:spark-warehouse/events \
                 db.sales:delta:spark-warehouse/sales

Or use the installed CLI:
    compaction-engine --config config/default_config.yaml --tables db.events:delta
"""

import argparse
import logging
import signal
import sys
import time
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("compaction_service")


def parse_table_spec(spec: str):
    """Parse 'table_name:format:optional_path' into a TableRegistration."""
    from compaction_engine.scheduler import TableRegistration
    parts = spec.split(":")
    if len(parts) < 2:
        raise ValueError(f"Invalid table spec '{spec}' — expected name:format[:path]")
    return TableRegistration(
        table_name=parts[0],
        table_format=parts[1],
        table_path=parts[2] if len(parts) > 2 else None,
    )


def build_spark(config: dict):
    from pyspark.sql import SparkSession
    builder = SparkSession.builder.appName(
        config.get("spark", {}).get("app_name", "IntelligentCompactionEngine")
    )
    delta_conf = config.get("spark", {}).get("delta", {})
    for key, val in delta_conf.items():
        builder = builder.config(key, val)
    return builder.getOrCreate()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Intelligent Compaction & Pruning Service")
    parser.add_argument("--config", default="config/default_config.yaml", help="Config YAML path")
    parser.add_argument(
        "--tables", nargs="+", required=True,
        metavar="name:format[:path]",
        help="Tables to manage. e.g. db.events:delta:spark-warehouse/events",
    )
    parser.add_argument("--run-now", action="store_true", help="Run immediately and exit")
    parser.add_argument("--dry-run", action="store_true", help="Plan but do not execute")
    args = parser.parse_args(argv)

    with open(args.config) as f:
        config = yaml.safe_load(f)

    engine_cfg = config.get("engine", {})
    scheduler_cfg = config.get("scheduler", {})
    merged_config = {**engine_cfg, **scheduler_cfg, **config.get("metrics", {})}

    spark = build_spark(config)

    from compaction_engine.scheduler import CompactionScheduler
    scheduler = CompactionScheduler(spark, merged_config)

    for spec in args.tables:
        reg = parse_table_spec(spec)
        scheduler.register_table(reg)

    if args.run_now:
        logger.info("Running immediate compaction for all tables (dry_run=%s)", args.dry_run)
        results = scheduler.run_all_now(dry_run=args.dry_run)
        for r in results:
            logger.info("Result: %s", r)
        spark.stop()
        return

    # Register graceful shutdown
    def _shutdown(sig, frame):
        logger.info("Received signal %s — stopping scheduler", sig)
        scheduler.stop()
        spark.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    compaction_cron = scheduler_cfg.get("compaction_cron", "0 2 * * *")
    pruning_cron = scheduler_cfg.get("pruning_cron", "0 3 * * 0")
    scheduler.start(compaction_cron=compaction_cron, pruning_cron=pruning_cron)

    logger.info("Compaction service running. Press Ctrl+C to stop.")
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
