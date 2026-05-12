"""
Nightly drift detection scheduler.

Runs at 02:00 UTC every day (configurable via DRIFT_CRON env var).
On demand, can also be triggered via: python -m scheduler.nightly_job --run-now
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from feature_store.drift_detector import DriftDetector
from feature_store.offline_store import OfflineStore
from feature_store.online_store import OnlineStore
from feature_store.registry import FeatureType
from feature_store.retraining_trigger import RetrainingTrigger
from feature_store.transformations import build_registry

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DRIFT_CRON = os.getenv("DRIFT_CRON", "0 2 * * *")  # 02:00 UTC daily


def run_drift_check() -> None:
    logger.info("=== Nightly drift check starting ===")

    registry = build_registry()
    online_store = OnlineStore()
    offline_store = OfflineStore()
    detector = DriftDetector()
    trigger = RetrainingTrigger()

    # 1. Load training distribution from most recent training snapshot
    training_df = offline_store.read_partition("latest")
    if training_df.empty:
        # Fall back to any available training snapshot
        for df in offline_store.iter_recent(n_partitions=7):
            if not df.empty:
                training_df = df
                break

    if training_df.empty:
        logger.warning("No training data found — skipping drift check.")
        return

    logger.info("Training snapshot: %d rows, %d features", len(training_df), len(training_df.columns))

    # 2. Fetch production distributions from Redis ring-buffer
    feature_types: dict[str, FeatureType] = {
        f.name: f.feature_type for f in registry.all_features()
    }
    production_values: dict[str, list] = {}
    for feat_name in feature_types:
        vals = online_store.get_recent_values(feat_name)
        production_values[feat_name] = vals
        logger.info("  %s: %d production samples", feat_name, len(vals))

    # 3. Run statistical comparison
    report = detector.compare(training_df, production_values, feature_types)

    logger.info(
        "Drift report: %d/%d features drifted (score=%.2f)",
        len(report.drifted_features),
        len(report.feature_results),
        report.overall_drift_score,
    )

    for result in report.feature_results:
        status = "DRIFTED" if result.drifted else "ok"
        logger.info("  [%s] %s — %s", status, result.feature_name, result.details)

    # 4. Persist report to Redis
    online_store.set_drift_report(report.to_dict())

    # 5. Trigger retraining if drift exceeds threshold
    fired = trigger.trigger(report)
    report.retraining_triggered = fired

    # 6. Persist updated report
    online_store.set_drift_report(report.to_dict())

    logger.info("=== Nightly drift check complete ===")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-now", action="store_true", help="Run once immediately and exit")
    args = parser.parse_args()

    if args.run_now:
        run_drift_check()
        return

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run_drift_check,
        trigger=CronTrigger.from_crontab(DRIFT_CRON),
        id="nightly_drift_check",
        name="Nightly Feature Drift Detection",
        misfire_grace_time=3600,
    )

    logger.info("Scheduler started — drift check cron: %s", DRIFT_CRON)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    main()
