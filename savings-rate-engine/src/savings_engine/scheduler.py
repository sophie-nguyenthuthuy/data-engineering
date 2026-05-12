"""
APScheduler-based background scheduler.

Usage (standalone):
    python -m savings_engine.scheduler          # uses SCRAPE_INTERVAL_HOURS from .env
    sre schedule --interval 6

The scheduler also exposes start_scheduler() so the CLI command can call it.
"""
import logging
import signal
import sys
from typing import Optional

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from savings_engine.config import settings
from savings_engine.pipeline import run_pipeline

logger = logging.getLogger(__name__)


def _job(bank_codes: Optional[list[str]] = None) -> None:
    logger.info("Scheduler triggered scrape run")
    try:
        run_pipeline(bank_codes)
    except Exception as exc:
        logger.exception("Unhandled error in scheduled pipeline run: %s", exc)


def start_scheduler(
    interval_hours: int = settings.scrape_interval_hours,
    bank_codes: Optional[list[str]] = None,
) -> None:
    """Start the blocking scheduler.  Runs the pipeline once immediately, then on interval."""
    scheduler = BlockingScheduler(timezone="Asia/Ho_Chi_Minh")

    scheduler.add_job(
        _job,
        trigger=IntervalTrigger(hours=interval_hours),
        kwargs={"bank_codes": bank_codes},
        id="scrape_pipeline",
        name="Vietnamese bank rate scraper",
        next_run_time=__import__("datetime").datetime.now(),  # run immediately
        misfire_grace_time=300,
        coalesce=True,
    )

    def _shutdown(signum, frame):
        logger.info("Signal %s received — shutting down scheduler", signum)
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    logger.info("Scheduler started — interval: %dh", interval_hours)
    scheduler.start()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    start_scheduler()
