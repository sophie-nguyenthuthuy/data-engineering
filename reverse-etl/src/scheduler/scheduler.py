from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from ..models import PipelineConfig
from ..pipeline import run_pipeline
from ..logger import get_logger

logger = get_logger(__name__)


class PipelineScheduler:
    def __init__(self) -> None:
        self._scheduler = BackgroundScheduler(timezone="UTC")

    def register(self, config: PipelineConfig) -> None:
        if not config.enabled or config.schedule is None:
            return

        sched = config.schedule

        if sched.type == "cron":
            if not sched.cron:
                raise ValueError(f"[{config.name}] cron schedule requires a 'cron' expression")
            trigger = CronTrigger.from_crontab(sched.cron, timezone="UTC")
        elif sched.type == "interval":
            if not sched.seconds:
                raise ValueError(f"[{config.name}] interval schedule requires 'seconds'")
            trigger = IntervalTrigger(seconds=sched.seconds)
        else:
            raise ValueError(f"[{config.name}] unknown schedule type: {sched.type!r}")

        self._scheduler.add_job(
            func=run_pipeline,
            trigger=trigger,
            args=[config],
            id=config.name,
            name=config.name,
            replace_existing=True,
            misfire_grace_time=300,
        )
        logger.info(f"[{config.name}] registered ({sched.type}: {sched.cron or sched.seconds})")

    def register_all(self, configs: list[PipelineConfig]) -> None:
        for config in configs:
            self.register(config)

    def start(self) -> None:
        self._scheduler.start()
        logger.info("Scheduler started")

    def stop(self) -> None:
        self._scheduler.shutdown(wait=True)
        logger.info("Scheduler stopped")

    def list_jobs(self) -> list[dict]:
        return [
            {
                "id": job.id,
                "name": job.name,
                "next_run": str(job.next_run_time),
            }
            for job in self._scheduler.get_jobs()
        ]
