"""
Background scheduler: runs compaction jobs without disrupting queries.

The scheduler runs as a daemon thread and uses the `schedule` library for
cron-like job timing.  Each job runs in its own thread with a configurable
timeout so a slow compaction never blocks the next scheduled window.

Key design choices:
- Jobs are skipped (not queued) if a previous run is still active, preventing pile-up
- Delta OPTIMIZE and Iceberg rewrite are non-blocking for readers by design
- Graceful shutdown: waits for the active job to finish before exiting
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor, Future, TimeoutError as FutureTimeoutError
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional

import schedule

from compaction_engine.analyzer import TableAnalyzer, QueryPatternAnalyzer, TableHealth
from compaction_engine.compactor import FileCompactor, CompactionResult
from compaction_engine.optimizer import ZOrderOptimizer
from compaction_engine.planner import CompactionPlanner, ActionType
from compaction_engine.pruner import PartitionPruner
from compaction_engine.metrics import PerformanceMetrics, BenchmarkQuery

logger = logging.getLogger(__name__)


@dataclass
class TableRegistration:
    """A table registered for automatic compaction."""
    table_name: str
    table_format: str           # "delta" | "iceberg"
    table_path: Optional[str]   # for Delta path-based tables
    benchmark_queries: list[BenchmarkQuery] = field(default_factory=list)
    compaction_enabled: bool = True
    pruning_enabled: bool = True
    zorder_enabled: bool = True


@dataclass
class JobRecord:
    table_name: str
    action: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    success: bool = False
    error: Optional[str] = None

    @property
    def duration_seconds(self) -> float:
        if not self.finished_at:
            return 0.0
        return (self.finished_at - self.started_at).total_seconds()


class CompactionScheduler:
    """
    Daemon scheduler that manages compaction, pruning, and Z-ordering
    for a fleet of registered tables.

    Usage
    -----
    scheduler = CompactionScheduler(spark, config)
    scheduler.register_table(TableRegistration(table_name="db.events", ...))
    scheduler.start()  # non-blocking; runs in background
    # ... do other work ...
    scheduler.stop()
    """

    def __init__(self, spark, config: dict | None = None):
        self.spark = spark
        self.config = config or {}

        self._tables: dict[str, TableRegistration] = {}
        self._job_history: list[JobRecord] = []
        self._active_futures: dict[str, Future] = {}
        self._lock = threading.Lock()
        self._running = False
        self._scheduler_thread: Optional[threading.Thread] = None
        self._executor = ThreadPoolExecutor(
            max_workers=self.config.get("max_concurrent_jobs", 2),
            thread_name_prefix="compaction-worker",
        )
        self._job_timeout = self.config.get("job_timeout_minutes", 120) * 60

        # Sub-components
        self.query_analyzer = QueryPatternAnalyzer(
            db_path=self.config.get("db_path", "compaction_metrics.db")
        )
        self.table_analyzer = TableAnalyzer(spark, config)
        self.compactor = FileCompactor(
            spark,
            target_file_size_mb=self.config.get("target_file_size_mb", 128),
            small_file_size_mb=self.config.get("small_file_size_mb", 32),
        )
        self.pruner = PartitionPruner(
            spark,
            stale_partition_days=self.config.get("stale_partition_days", 365),
            dry_run=self.config.get("dry_run", False),
        )
        self.metrics = PerformanceMetrics(
            spark,
            db_path=self.config.get("db_path", "compaction_metrics.db"),
            prometheus_port=self.config.get("prometheus_port"),
        )
        self.planner = CompactionPlanner(spark, self.query_analyzer, config)

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_table(self, reg: TableRegistration) -> None:
        self._tables[reg.table_name] = reg
        logger.info("Registered table %s (%s)", reg.table_name, reg.table_format)

    def unregister_table(self, table_name: str) -> None:
        self._tables.pop(table_name, None)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(
        self,
        compaction_cron: str = "0 2 * * *",
        pruning_cron: str = "0 3 * * 0",
    ) -> None:
        """Start the background scheduler (non-blocking)."""
        if self._running:
            logger.warning("Scheduler already running")
            return

        self._running = True
        self._setup_schedule(compaction_cron, pruning_cron)
        self._scheduler_thread = threading.Thread(
            target=self._run_loop, daemon=True, name="compaction-scheduler"
        )
        self._scheduler_thread.start()
        logger.info(
            "Compaction scheduler started (compaction=%s, pruning=%s)",
            compaction_cron, pruning_cron,
        )

    def stop(self, wait_seconds: int = 30) -> None:
        """Gracefully stop the scheduler."""
        self._running = False
        schedule.clear()
        self._executor.shutdown(wait=True, cancel_futures=False)
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=wait_seconds)
        logger.info("Scheduler stopped")

    def run_now(self, table_name: str, dry_run: bool = False) -> dict:
        """Trigger an immediate compaction run for a specific table."""
        reg = self._tables.get(table_name)
        if not reg:
            raise ValueError(f"Table '{table_name}' not registered")
        return self._run_compaction_job(reg, dry_run=dry_run)

    def run_all_now(self, dry_run: bool = False) -> list[dict]:
        """Trigger immediate compaction for all registered tables."""
        results = []
        for reg in self._tables.values():
            try:
                result = self._run_compaction_job(reg, dry_run=dry_run)
                results.append(result)
            except Exception as e:
                logger.error("Job failed for %s: %s", reg.table_name, e)
                results.append({"table": reg.table_name, "error": str(e)})
        return results

    @property
    def job_history(self) -> list[JobRecord]:
        return list(self._job_history)

    # ------------------------------------------------------------------
    # Scheduler internals
    # ------------------------------------------------------------------

    def _setup_schedule(self, compaction_cron: str, pruning_cron: str) -> None:
        # schedule library uses human-readable API; map cron hour to schedule
        compaction_hour = self._cron_to_hour(compaction_cron)
        pruning_hour = self._cron_to_hour(pruning_cron)

        schedule.every().day.at(f"{compaction_hour:02d}:00").do(
            self._scheduled_compaction
        )
        schedule.every().sunday.at(f"{pruning_hour:02d}:00").do(
            self._scheduled_pruning
        )

    def _cron_to_hour(self, cron: str) -> int:
        """Parse the hour field from a simple cron expression."""
        try:
            parts = cron.strip().split()
            return int(parts[1]) if len(parts) >= 2 else 2
        except (ValueError, IndexError):
            return 2

    def _run_loop(self) -> None:
        while self._running:
            schedule.run_pending()
            time.sleep(30)

    def _scheduled_compaction(self) -> None:
        logger.info("Scheduled compaction starting for %d tables", len(self._tables))
        for reg in list(self._tables.values()):
            if not reg.compaction_enabled:
                continue
            with self._lock:
                if reg.table_name in self._active_futures:
                    fut = self._active_futures[reg.table_name]
                    if not fut.done():
                        logger.warning(
                            "Skipping %s — previous job still running", reg.table_name
                        )
                        continue

            fut = self._executor.submit(self._run_compaction_job, reg)
            with self._lock:
                self._active_futures[reg.table_name] = fut

    def _scheduled_pruning(self) -> None:
        logger.info("Scheduled pruning starting for %d tables", len(self._tables))
        for reg in list(self._tables.values()):
            if not reg.pruning_enabled:
                continue
            self._executor.submit(self._run_pruning_job, reg)

    # ------------------------------------------------------------------
    # Job executors
    # ------------------------------------------------------------------

    def _run_compaction_job(
        self, reg: TableRegistration, dry_run: bool = False
    ) -> dict:
        record = JobRecord(
            table_name=reg.table_name,
            action="compaction",
            started_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )
        result: dict = {"table": reg.table_name}

        try:
            # --- Analyze ---
            if reg.table_format == "delta":
                health = self.table_analyzer.analyze_delta_table(
                    reg.table_path or reg.table_name, self.query_analyzer
                )
            else:
                health = self.table_analyzer.analyze_iceberg_table(
                    reg.table_name, self.query_analyzer
                )

            plan = self.planner.plan(health)
            result["plan"] = plan.summary()

            # --- Benchmark before ---
            if reg.benchmark_queries:
                before = self.metrics.run_benchmark(
                    reg.table_name,
                    reg.benchmark_queries,
                    phase="before",
                    file_count=health.total_files,
                    avg_file_size_mb=health.avg_file_size_mb,
                    total_size_gb=health.total_size_gb,
                )

            # --- Execute planned actions ---
            for action in plan.ordered_actions:
                if action.action_type == ActionType.COMPACT and reg.compaction_enabled:
                    compact_result = self.compactor.compact(health, dry_run=dry_run)
                    result["compaction"] = compact_result.summary()

                elif action.action_type == ActionType.ZORDER and reg.zorder_enabled:
                    if plan.zorder_plan:
                        zorder_result = self.planner.zorder_optimizer.execute(
                            plan.zorder_plan, dry_run=dry_run
                        )
                        result["zorder"] = zorder_result

                elif action.action_type == ActionType.VACUUM:
                    vacuum_result = self.pruner.vacuum(
                        reg.table_name, reg.table_format, reg.table_path
                    )
                    result["vacuum"] = vacuum_result

            # --- Benchmark after ---
            if reg.benchmark_queries:
                after_health = self.table_analyzer.analyze_delta_table(
                    reg.table_path or reg.table_name
                ) if reg.table_format == "delta" else health

                after = self.metrics.run_benchmark(
                    reg.table_name,
                    reg.benchmark_queries,
                    phase="after",
                    file_count=after_health.total_files,
                    avg_file_size_mb=after_health.avg_file_size_mb,
                    total_size_gb=after_health.total_size_gb,
                )
                impact = self.metrics.compare(before, after)
                result["impact_report"] = impact.report()
                logger.info(impact.report())

            record.success = True

        except Exception as e:
            logger.exception("Compaction job failed for %s", reg.table_name)
            record.error = str(e)
            result["error"] = str(e)

        finally:
            record.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
            self._job_history.append(record)
            logger.info(
                "Job [%s] finished in %.1fs success=%s",
                reg.table_name, record.duration_seconds, record.success,
            )

        return result

    def _run_pruning_job(self, reg: TableRegistration) -> dict:
        record = JobRecord(
            table_name=reg.table_name,
            action="pruning",
            started_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )
        result: dict = {"table": reg.table_name}
        try:
            if reg.table_format == "delta":
                health = self.table_analyzer.analyze_delta_table(
                    reg.table_path or reg.table_name
                )
            else:
                health = self.table_analyzer.analyze_iceberg_table(reg.table_name)

            prune_result = self.pruner.prune(health)
            result["pruning"] = prune_result.summary()
            record.success = True
        except Exception as e:
            logger.exception("Pruning job failed for %s", reg.table_name)
            record.error = str(e)
            result["error"] = str(e)
        finally:
            record.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
            self._job_history.append(record)
        return result
