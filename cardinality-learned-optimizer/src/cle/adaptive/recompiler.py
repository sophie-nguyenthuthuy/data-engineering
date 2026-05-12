"""Adaptive query recompilation.

When the CardinalityMonitor detects a ≥100× error, this module:
  1. Builds corrected cardinality hints from the actual rows observed
  2. Re-submits the query to PostgreSQL with those hints
  3. Returns the new plan + latency

This emulates "mid-execution recompilation" at the granularity of
complete query re-execution (PostgreSQL does not expose sub-plan restart
hooks without extension code; we approximate it here).

For a real implementation the PG patch from CMU's Orca/Bao repo would
intercept executor nodes via ExecutorRun hooks — that approach is
described in Section 4.2 of the Bao paper.
"""
from __future__ import annotations
import logging
from dataclasses import dataclass
from typing import Optional

from ..db.connector import ConnectionPool
from ..db.hint_injector import build_correction_hints, apply_hint_set_to_connection, reset_hint_set
from ..db.interceptor import QueryInterceptor, ExecutionRecord
from ..plan.node import PlanNode
from ..plan.parser import has_critical_error
from .monitor import CardinalityMonitor, MonitorReport

logger = logging.getLogger(__name__)


@dataclass
class RecompileResult:
    original_record: ExecutionRecord
    recompiled_record: Optional[ExecutionRecord]
    monitor_report: MonitorReport
    triggered: bool
    speedup: float   # original_ms / recompiled_ms; > 1 means recompile was faster

    def summary(self) -> str:
        if not self.triggered:
            return f"No recompile needed. Latency={self.original_record.latency_ms:.1f}ms"
        orig = self.original_record.latency_ms
        new = self.recompiled_record.latency_ms if self.recompiled_record else orig
        return (
            f"Recompiled: {orig:.1f}ms → {new:.1f}ms  "
            f"(speedup={self.speedup:.2f}×)"
        )


class AdaptiveRecompiler:
    def __init__(
        self,
        pool: ConnectionPool,
        threshold: float = 100.0,
        max_retries: int = 1,
    ) -> None:
        self.pool = pool
        self.interceptor = QueryInterceptor(pool)
        self.monitor = CardinalityMonitor(threshold)
        self.max_retries = max_retries

    def run(
        self,
        sql: str,
        hint_id: int = 0,
        base_hints: Optional[str] = None,
        timeout_ms: int = 60_000,
    ) -> RecompileResult:
        """Execute query; recompile with corrected hints if error ≥ threshold."""

        # Initial execution
        record = self.interceptor.intercept(sql, hint_id=hint_id, hints=base_hints, timeout_ms=timeout_ms)
        report = self.monitor.analyze(record.plan_analyzed)

        if not report.needs_replan:
            return RecompileResult(
                original_record=record,
                recompiled_record=None,
                monitor_report=report,
                triggered=False,
                speedup=1.0,
            )

        # Build correction hints from actual rows in the first execution
        correction_hints = build_correction_hints(record.plan_analyzed, self.monitor.threshold)
        if base_hints:
            all_hints = f"{base_hints} {correction_hints}"
        else:
            all_hints = correction_hints

        logger.info(
            "Recompiling with corrected hints (%d affected nodes): %s",
            report.affected_nodes,
            correction_hints[:200],
        )

        # Re-execute with corrected cardinality hints
        recompiled = self.interceptor.intercept(
            sql, hint_id=hint_id, hints=all_hints, timeout_ms=timeout_ms
        )

        speedup = record.latency_ms / max(recompiled.latency_ms, 0.001)

        return RecompileResult(
            original_record=record,
            recompiled_record=recompiled,
            monitor_report=report,
            triggered=True,
            speedup=speedup,
        )

    def run_batch(
        self,
        queries: list[str],
        timeout_ms: int = 60_000,
    ) -> list[RecompileResult]:
        results = []
        for sql in queries:
            try:
                r = self.run(sql, timeout_ms=timeout_ms)
                results.append(r)
                logger.info("Query result: %s", r.summary())
            except Exception as e:
                logger.error("Query failed: %s — %s", sql[:80], e)
        return results
