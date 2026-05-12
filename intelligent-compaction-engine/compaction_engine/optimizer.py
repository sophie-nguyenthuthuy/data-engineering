"""
Z-order and clustering optimizer.

Analyzes column scores from QueryPatternAnalyzer and issues OPTIMIZE ZORDER
commands (Delta) or table rewrites with sort orders (Iceberg) at the right
granularity to minimize data skipping overhead without full rewrites.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from compaction_engine.analyzer import TableHealth, QueryPatternAnalyzer

logger = logging.getLogger(__name__)


@dataclass
class ZOrderPlan:
    table_name: str
    table_format: str
    recommended_columns: list[str]
    current_columns: list[str]
    score_delta: float  # how much better the new ordering is vs current
    estimated_speedup: str  # qualitative: "low" | "moderate" | "high"
    sql_command: str

    @property
    def should_execute(self) -> bool:
        return bool(self.recommended_columns) and self.score_delta > 0


class ZOrderOptimizer:
    """
    Recommends and executes Z-ordering for Delta Lake and sort-order
    rewrites for Iceberg, guided by query pattern scores.
    """

    def __init__(
        self,
        spark,
        query_analyzer: QueryPatternAnalyzer,
        max_zorder_columns: int = 4,
        min_column_frequency: int = 3,
    ):
        self.spark = spark
        self.query_analyzer = query_analyzer
        self.max_zorder_columns = max_zorder_columns
        self.min_column_frequency = min_column_frequency

    # ------------------------------------------------------------------
    # Planning
    # ------------------------------------------------------------------

    def recommend(
        self, health: TableHealth, lookback_days: int = 30
    ) -> ZOrderPlan:
        """Generate a Z-order plan without executing it."""
        top_cols = self.query_analyzer.top_zorder_columns(
            health.table_name,
            max_cols=self.max_zorder_columns,
            min_frequency=self.min_column_frequency,
            lookback_days=lookback_days,
        )

        current_cols = self._infer_current_zorder(health)
        score_delta = self._score_delta(top_cols, current_cols, health)
        speedup = self._estimate_speedup(health, top_cols)
        sql = self._build_sql(health, top_cols)

        plan = ZOrderPlan(
            table_name=health.table_name,
            table_format=health.table_format,
            recommended_columns=top_cols,
            current_columns=current_cols,
            score_delta=score_delta,
            estimated_speedup=speedup,
            sql_command=sql,
        )
        logger.info(
            "Z-order plan for %s: cols=%s speedup=%s execute=%s",
            health.table_name, top_cols, speedup, plan.should_execute,
        )
        return plan

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def execute(self, plan: ZOrderPlan, dry_run: bool = False) -> dict:
        """Execute the Z-order plan; returns timing and row-group stats."""
        if not plan.should_execute:
            logger.info("Skipping Z-order for %s — no benefit detected", plan.table_name)
            return {"skipped": True, "reason": "no_benefit"}

        logger.info("Executing Z-order: %s", plan.sql_command)
        if dry_run:
            return {"dry_run": True, "sql": plan.sql_command}

        import time
        start = time.perf_counter()
        result_df = self.spark.sql(plan.sql_command)
        result = result_df.collect()
        elapsed = time.perf_counter() - start

        stats = {"elapsed_seconds": round(elapsed, 2), "sql": plan.sql_command}
        if result:
            row = result[0].asDict()
            stats.update(row)

        logger.info("Z-order completed in %.1fs: %s", elapsed, stats)
        return stats

    def recommend_and_execute(
        self, health: TableHealth, dry_run: bool = False
    ) -> dict:
        plan = self.recommend(health)
        return self.execute(plan, dry_run=dry_run)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _infer_current_zorder(self, health: TableHealth) -> list[str]:
        """Try to read the last OPTIMIZE ZORDER columns from Delta history."""
        if health.table_format != "delta":
            return []
        try:
            history = self.spark.sql(
                f"DESCRIBE HISTORY delta.`{health.table_name}`"
            ).filter("operation = 'OPTIMIZE'")
            if history.count() == 0:
                return []
            last = history.orderBy("version", ascending=False).first()
            params = last["operationParameters"] or {}
            zorder_str = params.get("zOrderBy", "[]")
            import json
            return json.loads(zorder_str)
        except Exception:
            return []

    def _score_delta(
        self,
        new_cols: list[str],
        current_cols: list[str],
        health: TableHealth,
    ) -> float:
        """Score improvement = sum of new column scores minus current column scores."""
        scores = health.column_scores
        new_score = sum(scores[c].total_score for c in new_cols if c in scores)
        current_score = sum(scores[c].total_score for c in current_cols if c in scores)
        return new_score - current_score

    def _estimate_speedup(self, health: TableHealth, cols: list[str]) -> str:
        if not cols:
            return "none"
        scores = health.column_scores
        top_score = max((scores[c].total_score for c in cols if c in scores), default=0)
        if top_score >= 20:
            return "high"
        elif top_score >= 8:
            return "moderate"
        else:
            return "low"

    def _build_sql(self, health: TableHealth, cols: list[str]) -> str:
        if not cols:
            return ""

        if health.table_format == "delta":
            cols_str = ", ".join(cols)
            return f"OPTIMIZE delta.`{health.table_name}` ZORDER BY ({cols_str})"

        elif health.table_format == "iceberg":
            # Iceberg uses REWRITE DATA FILES with a sort order strategy
            cols_str = ", ".join(cols)
            return (
                f"CALL spark_catalog.system.rewrite_data_files("
                f"table => '{health.table_name}', "
                f"strategy => 'sort', "
                f"sort_order => '{cols_str}')"
            )

        return ""
