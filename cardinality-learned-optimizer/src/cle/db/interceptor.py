"""Query interceptor: run queries with EXPLAIN ANALYZE and collect plan trees."""
from __future__ import annotations
import logging
import time
from dataclasses import dataclass
from typing import Optional

from .connector import ConnectionPool
from ..plan.node import PlanNode
from ..plan.parser import parse_explain_result, has_critical_error

logger = logging.getLogger(__name__)

# EXPLAIN flags we always use
_EXPLAIN_PREFIX = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON, VERBOSE)"
_EXPLAIN_DRY = "EXPLAIN (FORMAT JSON, VERBOSE)"


@dataclass
class ExecutionRecord:
    sql: str
    hint_sql: Optional[str]             # SQL with pg_hint_plan header if any
    plan_dry: PlanNode                   # plan without actuals (dry run)
    plan_analyzed: Optional[PlanNode]    # plan with actuals (after execution)
    latency_ms: float = 0.0
    hint_id: int = 0


class QueryInterceptor:
    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool

    def explain_dry(self, sql: str) -> PlanNode:
        """Get estimated plan without executing."""
        rows = self.pool.execute(f"{_EXPLAIN_DRY} {sql}")
        return parse_explain_result(rows)

    def explain_analyze(self, sql: str, timeout_ms: int = 60_000) -> tuple[PlanNode, float]:
        """Execute query with EXPLAIN ANALYZE; return (plan_tree, latency_ms)."""
        self.pool.set_timeout(timeout_ms)
        try:
            t0 = time.perf_counter()
            rows = self.pool.execute(f"{_EXPLAIN_PREFIX} {sql}")
            latency_ms = (time.perf_counter() - t0) * 1000
        finally:
            self.pool.reset_timeout()
        return parse_explain_result(rows), latency_ms

    def run_with_hints(
        self,
        sql: str,
        hints: str,
        timeout_ms: int = 60_000,
    ) -> tuple[PlanNode, float]:
        """Run query with pg_hint_plan hints block prepended."""
        hint_sql = f"/*+ {hints} */\n{sql}"
        return self.explain_analyze(hint_sql, timeout_ms)

    def intercept(
        self,
        sql: str,
        hint_id: int = 0,
        hints: Optional[str] = None,
        timeout_ms: int = 60_000,
    ) -> ExecutionRecord:
        """Full interception: dry explain → execute → return record."""
        plan_dry = self.explain_dry(sql)
        actual_sql = sql
        if hints:
            actual_sql = f"/*+ {hints} */\n{sql}"

        plan_analyzed, latency_ms = self.explain_analyze(actual_sql, timeout_ms)

        rec = ExecutionRecord(
            sql=sql,
            hint_sql=actual_sql if hints else None,
            plan_dry=plan_dry,
            plan_analyzed=plan_analyzed,
            latency_ms=latency_ms,
            hint_id=hint_id,
        )
        if has_critical_error(plan_analyzed):
            worst = [
                (n, n.q_error)
                for n in plan_analyzed.all_nodes()
                if n.q_error and n.q_error >= 100
            ]
            logger.warning(
                "Critical cardinality error in %d node(s): %s",
                len(worst),
                [(n.node_type, f"{qe:.1f}x") for n, qe in worst[:3]],
            )
        return rec
