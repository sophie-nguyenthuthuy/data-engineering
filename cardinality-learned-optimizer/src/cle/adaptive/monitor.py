"""Cardinality error monitoring and 100× threshold detection.

The adaptive query processing pipeline:
  1. Execute query with EXPLAIN ANALYZE
  2. Walk the plan tree, identify nodes with q-error ≥ 100×
  3. Flag those nodes and report corrected estimates
  4. (Recompiler will re-execute with fixed hints if triggered)
"""
from __future__ import annotations
import logging
from dataclasses import dataclass
from typing import Optional

from ..plan.node import PlanNode
from ..plan.parser import extract_cardinality_errors, get_worst_node

logger = logging.getLogger(__name__)

CRITICAL_THRESHOLD = 100.0   # 100× rule from the AQP literature


@dataclass
class CardinalityAlert:
    node: PlanNode
    q_error: float
    estimated: float
    actual: float
    direction: str   # "over" or "under"

    def __str__(self) -> str:
        return (
            f"[{self.direction.upper()} {self.q_error:.0f}×] "
            f"{self.node.node_type} "
            f"(rel={self.node.relation_name or '?'}) "
            f"est={self.estimated:.0f} act={self.actual:.0f}"
        )


@dataclass
class MonitorReport:
    alerts: list[CardinalityAlert]
    needs_replan: bool
    worst_q_error: float
    total_nodes: int
    affected_nodes: int

    def summary(self) -> str:
        if not self.needs_replan:
            return f"OK — worst q-error={self.worst_q_error:.1f}× ({self.total_nodes} nodes)"
        return (
            f"REPLAN NEEDED — {self.affected_nodes}/{self.total_nodes} nodes "
            f"exceed {CRITICAL_THRESHOLD:.0f}×; worst={self.worst_q_error:.0f}×\n"
            + "\n".join(f"  {a}" for a in self.alerts[:5])
        )


class CardinalityMonitor:
    def __init__(self, threshold: float = CRITICAL_THRESHOLD) -> None:
        self.threshold = threshold

    def analyze(self, root: PlanNode) -> MonitorReport:
        """Inspect a post-ANALYZE plan tree and produce a monitoring report."""
        all_errors = extract_cardinality_errors(root)
        alerts = []
        worst = 1.0

        for node, qe in all_errors:
            worst = max(worst, qe)
            if qe >= self.threshold:
                act = node.actual_rows_total or 1.0
                est = node.estimated_rows
                direction = "over" if est > act else "under"
                alerts.append(CardinalityAlert(
                    node=node,
                    q_error=qe,
                    estimated=est,
                    actual=act,
                    direction=direction,
                ))

        alerts.sort(key=lambda a: a.q_error, reverse=True)

        report = MonitorReport(
            alerts=alerts,
            needs_replan=len(alerts) > 0,
            worst_q_error=worst,
            total_nodes=len(all_errors),
            affected_nodes=len(alerts),
        )

        if report.needs_replan:
            logger.warning(report.summary())
        else:
            logger.debug(report.summary())

        return report

    def correction_map(self, report: MonitorReport) -> dict[int, float]:
        """Return {node_id: corrected_rows} for all alerted nodes."""
        return {a.node.node_id: a.actual for a in report.alerts}
