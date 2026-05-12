"""
Alert engine — evaluates risk metrics and produces structured alerts.

Alert severity levels:
  CRITICAL  — immediate systemic risk (large cycle + high contagion spread)
  HIGH      — significant risk (systemic node detected, concentrated market)
  MEDIUM    — early warning (short cycle, moderate concentration)
  INFO      — informational (new institution, routine topology change)
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable

from src.algorithms.cycle_detection import Cycle
from src.algorithms.centrality import NodeMetrics, ConcentrationMetrics
from src.algorithms.contagion import ContagionResult

log = logging.getLogger(__name__)


class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    INFO = "INFO"


@dataclass
class Alert:
    alert_id: str
    severity: Severity
    category: str          # CYCLE | CONCENTRATION | SYSTEMIC_NODE | CONTAGION
    title: str
    description: str
    affected_nodes: list[str]
    metrics: dict
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "alert_id": self.alert_id,
            "severity": self.severity.value,
            "category": self.category,
            "title": self.title,
            "description": self.description,
            "affected_nodes": self.affected_nodes,
            "metrics": self.metrics,
            "timestamp": self.timestamp,
        }


AlertCallback = Callable[[Alert], None]

_alert_counter = 0


def _next_id(category: str) -> str:
    global _alert_counter
    _alert_counter += 1
    return f"{category}-{int(time.time())}-{_alert_counter:04d}"


class AlertEngine:
    def __init__(self):
        self._callbacks: list[AlertCallback] = []
        self._recent: list[Alert] = []
        self._max_recent = 200

    def subscribe(self, cb: AlertCallback) -> None:
        self._callbacks.append(cb)

    def _emit(self, alert: Alert) -> None:
        self._recent.append(alert)
        if len(self._recent) > self._max_recent:
            self._recent.pop(0)
        for cb in self._callbacks:
            try:
                cb(alert)
            except Exception as exc:
                log.error("Alert callback error: %s", exc)

    def recent(self, limit: int = 50) -> list[dict]:
        return [a.to_dict() for a in self._recent[-limit:]]

    # ------------------------------------------------------------------ #
    # Evaluators
    # ------------------------------------------------------------------ #

    def evaluate_cycles(self, cycles: list[Cycle]) -> None:
        for cycle in cycles:
            if cycle.risk_score >= 0.5:
                severity = Severity.CRITICAL
            elif cycle.risk_score >= 0.2:
                severity = Severity.HIGH
            else:
                severity = Severity.MEDIUM

            chain = " → ".join(cycle.nodes + [cycle.nodes[0]])
            self._emit(Alert(
                alert_id=_next_id("CYCLE"),
                severity=severity,
                category="CYCLE",
                title=f"Circular dependency detected ({len(cycle.nodes)}-node cycle)",
                description=(
                    f"Circular lending chain: {chain}. "
                    f"Total exposure ${cycle.total_exposure:,.0f}M. "
                    f"Bottleneck edge ${cycle.min_edge:,.0f}M."
                ),
                affected_nodes=cycle.nodes,
                metrics={
                    "total_exposure": cycle.total_exposure,
                    "min_edge": cycle.min_edge,
                    "risk_score": cycle.risk_score,
                    "length": len(cycle.nodes),
                },
            ))

    def evaluate_concentration(self, conc: ConcentrationMetrics) -> None:
        if not conc.is_concentrated:
            return
        severity = Severity.CRITICAL if conc.hhi > 0.4 else Severity.HIGH
        self._emit(Alert(
            alert_id=_next_id("CONCENTRATION"),
            severity=severity,
            category="CONCENTRATION",
            title=f"Liquidity concentration risk (HHI={conc.hhi:.3f})",
            description=(
                f"Interbank lending market is concentrated. "
                f"HHI={conc.hhi:.3f} (threshold {0.25}), "
                f"Gini={conc.gini:.3f}. "
                f"Dominant nodes: {', '.join(conc.top_nodes)}."
            ),
            affected_nodes=conc.top_nodes,
            metrics={"hhi": conc.hhi, "gini": conc.gini},
        ))

    def evaluate_systemic_nodes(self, node_metrics: list[NodeMetrics]) -> None:
        for nm in node_metrics:
            if not nm.is_systemic:
                continue
            severity = Severity.CRITICAL if nm.betweenness > 0.6 else Severity.HIGH
            self._emit(Alert(
                alert_id=_next_id("SYSTEMIC_NODE"),
                severity=severity,
                category="SYSTEMIC_NODE",
                title=f"Systemically important node: {nm.node_id}",
                description=(
                    f"{nm.node_id} has betweenness centrality {nm.betweenness:.2%} "
                    f"and PageRank {nm.pagerank:.4f}. "
                    f"Net exposure ${nm.net_exposure:,.0f}M. "
                    f"Failure would fragment the network."
                ),
                affected_nodes=[nm.node_id],
                metrics={
                    "betweenness": nm.betweenness,
                    "pagerank": nm.pagerank,
                    "net_exposure": nm.net_exposure,
                    "degree_in": nm.degree_in,
                    "degree_out": nm.degree_out,
                },
            ))

    def evaluate_contagion(self, result: ContagionResult) -> None:
        if result.fraction_failed < 0.10:
            return
        if result.fraction_failed >= 0.40:
            severity = Severity.CRITICAL
        elif result.fraction_failed >= 0.20:
            severity = Severity.HIGH
        else:
            severity = Severity.MEDIUM

        self._emit(Alert(
            alert_id=_next_id("CONTAGION"),
            severity=severity,
            category="CONTAGION",
            title=f"Contagion cascade from {result.seed_node} ({result.fraction_failed:.0%} of network)",
            description=(
                f"Simulated failure of {result.seed_node} causes cascade affecting "
                f"{len(result.failed_nodes)} additional institutions "
                f"({result.fraction_failed:.1%} of network) over {result.cascade_depth} hops. "
                f"Total exposure lost: ${result.total_exposure_lost:,.0f}M."
            ),
            affected_nodes=[result.seed_node] + result.failed_nodes,
            metrics={
                "seed": result.seed_node,
                "failed_count": len(result.failed_nodes),
                "fraction_failed": result.fraction_failed,
                "cascade_depth": result.cascade_depth,
                "exposure_lost": result.total_exposure_lost,
            },
        ))
