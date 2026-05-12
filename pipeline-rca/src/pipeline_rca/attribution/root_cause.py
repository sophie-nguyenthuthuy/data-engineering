"""
Root cause attribution engine.

Orchestrates the full pipeline:
  MetricDegradation
       │
       ▼
  LineageTracer      ← which upstream tables/columns are involved?
       │
       ▼
  SchemaStore        ← what changed recently in those tables?
       │
       ▼
  ITSAnalyzer        ← does each change causally explain the degradation?
       │
       ▼
  RootCauseReport    ← ranked, human-readable results
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta

from pipeline_rca.analysis.causal_impact import ITSAnalyzer, rank_candidates
from pipeline_rca.lineage.tracer import LineageTracer
from pipeline_rca.models import (
    CausalEstimate,
    MetricDegradation,
    RootCauseReport,
    SchemaChange,
)
from pipeline_rca.monitors.schema_monitor import SchemaStore

logger = logging.getLogger(__name__)


class RootCauseAttributor:
    """
    High-level orchestrator that ties together all sub-systems.

    Parameters
    ----------
    tracer : LineageTracer
    schema_store : SchemaStore
    look_back_days : int
        How far back in time to search for candidate changes.
    confidence_level : float
        Passed through to ITSAnalyzer.
    min_effect_size : float
        Changes with |relative effect| below this threshold are treated as
        non-causal even if statistically significant.
    top_k : int
        Maximum number of top causes to highlight in the report.
    """

    def __init__(
        self,
        tracer: LineageTracer,
        schema_store: SchemaStore,
        look_back_days: int = 7,
        confidence_level: float = 0.95,
        min_effect_size: float = 0.05,
        top_k: int = 5,
    ) -> None:
        self.tracer = tracer
        self.schema_store = schema_store
        self.look_back_days = look_back_days
        self.min_effect_size = min_effect_size
        self.top_k = top_k
        self._its = ITSAnalyzer(
            confidence_level=confidence_level, min_pre_periods=7
        )

    def attribute(self, degradation: MetricDegradation) -> RootCauseReport:
        """
        Run full attribution for a detected metric degradation.

        1. Find upstream tables via lineage.
        2. Retrieve recent changes from the schema store.
        3. Run ITS for each change as a candidate intervention.
        4. If no schema changes found, synthesise a generic "unknown upstream"
           candidate so the report is never empty.
        5. Return a ranked RootCauseReport.
        """
        incident_id = str(uuid.uuid4())[:8].upper()
        logger.info(
            "Starting RCA for %s (incident %s)", degradation.metric_name, incident_id
        )

        upstream_tables = self.tracer.graph.upstream_tables(degradation.metric_name)
        logger.info("Upstream tables: %s", upstream_tables)

        since = degradation.detected_at - timedelta(days=self.look_back_days)
        changes: list[SchemaChange] = []
        if upstream_tables:
            changes = self.schema_store.get_recent_changes(upstream_tables, since=since)
        logger.info("Found %d candidate change(s) to evaluate", len(changes))

        estimates: list[CausalEstimate] = []

        if changes:
            for change in changes:
                label = _change_label(change)
                estimate = self._its.analyze(
                    metric_series=degradation.series,
                    intervention_at=change.occurred_at,
                    candidate_label=label,
                    change=change,
                )
                if estimate is None:
                    continue
                if estimate.effect_size >= self.min_effect_size:
                    estimates.append(estimate)

        # If nothing passed the effect size filter, add a catch-all unknown candidate
        if not estimates:
            estimates = [_unknown_candidate(degradation, upstream_tables)]

        ranked = rank_candidates(estimates)
        top_causes = [e for e in ranked if e.is_significant][: self.top_k]
        if not top_causes:
            top_causes = ranked[: self.top_k]

        report = RootCauseReport(
            incident_id=incident_id,
            degradation=degradation,
            top_causes=top_causes,
            all_candidates=ranked,
        )
        logger.info(
            "RCA complete for incident %s: %d significant cause(s) found",
            incident_id,
            len(top_causes),
        )
        return report


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _change_label(change: SchemaChange) -> str:
    if change.column:
        return f"{change.table}.{change.column} [{change.kind.value}]"
    return f"{change.table} [{change.kind.value}]"


def _unknown_candidate(
    degradation: MetricDegradation, upstream_tables: list[str]
) -> CausalEstimate:
    """Synthetic candidate when no schema changes were logged."""
    tables_str = ", ".join(upstream_tables) if upstream_tables else "unknown"
    return CausalEstimate(
        candidate=f"unknown upstream change (tables: {tables_str})",
        change=None,
        effect_size=abs(degradation.relative_change),
        absolute_effect=degradation.observed_value - degradation.baseline_value,
        p_value=float("nan"),
        confidence_interval=(float("nan"), float("nan")),
        is_significant=False,
        counterfactual=[],
    )
