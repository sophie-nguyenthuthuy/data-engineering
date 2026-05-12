"""
Compaction planner: orchestrates analysis → recommendation → execution plan.

The planner is the main decision layer.  It reads TableHealth, scores the
urgency of each operation, and produces a CompactionPlan with an ordered
list of actions.  The scheduler calls the planner and then hands the plan
to the executor.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from compaction_engine.analyzer import TableHealth, QueryPatternAnalyzer
from compaction_engine.optimizer import ZOrderOptimizer, ZOrderPlan

logger = logging.getLogger(__name__)


class ActionType(str, Enum):
    COMPACT = "compact"
    ZORDER = "zorder"
    PRUNE_PARTITIONS = "prune_partitions"
    VACUUM = "vacuum"
    SKIP = "skip"


@dataclass
class PlannedAction:
    action_type: ActionType
    priority: int  # lower = higher priority
    reason: str
    estimated_benefit: str  # qualitative
    metadata: dict = field(default_factory=dict)


@dataclass
class CompactionPlan:
    table_name: str
    table_format: str
    health: TableHealth
    actions: list[PlannedAction] = field(default_factory=list)
    zorder_plan: Optional[ZOrderPlan] = None

    @property
    def ordered_actions(self) -> list[PlannedAction]:
        return sorted(self.actions, key=lambda a: a.priority)

    def summary(self) -> str:
        if not self.actions:
            return f"[{self.table_name}] No actions required"
        action_strs = ", ".join(a.action_type.value for a in self.ordered_actions)
        return f"[{self.table_name}] Planned: {action_strs}"


class CompactionPlanner:
    """
    Decides which operations to run and in what order for a given table.

    Scoring model
    -------------
    - File fragmentation > 30%  → compact (priority 1)
    - Stale partitions exist    → prune   (priority 2)
    - Z-order score > 0         → zorder  (priority 3, runs after compact)
    - Always                    → vacuum  (priority 4, lowest impact)
    """

    def __init__(
        self,
        spark,
        query_analyzer: QueryPatternAnalyzer,
        config: dict | None = None,
    ):
        self.spark = spark
        self.query_analyzer = query_analyzer
        self.config = config or {}
        self.zorder_optimizer = ZOrderOptimizer(
            spark=spark,
            query_analyzer=query_analyzer,
            max_zorder_columns=self.config.get("max_zorder_columns", 4),
            min_column_frequency=self.config.get("min_column_query_frequency", 3),
        )

    def plan(self, health: TableHealth) -> CompactionPlan:
        """Build an execution plan for the given table health snapshot."""
        cp = CompactionPlan(
            table_name=health.table_name,
            table_format=health.table_format,
            health=health,
        )

        # ---- 1. Small file compaction ----
        if health.needs_compaction:
            reason = (
                f"{health.small_files}/{health.total_files} files below target size "
                f"({health.fragmentation_ratio*100:.0f}% fragmentation)"
            )
            cp.actions.append(PlannedAction(
                action_type=ActionType.COMPACT,
                priority=1,
                reason=reason,
                estimated_benefit="high" if health.fragmentation_ratio > 0.5 else "moderate",
            ))

        # ---- 2. Partition pruning ----
        if health.needs_pruning:
            cp.actions.append(PlannedAction(
                action_type=ActionType.PRUNE_PARTITIONS,
                priority=2,
                reason=f"{health.stale_partition_count} stale partitions detected",
                estimated_benefit="moderate",
            ))

        # ---- 3. Z-ordering ----
        zorder_plan = self.zorder_optimizer.recommend(health)
        cp.zorder_plan = zorder_plan
        if zorder_plan.should_execute:
            cp.actions.append(PlannedAction(
                action_type=ActionType.ZORDER,
                priority=3,
                reason=f"Columns {zorder_plan.recommended_columns} drive most filters",
                estimated_benefit=zorder_plan.estimated_speedup,
                metadata={"sql": zorder_plan.sql_command},
            ))

        # ---- 4. Vacuum (always, lowest priority) ----
        cp.actions.append(PlannedAction(
            action_type=ActionType.VACUUM,
            priority=4,
            reason="Routine storage cleanup",
            estimated_benefit="low",
        ))

        if not cp.actions or all(a.action_type == ActionType.VACUUM for a in cp.actions):
            cp.actions = [PlannedAction(
                action_type=ActionType.SKIP,
                priority=99,
                reason="Table is healthy — no compaction needed",
                estimated_benefit="none",
            )]

        logger.info(cp.summary())
        return cp
