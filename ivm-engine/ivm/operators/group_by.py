"""GROUP BY operator with incremental aggregate maintenance.

The core insight: when a row arrives with diff d, we:
  1. Emit a retraction of the *current* group output (if the group existed).
  2. Update the aggregate state.
  3. Emit an assertion of the *new* group output (if the group is non-empty).

This produces the correct delta to the view without recomputing from scratch.

Supports: Count, Sum, Avg, Min, Max, CountDistinct (see ivm/aggregates.py).
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from ivm.aggregates import Aggregate
from ivm.operators.base import Operator
from ivm.types import Record, Update, record_key


class GroupByOperator(Operator):
    """
    Parameters
    ----------
    key_columns : list of str
        Columns that form the group key (SELECT … GROUP BY these).
    aggregates : dict[str, Aggregate]
        Maps output column name → Aggregate instance.
        Example: {"revenue": Sum("amount"), "orders": Count()}
    """

    def __init__(self, key_columns: List[str], aggregates: Dict[str, Aggregate]):
        super().__init__()
        self.key_columns = key_columns
        self.aggregates = aggregates  # output_col -> Aggregate

        # group_key -> {agg_col: state}
        self._state: Dict[Tuple, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fresh_state(self) -> Dict[str, Any]:
        return {col: agg.initial_state() for col, agg in self.aggregates.items()}

    def _build_record(self, key: Tuple, agg_state: Dict[str, Any]) -> Record:
        rec = {col: val for col, val in zip(self.key_columns, key)}
        for col, agg in self.aggregates.items():
            rec[col] = agg.result(agg_state[col])
        return rec

    def _is_empty(self, agg_state: Dict[str, Any]) -> bool:
        return any(
            agg.is_empty(agg_state[col])
            for col, agg in self.aggregates.items()
        )

    # ------------------------------------------------------------------
    # Core processing
    # ------------------------------------------------------------------

    def process(self, updates: List[Update]) -> List[Update]:
        out: List[Update] = []

        for u in updates:
            key = record_key(u.record, self.key_columns)
            state = self._state.get(key)

            # Step 1 — retract the current output for this group (if any)
            if state is not None:
                old_rec = self._build_record(key, state)
                out.append(Update(old_rec, u.timestamp, -1))

            # Step 2 — update aggregate state
            if state is None:
                state = self._fresh_state()

            new_state: Dict[str, Any] = {}
            for col, agg in self.aggregates.items():
                # Each aggregate decides what "value" to extract from the record.
                # For Count, value is ignored.  For Sum/Min/Max, it's the column.
                value = u.record.get(getattr(agg, "column", None), None)
                new_state[col] = agg.add(state[col], value, u.diff)

            # Step 3 — store new state and emit new output
            if self._is_empty(new_state):
                self._state.pop(key, None)
            else:
                self._state[key] = new_state
                new_rec = self._build_record(key, new_state)
                out.append(Update(new_rec, u.timestamp, +1))

        return out

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    def current_groups(self) -> List[Record]:
        """Snapshot of all active groups and their aggregate values."""
        return [
            self._build_record(key, state)
            for key, state in self._state.items()
        ]
