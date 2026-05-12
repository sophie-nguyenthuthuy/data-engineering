"""Window operator — tumbling/sliding time windows and partition-based ranking.

Three window types are supported:

  TumblingWindow(size_ms)
      Non-overlapping, fixed-size time buckets.  Every record belongs to
      exactly one bucket: bucket_id = floor(timestamp / size_ms).

  SlidingWindow(size_ms, step_ms)
      Overlapping fixed-size windows.  A record at time t belongs to all
      windows whose start is in [t - size_ms + step_ms, t] (aligned to step).

  PartitionWindow(partition_by, order_by, ...)
      Row-level window functions (ROW_NUMBER, RANK, LAG, LEAD) within a
      logical partition, ordered by specified columns.

Incremental maintenance strategy
---------------------------------
For time windows the output record includes the window boundaries.  When a
record arrives we:
  1. Determine all window(s) it belongs to.
  2. For each window: retract the old aggregate, update state, emit new aggregate.

For partition windows (ROW_NUMBER / RANK) we:
  1. Insert/remove the row from the sorted partition index.
  2. For every row whose rank changed: retract old output, emit new output.
  This is O(n) in the worst case — acceptable for a reference implementation.
"""
from __future__ import annotations

import bisect
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from ivm.aggregates import Aggregate
from ivm.operators.base import Operator
from ivm.types import Record, Timestamp, Update, freeze_record, record_key


# ---------------------------------------------------------------------------
# Window spec classes
# ---------------------------------------------------------------------------

@dataclass
class TumblingWindow:
    """Fixed-size, non-overlapping time windows."""
    size_ms: int

    def window_ids(self, ts: int) -> List[int]:
        return [ts // self.size_ms]

    def window_bounds(self, wid: int) -> Tuple[int, int]:
        return (wid * self.size_ms, (wid + 1) * self.size_ms - 1)


@dataclass
class SlidingWindow:
    """Overlapping time windows (hop windows)."""
    size_ms: int
    step_ms: int

    def window_ids(self, ts: int) -> List[int]:
        # earliest window that could contain ts starts at ts - size_ms + step_ms
        first = (ts - self.size_ms) // self.step_ms + 1
        last = ts // self.step_ms
        return list(range(first, last + 1))

    def window_bounds(self, wid: int) -> Tuple[int, int]:
        start = wid * self.step_ms
        return (start, start + self.size_ms - 1)


@dataclass
class PartitionWindow:
    """Logical window for row-level functions (ROW_NUMBER, RANK, LAG, LEAD).

    Parameters
    ----------
    partition_by : list[str]
        Columns that define the partition (like GROUP BY in SQL window syntax).
    order_by : list[tuple[str, str]]
        List of (column_name, "asc"|"desc") pairs.
    """
    partition_by: List[str]
    order_by: List[Tuple[str, str]]  # [(col, "asc"|"desc"), ...]


# ---------------------------------------------------------------------------
# Window operator
# ---------------------------------------------------------------------------

class WindowOperator(Operator):
    """
    Parameters
    ----------
    window_spec : TumblingWindow | SlidingWindow | PartitionWindow
    aggregates : dict[str, Aggregate]
        For time windows: output_col -> Aggregate instance.
    rank_fns : dict[str, str]
        For partition windows: output_col -> function name.
        Supported: "row_number", "rank", "lag:<col>:<offset>", "lead:<col>:<offset>"
    """

    def __init__(self, window_spec, aggregates: Dict[str, Aggregate],
                 rank_fns: Dict[str, str]):
        super().__init__()
        self.spec = window_spec
        self.aggregates = aggregates
        self.rank_fns = rank_fns

        # Time-window state: (window_id, frozen_record) -> diff count
        self._window_records: Dict[int, Counter] = {}
        # Time-window aggregate state: window_id -> {col: agg_state}
        self._window_agg: Dict[int, Dict[str, Any]] = {}

        # Partition-window state: partition_key -> sorted [(order_key, frozen_record)]
        self._partitions: Dict[Tuple, List[Tuple]] = {}

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def process(self, updates: List[Update]) -> List[Update]:
        if isinstance(self.spec, PartitionWindow):
            return self._process_partition(updates)
        return self._process_time_window(updates)

    # ------------------------------------------------------------------
    # Time-window maintenance
    # ------------------------------------------------------------------

    def _fresh_agg_state(self) -> Dict[str, Any]:
        return {col: agg.initial_state() for col, agg in self.aggregates.items()}

    def _build_window_record(self, wid: int, agg_state: Dict[str, Any]) -> Record:
        lo, hi = self.spec.window_bounds(wid)
        rec: Record = {"window_start": lo, "window_end": hi}
        for col, agg in self.aggregates.items():
            rec[col] = agg.result(agg_state[col])
        return rec

    def _window_is_empty(self, agg_state: Dict[str, Any]) -> bool:
        return any(agg.is_empty(agg_state[col]) for col, agg in self.aggregates.items())

    def _process_time_window(self, updates: List[Update]) -> List[Update]:
        out: List[Update] = []
        for u in updates:
            for wid in self.spec.window_ids(u.timestamp):
                # Retract old aggregate output
                if wid in self._window_agg:
                    old_rec = self._build_window_record(wid, self._window_agg[wid])
                    out.append(Update(old_rec, u.timestamp, -1))

                # Update per-record count in this window
                if wid not in self._window_records:
                    self._window_records[wid] = Counter()
                frozen = freeze_record(u.record)
                self._window_records[wid][frozen] += u.diff
                if self._window_records[wid][frozen] == 0:
                    del self._window_records[wid][frozen]

                # Update aggregate state
                state = self._window_agg.get(wid, self._fresh_agg_state())
                for col, agg in self.aggregates.items():
                    value = u.record.get(getattr(agg, "column", None), None)
                    state[col] = agg.add(state[col], value, u.diff)

                if self._window_is_empty(state):
                    self._window_agg.pop(wid, None)
                    self._window_records.pop(wid, None)
                else:
                    self._window_agg[wid] = state
                    new_rec = self._build_window_record(wid, state)
                    out.append(Update(new_rec, u.timestamp, +1))
        return out

    # ------------------------------------------------------------------
    # Partition-window maintenance (ROW_NUMBER, RANK, LAG, LEAD)
    # ------------------------------------------------------------------

    def _order_key(self, record: Record) -> Tuple:
        """Build a sort key tuple respecting ASC/DESC."""
        parts = []
        for col, direction in self.spec.order_by:
            val = record.get(col)
            # Invert numeric values for DESC; wrap in a comparable pair for None safety
            if direction.lower() == "desc":
                # Negate for numbers; wrap strings in a flag so None sorts last
                try:
                    parts.append(-val)
                except TypeError:
                    parts.append(val)
            else:
                parts.append(val)
        return tuple(parts)

    def _partition_key(self, record: Record) -> Tuple:
        return record_key(record, self.spec.partition_by)

    def _compute_rank_record(self, record: Record, position: int,
                              partition: List[Tuple]) -> Record:
        """Build the output record with window function values."""
        out = dict(record)
        for col, fn in self.rank_fns.items():
            if fn == "row_number":
                out[col] = position + 1  # 1-based
            elif fn == "rank":
                # RANK: same order key = same rank; gaps after ties
                if position == 0:
                    out[col] = 1
                else:
                    prev_ok, _ = partition[position - 1]
                    cur_ok = self._order_key(record)
                    if prev_ok == cur_ok:
                        # find the first position with this order key
                        start = position
                        while start > 0 and partition[start - 1][0] == cur_ok:
                            start -= 1
                        out[col] = start + 1
                    else:
                        out[col] = position + 1
            elif fn.startswith("lag:") or fn.startswith("lead:"):
                kind, lag_col, offset_str = fn.split(":")
                offset = int(offset_str)
                if kind == "lead":
                    target_pos = position + offset
                else:
                    target_pos = position - offset
                if 0 <= target_pos < len(partition):
                    _, target_frozen = partition[target_pos]
                    out[col] = dict(target_frozen).get(lag_col)
                else:
                    out[col] = None
        return out

    def _emit_partition_range(self, partition: List[Tuple], start: int,
                               ts: int, diff: int) -> List[Update]:
        """Emit retraction or assertion for rows [start, end) in partition."""
        out = []
        for i in range(start, len(partition)):
            ok, frozen = partition[i]
            record = dict(frozen)
            ranked = self._compute_rank_record(record, i, partition)
            out.append(Update(ranked, ts, diff))
        return out

    def _process_partition(self, updates: List[Update]) -> List[Update]:
        out: List[Update] = []

        for u in updates:
            pk = self._partition_key(u.record)
            partition = self._partitions.setdefault(pk, [])
            ok = self._order_key(u.record)
            frozen = freeze_record(u.record)
            entry = (ok, frozen)

            if u.diff > 0:
                # Find insertion position
                keys = [e[0] for e in partition]
                pos = bisect.bisect_left(keys, ok)
                # Retract all rows at and after insertion point (their ranks shift)
                out += self._emit_partition_range(partition, pos, u.timestamp, -1)
                # Insert the new row
                partition.insert(pos, entry)
                # Re-emit from insertion point with updated ranks
                out += self._emit_partition_range(partition, pos, u.timestamp, +1)
            else:
                # Retraction: find and remove the row
                try:
                    pos = partition.index(entry)
                except ValueError:
                    continue
                # Retract all rows at and after removal point
                out += self._emit_partition_range(partition, pos, u.timestamp, -1)
                partition.pop(pos)
                # Re-emit remaining rows from pos with updated ranks
                out += self._emit_partition_range(partition, pos, u.timestamp, +1)

        return out

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    def active_windows(self) -> List[Dict]:
        if isinstance(self.spec, PartitionWindow):
            return [{"partition": pk, "rows": len(rows)}
                    for pk, rows in self._partitions.items()]
        return [{"window_id": wid, **self._build_window_record(wid, state)}
                for wid, state in self._window_agg.items()]
