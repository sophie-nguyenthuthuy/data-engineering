"""IVM for window functions.

We implement ROW_NUMBER() OVER (PARTITION BY p ORDER BY t).

Naive: any insert at time t reshuffles ranks of all rows after t in the
partition. Our IVM maintains per-partition order and emits deltas only for
the affected suffix.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from bisect import insort, bisect_left


@dataclass
class RowNumberIVM:
    """Maintains ROW_NUMBER() rankings per partition under incremental insertion."""
    _partitions: dict = field(default_factory=dict)  # partition -> sorted list of (t, row_id)

    def insert(self, partition, t, row_id) -> list:
        """Insert one row. Return list of (row_id, new_rank) deltas for the
        affected suffix (the new row + all rows after it)."""
        rows = self._partitions.setdefault(partition, [])
        insort(rows, (t, row_id))
        idx = bisect_left(rows, (t, row_id))
        deltas = []
        for i in range(idx, len(rows)):
            ti, ri = rows[i]
            deltas.append((ri, i + 1))  # rank = 1-indexed position
        return deltas

    def delete(self, partition, t, row_id) -> list:
        """Delete a row. Return deltas for the suffix that shifts up by 1."""
        rows = self._partitions.get(partition, [])
        try:
            idx = rows.index((t, row_id))
        except ValueError:
            return []
        rows.pop(idx)
        deltas = []
        for i in range(idx, len(rows)):
            ti, ri = rows[i]
            deltas.append((ri, i + 1))
        return deltas

    def rank(self, partition, t, row_id) -> int | None:
        rows = self._partitions.get(partition)
        if rows is None:
            return None
        try:
            return rows.index((t, row_id)) + 1
        except ValueError:
            return None


__all__ = ["RowNumberIVM"]
