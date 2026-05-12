"""Project operator — reshape records by selecting or transforming columns."""
from __future__ import annotations

from typing import Callable, List, Optional

from ivm.operators.base import Operator
from ivm.types import Record, Update


class ProjectOperator(Operator):
    """Emit a transformed version of each record.

    Provide either:
      columns  — list of column names to keep
      transform — callable(record) -> new_record

    If both are provided, columns is applied first, then transform.
    """

    def __init__(self, columns: Optional[List[str]] = None,
                 transform: Optional[Callable[[Record], Record]] = None):
        super().__init__()
        self.columns = columns
        self.transform = transform

    def _apply(self, record: Record) -> Record:
        if self.columns is not None:
            record = {k: record[k] for k in self.columns if k in record}
        if self.transform is not None:
            record = self.transform(record)
        return record

    def process(self, updates: List[Update]) -> List[Update]:
        return [Update(self._apply(u.record), u.timestamp, u.diff) for u in updates]
