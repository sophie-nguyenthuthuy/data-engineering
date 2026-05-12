"""Filter operator — drop records that don't satisfy a predicate."""
from __future__ import annotations

from typing import Callable, List

from ivm.operators.base import Operator
from ivm.types import Record, Update


class FilterOperator(Operator):
    """Passes through updates whose record satisfies predicate(record) == True.

    The diff (insertion/retraction) is preserved unchanged.
    """

    def __init__(self, predicate: Callable[[Record], bool]):
        super().__init__()
        self.predicate = predicate

    def process(self, updates: List[Update]) -> List[Update]:
        return [u for u in updates if self.predicate(u.record)]
