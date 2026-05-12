"""Source operator — the entry point for a named input stream."""
from __future__ import annotations

from typing import List

from ivm.operators.base import Operator
from ivm.types import Update


class SourceOperator(Operator):
    """Passes updates through unchanged; acts as the root of a pipeline."""

    def __init__(self, stream_name: str):
        super().__init__()
        self.stream_name = stream_name
        self.name = stream_name

    def process(self, updates: List[Update]) -> List[Update]:
        return updates
