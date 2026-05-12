"""Operator base class.

An Operator is a stateful node in the dataflow graph. It receives input
records (each tagged with a Timestamp) and emits zero or more output
records, possibly to a different timestamp (e.g. inside an iterate scope).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from timely.timestamp.ts import Timestamp

# Emit signature: emit(downstream_op_name, ts, value)
EmitFn = Callable[[str, Timestamp, Any], None]


@dataclass
class Operator:
    name: str
    fn: Callable[[Timestamp, Any, EmitFn], None]
    inputs: list[str] = field(default_factory=list)   # upstream op names
    feedback: bool = False                            # iterate-scope loop edge

    def __repr__(self) -> str:
        suffix = " [feedback]" if self.feedback else ""
        return f"Operator({self.name}{suffix})"


__all__ = ["EmitFn", "Operator"]
