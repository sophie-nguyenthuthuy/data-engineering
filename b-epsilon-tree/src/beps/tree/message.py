"""Buffered messages.

Each insert/delete becomes a `Message` that lands in the root buffer and
percolates downward on flush. The `seq` field provides total ordering so
that older messages buried in upper buffers never override newer leaf state.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Any


class Op(IntEnum):
    PUT = 1
    DEL = 2


@dataclass(slots=True)
class Message:
    op: Op
    key: bytes
    value: Any = None         # None for DEL
    seq: int = 0              # monotone seq, set by the tree at injection

    def __repr__(self) -> str:
        if self.op == Op.PUT:
            return f"Put({self.key!r}={self.value!r}, seq={self.seq})"
        return f"Del({self.key!r}, seq={self.seq})"
