"""Operation history recording and parsing for linearizability analysis."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Iterator, List, Optional


@dataclass
class Op:
    """A single event in the operation history."""
    process: int
    type: str    # 'invoke' | 'ok' | 'fail' | 'info'
    f: str       # function name: 'read' | 'write' | 'enqueue' | 'dequeue'
    value: Any   # args on invoke, return value on ok/fail
    time: float = field(default_factory=time.monotonic)
    index: int = 0

    def __repr__(self) -> str:
        return f"Op(p{self.process} {self.type:6s} {self.f}({self.value!r}) t={self.time:.4f})"


@dataclass
class Entry:
    """Paired invoke+response entry for linearizability checking."""
    index: int
    process: int
    f: str
    invoke_value: Any
    response_value: Any
    invoke_time: float
    response_time: float
    ok: bool  # True = ok, False = fail/crash

    @property
    def duration(self) -> float:
        return self.response_time - self.invoke_time


class History:
    """Thread-safe, append-only log of operation events."""

    def __init__(self) -> None:
        self._ops: List[Op] = []
        self._lock = threading.Lock()
        self._counter = 0

    def record(self, op: Op) -> Op:
        with self._lock:
            op.index = self._counter
            self._counter += 1
            self._ops.append(op)
        return op

    def ops(self) -> List[Op]:
        with self._lock:
            return list(self._ops)

    def entries(self) -> List[Entry]:
        """Pair invoke/response events into Entry objects.

        Unmatched invocations (crashed processes) are included as failed entries
        with response_time = max(all response times) to be conservative.
        """
        ops = self.ops()
        pending: dict[tuple[int, str], Op] = {}
        entries: List[Entry] = []
        idx = 0

        max_time = max((o.time for o in ops), default=0.0)

        for op in ops:
            key = (op.process, op.f)
            if op.type == "invoke":
                pending[key] = op
            elif op.type in ("ok", "fail"):
                invoke_op = pending.pop(key, None)
                if invoke_op is None:
                    continue
                entries.append(Entry(
                    index=idx,
                    process=op.process,
                    f=op.f,
                    invoke_value=invoke_op.value,
                    response_value=op.value,
                    invoke_time=invoke_op.time,
                    response_time=op.time,
                    ok=op.type == "ok",
                ))
                idx += 1

        # Treat dangling invocations (process crashed mid-op) as failed
        for invoke_op in pending.values():
            entries.append(Entry(
                index=idx,
                process=invoke_op.process,
                f=invoke_op.f,
                invoke_value=invoke_op.value,
                response_value=None,
                invoke_time=invoke_op.time,
                response_time=max_time,
                ok=False,
            ))
            idx += 1

        return entries

    def __iter__(self) -> Iterator[Op]:
        return iter(self.ops())

    def __len__(self) -> int:
        with self._lock:
            return len(self._ops)
