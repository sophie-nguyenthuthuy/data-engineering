"""Write-amplification accounting.

In an external-memory analysis of a B^ε-tree, every message ultimately
hits a leaf — but may pass through O(log_B N / B^(1-ε)) buffer flushes
on the way. We count:

  - leaf_applies:    direct mutations to a leaf
  - buffer_inserts:  message landed in some buffer (root or middle)
  - flushes:         messages moved from a parent buffer to a child
  - splits:          node was rewritten due to overflow

Write amplification = (buffer_inserts + flushes + leaf_applies) / leaf_applies
This is the "rewrite ratio" relative to a hypothetical zero-overhead store.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field


@dataclass
class WriteAmpStats:
    leaf_applies: int = 0
    buffer_inserts: int = 0
    flushed_messages: int = 0
    splits: int = 0
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def record_leaf_apply(self) -> None:
        with self._lock:
            self.leaf_applies += 1

    def record_buffer_insert(self) -> None:
        with self._lock:
            self.buffer_inserts += 1

    def record_flush_messages(self, n: int) -> None:
        with self._lock:
            self.flushed_messages += n

    def record_split(self) -> None:
        with self._lock:
            self.splits += 1

    @property
    def total_message_movements(self) -> int:
        with self._lock:
            return self.leaf_applies + self.buffer_inserts + self.flushed_messages

    @property
    def write_amplification(self) -> float:
        """Average node rewrites per user-visible operation.

        For B^ε-tree the asymptotic write amplification is
            O((log_B N) / B^(1-ε))
        but we report the empirical value: total movement / leaf_applies.
        """
        with self._lock:
            if self.leaf_applies == 0:
                return 0.0
            return (self.buffer_inserts + self.flushed_messages + self.leaf_applies) / self.leaf_applies

    def snapshot(self) -> dict[str, int | float]:
        with self._lock:
            return {
                "leaf_applies": self.leaf_applies,
                "buffer_inserts": self.buffer_inserts,
                "flushed_messages": self.flushed_messages,
                "splits": self.splits,
                "write_amplification": self.write_amplification,
            }

    def reset(self) -> None:
        with self._lock:
            self.leaf_applies = 0
            self.buffer_inserts = 0
            self.flushed_messages = 0
            self.splits = 0
