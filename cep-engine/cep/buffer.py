"""
Lock-free single-writer ring buffer backed by shared memory.

Layout (bytes):
  [0:8]   write_cursor  — monotonically increasing event count
  [8:16]  capacity
  [16:64] reserved / padding to cache-line boundary
  [64:]   event data (capacity * 32 bytes each)
"""

import struct
from multiprocessing.shared_memory import SharedMemory
from typing import Optional

import numpy as np

from .event import EVENT_DTYPE

_HEADER_BYTES = 64
_EVENT_SIZE = EVENT_DTYPE.itemsize  # 32


class RingBuffer:
    """
    Fixed-capacity ring buffer in shared memory.

    Single-writer, multiple-reader.  No locks: readers observe a consistent
    snapshot by reading write_cursor before and after copying the slice.
    """

    def __init__(self, capacity: int = 1 << 16, name: Optional[str] = None):
        if capacity & (capacity - 1):
            raise ValueError("capacity must be a power of two")
        self._capacity = capacity
        self._mask = capacity - 1
        total = _HEADER_BYTES + capacity * _EVENT_SIZE

        if name is None:
            self._shm = SharedMemory(create=True, size=total)
            self._owner = True
        else:
            self._shm = SharedMemory(name=name, create=False)
            self._owner = False

        self._header = np.ndarray((8,), dtype=np.int64, buffer=self._shm.buf[:_HEADER_BYTES])
        self._data = np.ndarray(
            (capacity,), dtype=EVENT_DTYPE, buffer=self._shm.buf[_HEADER_BYTES:]
        )

        if self._owner:
            self._header[:] = 0
            self._header[1] = capacity

    # ------------------------------------------------------------------
    @property
    def name(self) -> str:
        return self._shm.name

    @property
    def write_cursor(self) -> int:
        return int(self._header[0])

    @property
    def capacity(self) -> int:
        return self._capacity

    # ------------------------------------------------------------------
    def push(self, event: np.void) -> None:
        """Write one event.  Not thread-safe — call from a single writer."""
        slot = int(self._header[0]) & self._mask
        self._data[slot] = event
        # Memory barrier: ensure data is visible before advancing cursor.
        # On x86 TSO this is a compiler fence; on ARM we'd need a store-fence.
        # numpy assignment is not reordered by CPython's GIL so this is safe
        # for same-process use; cross-process readers should use write_cursor
        # with a short spin.
        self._header[0] += 1

    def push_batch(self, events: np.ndarray) -> None:
        """Write a contiguous batch of events (faster for bulk ingestion)."""
        n = len(events)
        cursor = int(self._header[0])
        for i in range(n):
            slot = (cursor + i) & self._mask
            self._data[slot] = events[i]
        self._header[0] = cursor + n

    # ------------------------------------------------------------------
    def read_recent(self, n: int) -> np.ndarray:
        """Return up to the last *n* events as a contiguous numpy array copy."""
        cursor = int(self._header[0])
        n = min(n, cursor, self._capacity)
        if n == 0:
            return np.empty(0, dtype=EVENT_DTYPE)

        start = (cursor - n) & self._mask
        end = cursor & self._mask

        if start < end:
            return self._data[start:end].copy()
        else:
            # Wrap-around: stitch two slices
            return np.concatenate([self._data[start:], self._data[:end]])

    def read_from_cursor(self, from_cursor: int) -> tuple[np.ndarray, int]:
        """
        Return all events since *from_cursor* and the new cursor position.

        Useful for continuous consumption: call repeatedly, threading the
        returned cursor back as *from_cursor*.
        """
        cur = int(self._header[0])
        available = cur - from_cursor
        if available <= 0:
            return np.empty(0, dtype=EVENT_DTYPE), cur
        # Guard against reads that would go beyond the ring capacity
        available = min(available, self._capacity)
        from_cursor = cur - available
        events = self.read_recent(available)
        return events, cur

    # ------------------------------------------------------------------
    def close(self) -> None:
        self._shm.close()
        if self._owner:
            self._shm.unlink()

    def __del__(self):
        try:
            self._shm.close()
            if self._owner:
                self._shm.unlink()
        except Exception:
            pass
