"""LRU eviction policy for the remote buffer pool."""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

    from disagg.core.page import PageId


class LRUEvictor:
    """Thread-safe LRU. Items are touched on access; oldest are evicted first
    when capacity is exceeded.

    Production page servers also pin pages that are currently in-flight or
    held in write-locks; we omit pinning here but document the extension point.
    """

    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self.capacity = capacity
        self._order: OrderedDict[PageId, None] = OrderedDict()
        self._lock = threading.Lock()

    def touch(self, page_id: PageId) -> None:
        """Mark `page_id` as most-recently-used."""
        with self._lock:
            if page_id in self._order:
                self._order.move_to_end(page_id, last=True)
            else:
                self._order[page_id] = None

    def remove(self, page_id: PageId) -> None:
        with self._lock:
            self._order.pop(page_id, None)

    def evict_if_needed(self) -> list[PageId]:
        """Returns the page IDs that should be evicted to fit within capacity."""
        with self._lock:
            evicted: list[PageId] = []
            while len(self._order) > self.capacity:
                pid, _ = self._order.popitem(last=False)
                evicted.append(pid)
            return evicted

    def __len__(self) -> int:
        with self._lock:
            return len(self._order)

    def __contains__(self, page_id: PageId) -> bool:
        with self._lock:
            return page_id in self._order

    def __iter__(self) -> Iterable[PageId]:
        with self._lock:
            return iter(list(self._order.keys()))
