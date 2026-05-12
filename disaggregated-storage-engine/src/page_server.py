"""Remote buffer pool (page server) — accessed over a simulated network with
injected latency. Owns the canonical page state. Implements a simple
write-invalidate coherence directory.
"""
from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass, field
from threading import Lock


PAGE_SIZE = 4096


@dataclass
class CoherenceState:
    """Per-page directory entry."""
    holders: set = field(default_factory=set)   # client_ids with valid copies
    writer: int | None = None                   # exclusive writer (if any)


class PageServer:
    """Single-page-server holding canonical pages + coherence directory.

    Network latency is simulated by `sleep(net_latency_us / 1e6)`.
    """

    def __init__(self, net_latency_us: float = 50.0, capacity_pages: int = 1024):
        self.net_latency_us = net_latency_us
        self.capacity = capacity_pages
        self._pages: OrderedDict[int, bytes] = OrderedDict()  # page_id -> contents
        self._dir: dict[int, CoherenceState] = {}
        self._lock = Lock()
        self.stats = {"reads": 0, "writes": 0, "evictions": 0, "invalidations": 0}

    # ---- Network ----------------------------------------------------------

    def _hop(self):
        time.sleep(self.net_latency_us / 1e6)

    # ---- Read / Write -----------------------------------------------------

    def read(self, client_id: int, page_id: int) -> bytes:
        self._hop()
        with self._lock:
            self.stats["reads"] += 1
            page = self._pages.get(page_id, b"\x00" * PAGE_SIZE)
            self._pages[page_id] = page
            self._pages.move_to_end(page_id)
            d = self._dir.setdefault(page_id, CoherenceState())
            d.holders.add(client_id)
            self._maybe_evict()
            return page

    def write(self, client_id: int, page_id: int, data: bytes) -> list[int]:
        """Returns the list of client_ids that should invalidate their copy."""
        assert len(data) == PAGE_SIZE
        self._hop()
        with self._lock:
            self.stats["writes"] += 1
            self._pages[page_id] = data
            self._pages.move_to_end(page_id)
            d = self._dir.setdefault(page_id, CoherenceState())
            to_invalidate = list(d.holders - {client_id})
            self.stats["invalidations"] += len(to_invalidate)
            d.holders = {client_id}
            d.writer = client_id
            self._maybe_evict()
            return to_invalidate

    def release(self, client_id: int, page_id: int) -> None:
        with self._lock:
            d = self._dir.get(page_id)
            if d:
                d.holders.discard(client_id)
                if d.writer == client_id:
                    d.writer = None

    def _maybe_evict(self) -> None:
        while len(self._pages) > self.capacity:
            page_id, _ = self._pages.popitem(last=False)
            self.stats["evictions"] += 1


__all__ = ["PageServer", "PAGE_SIZE", "CoherenceState"]
