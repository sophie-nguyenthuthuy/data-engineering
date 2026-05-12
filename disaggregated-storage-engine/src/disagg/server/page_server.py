"""Remote buffer pool (page server).

Owns the canonical page state for all clients. Each request comes in over
a Transport; the server dispatches by operation name.

Operations:
    read(client_id, page_id)        -> Page (bytes-encoded)
    write(client_id, page_id, data) -> {invalidate: list[client_id]}
    release(client_id, page_id)     -> ack
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from disagg.core.page import Page, PageId, blank_page
from disagg.server.coherence import CoherenceDirectory
from disagg.server.eviction import LRUEvictor


@dataclass
class PageServerStats:
    reads: int = 0
    writes: int = 0
    evictions: int = 0
    invalidations: int = 0


class PageServer:
    """Holds canonical pages + a sharded coherence directory."""

    def __init__(self, capacity_pages: int = 1024, n_shards: int = 16) -> None:
        self._pages: dict[PageId, Page] = {}
        self._evictor = LRUEvictor(capacity=capacity_pages)
        self.dir = CoherenceDirectory(n_shards=n_shards)
        self.stats = PageServerStats()
        self._lock = threading.RLock()    # protects self._pages

    # ---- Transport dispatch ----------------------------------------------

    def dispatch(self, op: str, **kwargs: object) -> object:
        if op == "read":
            return self.read(int(kwargs["client_id"]), kwargs["page_id"])  # type: ignore[arg-type]
        if op == "write":
            return self.write(
                int(kwargs["client_id"]),
                kwargs["page_id"],  # type: ignore[arg-type]
                kwargs["data"],     # type: ignore[arg-type]
            )
        if op == "release":
            self.release(int(kwargs["client_id"]), kwargs["page_id"])  # type: ignore[arg-type]
            return None
        raise ValueError(f"unknown op: {op}")

    # ---- Public ops -------------------------------------------------------

    def read(self, client_id: int, page_id: PageId) -> Page:
        with self._lock:
            page = self._pages.get(page_id)
            if page is None:
                page = blank_page(page_id)
                self._pages[page_id] = page
            self.stats.reads += 1
            self._evictor.touch(page_id)
            self._evict_if_needed()
            self.dir.register_reader(page_id, client_id, page.version)
            return page.clone()

    def write(self, client_id: int, page_id: PageId, data: bytes) -> dict[str, list[int]]:
        """Returns {"invalidate": [client_id, ...]} — the holders that need to
        drop their local copy."""
        with self._lock:
            page = self._pages.get(page_id)
            if page is None:
                page = blank_page(page_id)
                self._pages[page_id] = page
            page.update(data)
            self.stats.writes += 1
            self._evictor.touch(page_id)
            self._evict_if_needed()
            invalidate = self.dir.register_writer(page_id, client_id, page.version)
            self.stats.invalidations += len(invalidate)
            return {"invalidate": invalidate}

    def release(self, client_id: int, page_id: PageId) -> None:
        self.dir.release(page_id, client_id)

    def get_page(self, page_id: PageId) -> Page | None:
        """Diagnostic — returns the canonical page without coherence side-effects."""
        with self._lock:
            page = self._pages.get(page_id)
            return page.clone() if page else None

    # ---- Internal ---------------------------------------------------------

    def _evict_if_needed(self) -> None:
        evicted = self._evictor.evict_if_needed()
        for pid in evicted:
            self._pages.pop(pid, None)
            self.stats.evictions += 1

    @property
    def page_count(self) -> int:
        with self._lock:
            return len(self._pages)
