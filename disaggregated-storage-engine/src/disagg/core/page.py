"""Page abstraction.

A `Page` is a fixed-size byte buffer + version counter. Real systems use
~16KB pages on PostgreSQL, 8KB on MySQL, 4KB everywhere else. We use 4KB.

PageId is a `(tenant_id, page_no)` pair so the system supports multi-tenant
isolation at the cache + directory layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field

PAGE_SIZE: int = 4096


@dataclass(frozen=True, slots=True)
class PageId:
    """Globally unique page identifier."""

    tenant: int       # 0 for single-tenant
    page_no: int

    def __repr__(self) -> str:
        return f"p({self.tenant}:{self.page_no})"


@dataclass(slots=True)
class Page:
    """Mutable in-memory page. `version` increments on every write.

    Used by the coherence protocol to detect stale copies.
    """

    page_id: PageId
    version: int = 0
    data: bytes = field(default=b"\x00" * PAGE_SIZE)

    def __post_init__(self) -> None:
        if len(self.data) != PAGE_SIZE:
            raise ValueError(
                f"Page data must be exactly {PAGE_SIZE} bytes (got {len(self.data)})"
            )

    def update(self, new_data: bytes) -> None:
        if len(new_data) != PAGE_SIZE:
            raise ValueError(f"page write must be {PAGE_SIZE} bytes")
        self.data = new_data
        self.version += 1

    def clone(self) -> Page:
        return Page(page_id=self.page_id, version=self.version, data=self.data)


def blank_page(page_id: PageId) -> Page:
    return Page(page_id=page_id, version=0, data=b"\x00" * PAGE_SIZE)
