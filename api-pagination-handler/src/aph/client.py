"""High-level client gluing a :class:`Paginator` + :class:`RetryPolicy`
to a transport callable.

``PaginatedClient.iter_records`` is a generator yielding records across
all pages; ``PaginatedClient.iter_pages`` yields each
:class:`Response` so callers can inspect headers / status. Both honour
``max_pages`` as a circuit-breaker against accidentally-infinite
paginators.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING, Any

from aph.retry import RetryPolicy

if TYPE_CHECKING:
    from collections.abc import Iterator

    from aph.paginators.base import Paginator
    from aph.transport import Response, Transport


@dataclass
class PaginatedClient:
    """Drive a :class:`Paginator` to completion against a ``Transport``."""

    transport: Transport
    paginator: Paginator
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    max_pages: int = 10_000

    def __post_init__(self) -> None:
        if self.max_pages < 1:
            raise ValueError("max_pages must be ≥ 1")

    # ----------------------------------------------------------- iteration

    def iter_pages(self, base_url: str) -> Iterator[Response]:
        if not base_url:
            raise ValueError("base_url must be non-empty")
        request = self.paginator.first(base_url)
        for page_index in range(self.max_pages):
            # `partial` snapshots the current request, avoiding the
            # late-binding loop-variable trap (ruff B023) entirely.
            fetch = partial(self.transport, request.url, dict(request.headers))
            resp = self.retry.run_response(fetch)
            if not resp.is_success():
                raise RuntimeError(
                    f"page {page_index} {request.url!r} failed with status {resp.status}"
                )
            yield resp
            nxt = self.paginator.next(request, resp)
            if nxt is None:
                return
            request = nxt

    def iter_records(self, base_url: str) -> Iterator[Any]:
        for resp in self.iter_pages(base_url):
            yield from self.paginator.records(resp)

    def fetch_all(self, base_url: str) -> list[Any]:
        return list(self.iter_records(base_url))


__all__ = ["PaginatedClient"]
