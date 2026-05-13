"""Offset / limit pagination.

The request carries ``offset`` and ``limit`` query parameters; the
response carries a list of records at ``records_path``. Iteration
stops when a page returns fewer than ``limit`` records.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from aph.paginators.base import PageRequest, Paginator, _resolve

if TYPE_CHECKING:
    from aph.transport import Response


def _set_query(url: str, params: dict[str, str]) -> str:
    parts = urlsplit(url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    q.update(params)
    return urlunsplit(parts._replace(query=urlencode(q)))


@dataclass
class OffsetPaginator(Paginator):
    """Offset + limit pagination."""

    limit: int = 100
    offset_param: str = "offset"
    limit_param: str = "limit"
    records_path: str = ""
    kind: str = "offset"

    def __post_init__(self) -> None:
        if self.limit < 1:
            raise ValueError("limit must be ≥ 1")
        if not self.offset_param or not self.limit_param:
            raise ValueError("offset_param and limit_param must be non-empty")

    def first(self, base_url: str) -> PageRequest:
        return PageRequest(
            url=_set_query(
                base_url,
                {self.offset_param: "0", self.limit_param: str(self.limit)},
            )
        )

    def next(self, prev: PageRequest, resp: Response) -> PageRequest | None:
        records = self.records(resp)
        if len(records) < self.limit:
            return None
        # Bump offset by the page size that we asked for.
        prev_offset = _read_int_query(prev.url, self.offset_param, default=0)
        return PageRequest(
            url=_set_query(
                prev.url,
                {self.offset_param: str(prev_offset + self.limit)},
            ),
            headers=prev.headers,
        )

    def records(self, resp: Response) -> list[Any]:
        return _records_at_path(resp.body, self.records_path)


def _read_int_query(url: str, key: str, default: int = 0) -> int:
    parts = urlsplit(url)
    q = dict(parse_qsl(parts.query))
    try:
        return int(q.get(key, default))
    except (TypeError, ValueError):
        return default


def _records_at_path(body: Any, path: str) -> list[Any]:
    value = _resolve(path, body)
    if isinstance(value, list):
        return list(value)
    if isinstance(body, list) and not path:
        return list(body)
    return []


__all__ = ["OffsetPaginator"]
