"""Cursor pagination.

The response body carries an opaque ``next_cursor`` value; the client
re-issues the request with that cursor in a query parameter until the
server returns no cursor (or returns the same one twice — many APIs
violate spec on this).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from aph.paginators.base import PageRequest, Paginator, _resolve

if TYPE_CHECKING:
    from aph.transport import Response


def _set_or_remove_query(url: str, key: str, value: str | None) -> str:
    parts = urlsplit(url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    if value is None:
        q.pop(key, None)
    else:
        q[key] = value
    return urlunsplit(parts._replace(query=urlencode(q)))


@dataclass
class CursorPaginator(Paginator):
    """Opaque-cursor pagination."""

    cursor_param: str = "cursor"
    cursor_path: str = "next_cursor"
    records_path: str = "data"
    kind: str = "cursor"

    def __post_init__(self) -> None:
        if not self.cursor_param:
            raise ValueError("cursor_param must be non-empty")
        if not self.cursor_path:
            raise ValueError("cursor_path must be non-empty")

    def first(self, base_url: str) -> PageRequest:
        return PageRequest(url=_set_or_remove_query(base_url, self.cursor_param, None))

    def next(self, prev: PageRequest, resp: Response) -> PageRequest | None:
        cursor = _resolve(self.cursor_path, resp.body)
        if cursor is None or cursor == "":
            return None
        cursor_str = str(cursor)
        prev_cursor = _read_query(prev.url, self.cursor_param)
        if prev_cursor is not None and prev_cursor == cursor_str:
            # Some buggy servers echo the cursor back; refuse to loop.
            return None
        return PageRequest(
            url=_set_or_remove_query(prev.url, self.cursor_param, cursor_str),
            headers=prev.headers,
        )

    def records(self, resp: Response) -> list[Any]:
        value = _resolve(self.records_path, resp.body)
        if isinstance(value, list):
            return list(value)
        return []


def _read_query(url: str, key: str) -> str | None:
    parts = urlsplit(url)
    q = dict(parse_qsl(parts.query))
    return q.get(key)


__all__ = ["CursorPaginator"]
