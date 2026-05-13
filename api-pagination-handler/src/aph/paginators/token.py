"""Page-token pagination (Google-style).

Indistinguishable from cursor pagination at the protocol level but
typically called ``pageToken`` / ``nextPageToken``. We split it into
its own class so users can configure the names independently without
shadowing the cursor strategy's defaults.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from aph.paginators.base import PageRequest, Paginator, _resolve

if TYPE_CHECKING:
    from aph.transport import Response


def _set_query(url: str, key: str, value: str | None) -> str:
    parts = urlsplit(url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    if value is None:
        q.pop(key, None)
    else:
        q[key] = value
    return urlunsplit(parts._replace(query=urlencode(q)))


@dataclass
class TokenPaginator(Paginator):
    """Google-style ``pageToken`` / ``nextPageToken`` pagination."""

    token_param: str = "pageToken"
    next_token_path: str = "nextPageToken"
    records_path: str = "items"
    kind: str = "token"

    def __post_init__(self) -> None:
        if not self.token_param:
            raise ValueError("token_param must be non-empty")
        if not self.next_token_path:
            raise ValueError("next_token_path must be non-empty")

    def first(self, base_url: str) -> PageRequest:
        return PageRequest(url=_set_query(base_url, self.token_param, None))

    def next(self, prev: PageRequest, resp: Response) -> PageRequest | None:
        nxt = _resolve(self.next_token_path, resp.body)
        if nxt is None or nxt == "":
            return None
        return PageRequest(
            url=_set_query(prev.url, self.token_param, str(nxt)),
            headers=prev.headers,
        )

    def records(self, resp: Response) -> list[Any]:
        value = _resolve(self.records_path, resp.body)
        if isinstance(value, list):
            return list(value)
        return []


__all__ = ["TokenPaginator"]
