"""RFC-5988 ``Link``-header pagination (GitHub-style).

The server emits a ``Link`` header containing one or more
``<url>; rel="next"`` (or ``rel="last"``, ``"prev"``, ``"first"``)
entries. The client follows ``rel="next"`` until the header no longer
includes one.

The records live in the response body (``records_path`` works the same
way as the other paginators).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from aph.paginators.base import PageRequest, Paginator, _resolve

if TYPE_CHECKING:
    from aph.transport import Response


# Conservative RFC-5988 parser: ``<url>; rel="<name>"`` entries separated by commas.
# We do not implement the full grammar (quoted-string escapes, multiple params)
# because real APIs rarely emit anything fancier.
_LINK_ENTRY = re.compile(r"""<\s*(?P<url>[^>]+)\s*>\s*;(?P<params>[^,]+)""")
_REL = re.compile(r"""rel\s*=\s*"(?P<rel>[^"]+)\"""")


def parse_link_header(value: str) -> dict[str, str]:
    """Return ``{rel: url}``; only the first ``rel`` per entry is kept."""
    out: dict[str, str] = {}
    for entry in _LINK_ENTRY.finditer(value):
        rel_match = _REL.search(entry.group("params"))
        if not rel_match:
            continue
        rel = rel_match.group("rel")
        out.setdefault(rel, entry.group("url").strip())
    return out


@dataclass
class LinkHeaderPaginator(Paginator):
    """GitHub-style ``Link: <url>; rel="next"`` pagination."""

    records_path: str = ""
    rel: str = "next"
    kind: str = "link"

    def __post_init__(self) -> None:
        if not self.rel:
            raise ValueError("rel must be non-empty")

    def first(self, base_url: str) -> PageRequest:
        return PageRequest(url=base_url)

    def next(self, prev: PageRequest, resp: Response) -> PageRequest | None:
        link = resp.header("Link")
        if not link:
            return None
        rels = parse_link_header(link)
        url = rels.get(self.rel)
        if not url or url == prev.url:
            return None
        return PageRequest(url=url, headers=prev.headers)

    def records(self, resp: Response) -> list[Any]:
        value = _resolve(self.records_path, resp.body) if self.records_path else resp.body
        if isinstance(value, list):
            return list(value)
        return []


__all__ = ["LinkHeaderPaginator", "parse_link_header"]
