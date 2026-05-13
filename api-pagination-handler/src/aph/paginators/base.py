"""Paginator base class.

Every paginator describes:

  1. How to build the **first** request from the starting URL.
  2. How to read the **records** out of a response.
  3. How to build the **next** request given the previous one and the
     response it returned — returning ``None`` ends the iteration.

The base class plus the four concrete strategies in this package cover
the four pagination shapes overwhelmingly common in REST APIs.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aph.transport import Response


@dataclass(frozen=True, slots=True)
class PageRequest:
    """A single request to be issued by the transport."""

    url: str
    headers: Mapping[str, str] = field(default_factory=dict)


class Paginator(ABC):
    """ABC for pagination strategies."""

    #: Slug used in CLI / logs (``offset``, ``cursor``, ``token``, ``link``).
    kind: str = "abstract"

    @abstractmethod
    def first(self, base_url: str) -> PageRequest:
        """Return the request for the first page."""

    @abstractmethod
    def next(self, prev: PageRequest, resp: Response) -> PageRequest | None:
        """Return the next page request, or ``None`` if pagination is exhausted."""

    @abstractmethod
    def records(self, resp: Response) -> list[Any]:
        """Extract the list of records from one page."""


def _resolve(path: str, body: Any) -> Any:
    """Walk a dot-separated path into a nested mapping."""
    if not path:
        return body
    cur = body
    for segment in path.split("."):
        if isinstance(cur, Mapping) and segment in cur:
            cur = cur[segment]
        else:
            return None
    return cur


__all__ = ["PageRequest", "Paginator"]
