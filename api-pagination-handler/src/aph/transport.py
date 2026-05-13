"""Transport-layer types.

A :class:`Response` is the minimal information any paginator needs to
advance:

  * ``status``   — HTTP status code (200, 429, 503, …).
  * ``headers``  — case-insensitive lookup of HTTP headers.
  * ``body``     — raw decoded JSON payload (``dict`` or ``list``).
  * ``url``      — the URL that produced the response (some paginators
                   build the next URL relative to this one).

We model the transport as a callable ``(url, headers) -> Response`` so
every paginator and the retry policy can be exercised against fakes —
no real network access required.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, TypeAlias


@dataclass(frozen=True, slots=True)
class Response:
    """Minimal HTTP response used by paginators."""

    status: int
    body: Any
    headers: Mapping[str, str] = field(default_factory=dict)
    url: str = ""

    def header(self, name: str) -> str | None:
        """Case-insensitive header lookup."""
        target = name.lower()
        for k, v in self.headers.items():
            if k.lower() == target:
                return v
        return None

    def is_success(self) -> bool:
        return 200 <= self.status < 300

    def is_retryable(self) -> bool:
        """Status codes that *might* succeed if retried."""
        return self.status in {408, 425, 429, 500, 502, 503, 504}


Transport: TypeAlias = Callable[[str, Mapping[str, str]], Response]


__all__ = ["Response", "Transport"]
