"""Source-adapter base class + shared types.

A :class:`Source` knows how to produce an iterable of :class:`Record`
objects. Each record is a plain ``dict[str, Any]`` plus a ``source_id``
that survives the trip through staging — so a downstream consumer can
join two sources without losing provenance.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator


class SourceError(RuntimeError):
    """Raised when a source adapter cannot produce records."""


@dataclass(frozen=True, slots=True)
class Record:
    """One record from a source.

    ``fields`` is the data payload (plain dict). ``source_id`` is a
    stable identifier *within* the source — primary key when one is
    known, otherwise the source-specific row number.
    """

    source_id: str
    fields: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.source_id:
            raise ValueError("source_id must be non-empty")


class Source(ABC):
    """Iterator interface implemented by every adapter."""

    #: Slug used by the naming convention (``http_api``, ``csv``, …).
    kind: str = "abstract"

    #: Logical dataset name (table). Override per instance.
    dataset: str = ""

    def __post_init__(self) -> None:  # pragma: no cover - dataclass hook
        if not self.dataset:
            raise ValueError("dataset must be non-empty")

    @abstractmethod
    def fetch(self) -> Iterator[Record]:
        """Yield :class:`Record` objects from the source."""

    def fetch_list(self) -> list[Record]:
        """Concrete helper to materialise the iterator."""
        return list(self.fetch())


__all__ = ["Record", "Source", "SourceError"]
