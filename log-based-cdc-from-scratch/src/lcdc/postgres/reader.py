"""pgoutput message dispatcher.

Each ``pgoutput`` message is a single tag byte followed by a kind-
specific binary payload. :class:`PgOutputReader.iter_messages` takes
an iterable of *whole-message* byte blobs (one wire message per blob —
as emitted by ``CopyData`` rows on the replication protocol) and
returns a stream of typed message objects.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

from lcdc.postgres.messages import (
    BeginMessage,
    CommitMessage,
    DeleteMessage,
    InsertMessage,
    RelationMessage,
    TruncateMessage,
    UpdateMessage,
)


class PgOutputDecodeError(ValueError):
    """Raised when a pgoutput payload cannot be decoded."""


PgMessage = (
    BeginMessage
    | CommitMessage
    | RelationMessage
    | InsertMessage
    | UpdateMessage
    | DeleteMessage
    | TruncateMessage
)


@dataclass
class PgOutputReader:
    """Dispatcher over a stream of pgoutput messages."""

    def iter_messages(self, payloads: Iterable[bytes]) -> Iterator[PgMessage]:
        for payload in payloads:
            yield self.decode(payload)

    @staticmethod
    def decode(payload: bytes) -> PgMessage:
        if not payload:
            raise PgOutputDecodeError("empty pgoutput payload")
        tag = payload[0:1]
        body = payload[1:]
        try:
            if tag == b"B":
                return BeginMessage.decode(body)
            if tag == b"C":
                return CommitMessage.decode(body)
            if tag == b"R":
                return RelationMessage.decode(body)
            if tag == b"I":
                return InsertMessage.decode(body)
            if tag == b"U":
                return UpdateMessage.decode(body)
            if tag == b"D":
                return DeleteMessage.decode(body)
            if tag == b"T":
                return TruncateMessage.decode(body)
        except ValueError as exc:
            raise PgOutputDecodeError(f"failed to decode {tag!r}: {exc}") from exc
        raise PgOutputDecodeError(f"unknown pgoutput tag {tag!r}")


__all__ = ["PgMessage", "PgOutputDecodeError", "PgOutputReader"]
