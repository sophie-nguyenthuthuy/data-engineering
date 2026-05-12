"""Message types for the distributed pipeline."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import uuid


def _short_id() -> str:
    return uuid.uuid4().hex[:8]


@dataclass
class DataMessage:
    """A data message flowing through the pipeline."""

    content: Any
    msg_id: str = field(default_factory=_short_id)
    # Monotonically increasing sequence number assigned by the source
    origin_seq: int = -1
    sender_id: str = ""

    def __repr__(self) -> str:
        return f"Data({self.content!r}, seq={self.origin_seq}, id={self.msg_id})"


@dataclass
class Marker:
    """
    Chandy-Lamport barrier marker.

    When a node receives this on channel C it must:
      - Record its local state (if not yet done for this snapshot_id)
      - Propagate the marker on all outgoing channels
      - Record messages arriving on other incoming channels until their markers arrive
    """

    snapshot_id: str
    initiator_id: str

    def __repr__(self) -> str:
        return f"Marker(snap={self.snapshot_id[:8]})"
