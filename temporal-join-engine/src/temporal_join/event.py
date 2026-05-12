"""Event and JoinResult types."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

STREAM_LEFT = "left"
STREAM_RIGHT = "right"


@dataclass(frozen=True)
class Event:
    """
    A single event on either the left (probe) or right (build) stream.

    key        -- join key (e.g. user_id, sensor_id)
    event_time -- logical timestamp in milliseconds
    stream_id  -- STREAM_LEFT or STREAM_RIGHT
    payload    -- arbitrary application data (not used for equality/hashing)
    """

    key: str
    event_time: int
    stream_id: str
    payload: Dict[str, Any] = field(default_factory=dict, hash=False, compare=False)

    def __post_init__(self) -> None:
        if self.stream_id not in (STREAM_LEFT, STREAM_RIGHT):
            raise ValueError(
                f"stream_id must be {STREAM_LEFT!r} or {STREAM_RIGHT!r}, got {self.stream_id!r}"
            )


@dataclass
class JoinResult:
    """
    Output record from the temporal join engine.

    retraction=True means the engine is withdrawing a previously emitted result
    for (left_event.key, left_event.event_time); it will be immediately followed
    by a non-retraction record with the corrected right match.
    """

    left_event: Event
    right_event: Optional[Event]
    retraction: bool = False

    def __repr__(self) -> str:
        tag = "RETRACT" if self.retraction else "EMIT   "
        r_t = self.right_event.event_time if self.right_event else "∅"
        return (
            f"JoinResult({tag} key={self.left_event.key!r} "
            f"L@{self.left_event.event_time} ⋈ R@{r_t})"
        )
