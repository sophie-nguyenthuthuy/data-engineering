"""MySQL binlog event header (v4 format, MySQL 5.0+).

Every binlog event starts with a fixed 19-byte header described in the
MySQL Internals manual:

    Bytes  Field           Notes
    -----  --------------- -------------------------------------------------
    4      timestamp       seconds since Unix epoch (uint32 LE)
    1      event_type      see :class:`EventType`
    4      server_id       MySQL server id that produced the event (uint32 LE)
    4      event_size      total size including this header (uint32 LE)
    4      log_pos         position of the next event in the binlog (uint32 LE)
    2      flags           per-event flags (uint16 LE)
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

HEADER_LEN = 19


@dataclass(frozen=True, slots=True)
class EventHeader:
    """The 19-byte event header."""

    timestamp: int
    event_type: int
    server_id: int
    event_size: int
    log_pos: int
    flags: int

    def __post_init__(self) -> None:
        for name in ("timestamp", "event_type", "server_id", "event_size", "log_pos", "flags"):
            v = getattr(self, name)
            if v < 0:
                raise ValueError(f"{name} must be ≥ 0")
        if self.event_size < HEADER_LEN:
            raise ValueError(
                f"event_size {self.event_size} smaller than header length {HEADER_LEN}"
            )
        if self.event_type > 0xFF:
            raise ValueError("event_type does not fit in a uint8")
        if self.flags > 0xFFFF:
            raise ValueError("flags does not fit in a uint16")

    @classmethod
    def decode(cls, buf: bytes) -> EventHeader:
        if len(buf) < HEADER_LEN:
            raise ValueError(f"buffer too short for event header: {len(buf)} < {HEADER_LEN}")
        ts, et, sid, sz, lp, fl = struct.unpack_from("<IBIIIH", buf, 0)
        return cls(timestamp=ts, event_type=et, server_id=sid, event_size=sz, log_pos=lp, flags=fl)

    def encode(self) -> bytes:
        return struct.pack(
            "<IBIIIH",
            self.timestamp,
            self.event_type,
            self.server_id,
            self.event_size,
            self.log_pos,
            self.flags,
        )

    @property
    def payload_size(self) -> int:
        """Bytes that follow the header for the event body."""
        return self.event_size - HEADER_LEN


__all__ = ["HEADER_LEN", "EventHeader"]
