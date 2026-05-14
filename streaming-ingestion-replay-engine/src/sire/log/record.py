"""Record + binary-format header.

Each :class:`Record` is the unit of work in the replay engine. The
on-disk shape is::

    +--------+----------+--------+--------+-----------+----------+
    | offset | timestamp| keylen | vallen |   key     |  value   |
    | (Q,8B) |  (q,8B)  | (I,4B) | (I,4B) | (keylen)  | (vallen) |
    +--------+----------+--------+--------+-----------+----------+

  * ``offset`` — monotonically-increasing record index inside the topic.
  * ``timestamp`` — record's logical timestamp (signed, ms-since-epoch).
  * ``key`` / ``value`` — arbitrary bytes; either may be empty.

Big-endian network order so a hex-dumped segment is human-readable
across architectures.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

HEADER_FMT = ">QqII"
HEADER_SIZE = struct.calcsize(HEADER_FMT)  # 24 bytes


@dataclass(frozen=True, slots=True)
class RecordHeader:
    """The fixed 24-byte prefix in front of each on-disk record."""

    offset: int
    timestamp: int
    key_len: int
    value_len: int

    def __post_init__(self) -> None:
        if self.offset < 0:
            raise ValueError("offset must be ≥ 0")
        if self.key_len < 0 or self.value_len < 0:
            raise ValueError("key/value length must be ≥ 0")

    def encode(self) -> bytes:
        return struct.pack(HEADER_FMT, self.offset, self.timestamp, self.key_len, self.value_len)

    @classmethod
    def decode(cls, buf: bytes) -> RecordHeader:
        if len(buf) < HEADER_SIZE:
            raise ValueError(f"buffer too short for header: {len(buf)} < {HEADER_SIZE}")
        offset, ts, kl, vl = struct.unpack_from(HEADER_FMT, buf, 0)
        return cls(offset=offset, timestamp=ts, key_len=kl, value_len=vl)


@dataclass(frozen=True, slots=True)
class Record:
    """One log entry."""

    offset: int
    timestamp: int
    key: bytes
    value: bytes

    def __post_init__(self) -> None:
        if self.offset < 0:
            raise ValueError("offset must be ≥ 0")
        if not isinstance(self.key, bytes | bytearray):
            raise TypeError("key must be bytes")
        if not isinstance(self.value, bytes | bytearray):
            raise TypeError("value must be bytes")

    def encode(self) -> bytes:
        hdr = RecordHeader(
            offset=self.offset,
            timestamp=self.timestamp,
            key_len=len(self.key),
            value_len=len(self.value),
        )
        return hdr.encode() + bytes(self.key) + bytes(self.value)

    @classmethod
    def decode(cls, buf: bytes, offset: int = 0) -> tuple[Record, int]:
        """Decode one record. Returns ``(record, bytes_consumed)``."""
        hdr = RecordHeader.decode(buf[offset:])
        cursor = offset + HEADER_SIZE
        end_key = cursor + hdr.key_len
        end_value = end_key + hdr.value_len
        if end_value > len(buf):
            raise ValueError("record truncated at key/value")
        return (
            cls(
                offset=hdr.offset,
                timestamp=hdr.timestamp,
                key=buf[cursor:end_key],
                value=buf[end_key:end_value],
            ),
            end_value - offset,
        )


__all__ = ["HEADER_FMT", "HEADER_SIZE", "Record", "RecordHeader"]
