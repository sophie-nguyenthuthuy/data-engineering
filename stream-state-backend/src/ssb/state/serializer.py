"""msgpack serialization helpers with timestamp-prefix encoding."""

from __future__ import annotations

import struct
import time
from typing import Any

import msgpack

# Tombstone marker written on explicit clear() calls.
TOMBSTONE = b"\x00"

# Struct format for the 8-byte big-endian timestamp prefix.
_TS_STRUCT = struct.Struct(">Q")
_TS_SIZE = _TS_STRUCT.size  # 8 bytes


def now_ms() -> int:
    """Return current wall-clock time in milliseconds."""
    return int(time.time() * 1000)


def encode_key(key: Any) -> bytes:
    """Serialize an arbitrary key to bytes using msgpack."""
    return msgpack.packb(key, use_bin_type=True)


def decode_key(raw: bytes) -> Any:
    """Deserialize a msgpack-encoded key."""
    return msgpack.unpackb(raw, raw=False)


def encode_value(value: Any, timestamp_ms: int | None = None) -> bytes:
    """
    Encode *value* with an 8-byte big-endian millisecond timestamp prefix.

    Parameters
    ----------
    value:
        The Python object to serialize.
    timestamp_ms:
        Explicit timestamp to use; defaults to current time.
    """
    ts = timestamp_ms if timestamp_ms is not None else now_ms()
    ts_bytes = _TS_STRUCT.pack(ts)
    payload = msgpack.packb(value, use_bin_type=True)
    return ts_bytes + payload


def decode_value(raw: bytes) -> tuple[int, Any]:
    """
    Decode a value blob produced by :func:`encode_value`.

    Returns
    -------
    (timestamp_ms, value)
    """
    if len(raw) < _TS_SIZE:
        raise ValueError(f"Value blob too short ({len(raw)} bytes)")
    ts = _TS_STRUCT.unpack(raw[:_TS_SIZE])[0]
    value = msgpack.unpackb(raw[_TS_SIZE:], raw=False)
    return ts, value


def is_tombstone(raw: bytes) -> bool:
    """Return True if *raw* is the special tombstone marker."""
    return raw == TOMBSTONE


def encode_map_suffix(map_key: Any) -> bytes:
    """
    Encode a MapState entry suffix: ``\\xff`` + msgpack(map_key).

    The ``\\xff`` byte sorts after all normal key content, keeping map
    entries grouped at the end of the record's key space while remaining
    distinguishable from regular entries.
    """
    return b"\xff" + encode_key(map_key)


def decode_map_suffix(suffix_bytes: bytes) -> Any:
    """Decode the map key from a suffix produced by :func:`encode_map_suffix`."""
    if not suffix_bytes or suffix_bytes[0:1] != b"\xff":
        raise ValueError("Not a valid map suffix")
    return decode_key(suffix_bytes[1:])
