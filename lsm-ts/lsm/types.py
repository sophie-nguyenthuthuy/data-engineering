"""Core types and binary encoding for the LSM time-series engine.

Key layout (lexicographically sortable):
  {metric}\x00{tag_k1=v1,...}\x00{timestamp_ns as 8-byte big-endian}

This lets us do prefix scans per metric, and range scans per metric+tagset.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass


@dataclass(frozen=True, order=True)
class TSKey:
    metric: str
    tags: tuple[tuple[str, str], ...]
    timestamp_ns: int

    @classmethod
    def make(cls, metric: str, tags: dict[str, str], timestamp_ns: int) -> TSKey:
        return cls(
            metric=metric,
            tags=tuple(sorted(tags.items())),
            timestamp_ns=timestamp_ns,
        )

    def encode(self) -> bytes:
        tag_str = ",".join(f"{k}={v}" for k, v in self.tags)
        prefix = f"{self.metric}\x00{tag_str}\x00".encode()
        return prefix + struct.pack(">Q", self.timestamp_ns)

    @classmethod
    def decode(cls, data: bytes) -> TSKey:
        timestamp_ns = struct.unpack(">Q", data[-8:])[0]
        parts = data[:-8].decode().split("\x00")
        metric = parts[0]
        tags: dict[str, str] = {}
        if len(parts) > 1 and parts[1]:
            for pair in parts[1].split(","):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    tags[k] = v
        return cls.make(metric, tags, timestamp_ns)

    def metric_prefix(self) -> bytes:
        """Byte prefix for all keys belonging to this metric+tagset."""
        tag_str = ",".join(f"{k}={v}" for k, v in self.tags)
        return f"{self.metric}\x00{tag_str}\x00".encode()


@dataclass(frozen=True)
class TSValue:
    value: float

    SIZE = 8  # float64

    def encode(self) -> bytes:
        return struct.pack(">d", self.value)

    @classmethod
    def decode(cls, data: bytes) -> TSValue:
        (v,) = struct.unpack(">d", data)
        return cls(value=v)


@dataclass(frozen=True)
class DataPoint:
    key: TSKey
    value: TSValue

    @property
    def encoded_key(self) -> bytes:
        return self.key.encode()

    @property
    def encoded_value(self) -> bytes:
        return self.value.encode()
