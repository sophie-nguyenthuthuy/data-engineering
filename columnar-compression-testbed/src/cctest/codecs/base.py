from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import numpy as np


@dataclass
class EncodedColumn:
    codec_name: str
    data: bytes
    metadata: dict = field(default_factory=dict)
    original_dtype: str = ""
    original_len: int = 0

    def total_bytes(self) -> int:
        import struct
        meta_bytes = 0
        for v in self.metadata.values():
            if isinstance(v, (int, float)):
                meta_bytes += 8
            elif isinstance(v, bytes):
                meta_bytes += len(v)
            elif isinstance(v, str):
                meta_bytes += len(v.encode())
        return len(self.data) + meta_bytes


@dataclass
class BenchmarkResult:
    codec_name: str
    original_bytes: int
    compressed_bytes: int
    encode_ms: float
    decode_ms: float
    lossless: bool = True

    @property
    def ratio(self) -> float:
        return self.original_bytes / self.compressed_bytes if self.compressed_bytes else 0.0

    @property
    def space_saving(self) -> float:
        return 1.0 - self.compressed_bytes / self.original_bytes if self.original_bytes else 0.0

    def __str__(self) -> str:
        status = "OK" if self.lossless else "LOSSY"
        return (
            f"{self.codec_name:<20} ratio={self.ratio:.2f}x  "
            f"saving={self.space_saving*100:.1f}%  "
            f"enc={self.encode_ms:.2f}ms  dec={self.decode_ms:.2f}ms  [{status}]"
        )


class Codec(ABC):
    name: str = "base"

    @abstractmethod
    def encode(self, data: np.ndarray) -> EncodedColumn: ...

    @abstractmethod
    def decode(self, encoded: EncodedColumn) -> np.ndarray: ...

    def supports_dtype(self, dtype: np.dtype) -> bool:
        return True

    def benchmark(self, data: np.ndarray, rounds: int = 5) -> BenchmarkResult:
        original_bytes = data.nbytes

        encode_times = []
        encoded = None
        for _ in range(rounds):
            t0 = time.perf_counter()
            encoded = self.encode(data)
            encode_times.append(time.perf_counter() - t0)

        decode_times = []
        decoded = None
        for _ in range(rounds):
            t0 = time.perf_counter()
            decoded = self.decode(encoded)
            decode_times.append(time.perf_counter() - t0)

        if data.dtype.kind == "U" or data.dtype.kind == "O":
            lossless = list(data) == list(decoded)
        elif data.dtype.kind == "f":
            lossless = np.allclose(data, decoded, equal_nan=True)
        else:
            lossless = np.array_equal(data, decoded)

        return BenchmarkResult(
            codec_name=self.name,
            original_bytes=original_bytes,
            compressed_bytes=encoded.total_bytes(),
            encode_ms=min(encode_times) * 1000,
            decode_ms=min(decode_times) * 1000,
            lossless=lossless,
        )
