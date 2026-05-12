"""ALP – Adaptive Lossless floating-Point compression.

Algorithm overview (based on the SIGMOD 2023 paper)
---------------------------------------------------
1. Sample the column and try every (exponent) e in [0, 18].
2. For each e, encode a float f as int64 via round(f × 10^e).
3. The best e maximises the fraction of values that round-trip exactly.
4. Non-round-tripping values are stored as exceptions (position + raw float64).
5. The int64 array is then frame-of-reference + bit-packed.
"""
from __future__ import annotations

import struct

import numpy as np

from .base import Codec, EncodedColumn

_MAX_EXPONENT = 18
_POWERS = np.array([10**e for e in range(_MAX_EXPONENT + 1)], dtype=np.float64)


def _count_exceptions(values: np.ndarray, e: int) -> int:
    factor = _POWERS[e]
    finite_mask = np.isfinite(values)
    if not finite_mask.any():
        return len(values)
    finite = values[finite_mask]
    with np.errstate(invalid="ignore", over="ignore"):
        encoded = np.round(finite * factor).astype(np.int64)
    reconstructed = encoded.astype(np.float64) / factor
    n_exc = int(np.sum(~np.isclose(finite, reconstructed, rtol=0, atol=1e-9)))
    return n_exc + int((~finite_mask).sum())


def _find_best_exponent(sample: np.ndarray) -> int:
    best_e = 0
    best_exc = len(sample) + 1
    for e in range(_MAX_EXPONENT + 1):
        exc = _count_exceptions(sample, e)
        if exc < best_exc:
            best_exc = exc
            best_e = e
        if best_exc == 0:
            break
    return best_e


def _bitpack_width(values: np.ndarray) -> int:
    if len(values) == 0:
        return 0
    mn, mx = int(values.min()), int(values.max())
    span = mx - mn
    if span == 0:
        return 0
    return int(span).bit_length()


def _bitpack(values: np.ndarray, width: int, ref: int) -> bytes:
    if width == 0:
        return b""
    shifted = (values.astype(np.int64) - ref).astype(np.uint64)
    out = bytearray()
    buf = 0
    bits_in_buf = 0
    for v in shifted:
        buf = (buf << width) | int(v)
        bits_in_buf += width
        while bits_in_buf >= 8:
            bits_in_buf -= 8
            out.append((buf >> bits_in_buf) & 0xFF)
    if bits_in_buf:
        out.append((buf << (8 - bits_in_buf)) & 0xFF)
    return bytes(out)


def _bitunpack(data: bytes, n: int, width: int, ref: int) -> np.ndarray:
    if width == 0:
        return np.full(n, ref, dtype=np.int64)
    out = np.empty(n, dtype=np.int64)
    buf = 0
    bits_in_buf = 0
    byte_pos = 0
    mask = (1 << width) - 1
    for i in range(n):
        while bits_in_buf < width:
            buf = (buf << 8) | (data[byte_pos] if byte_pos < len(data) else 0)
            bits_in_buf += 8
            byte_pos += 1
        bits_in_buf -= width
        out[i] = ((buf >> bits_in_buf) & mask) + ref
    return out


class ALPCodec(Codec):
    name = "alp"

    def __init__(self, sample_fraction: float = 0.1) -> None:
        self.sample_fraction = sample_fraction

    def supports_dtype(self, dtype: np.dtype) -> bool:
        return dtype.kind == "f"

    def encode(self, data: np.ndarray) -> EncodedColumn:
        values = data.astype(np.float64)

        # Find best exponent on sample
        n_sample = max(64, int(len(values) * self.sample_fraction))
        rng = np.random.default_rng(42)
        idx = rng.choice(len(values), size=min(n_sample, len(values)), replace=False)
        sample = values[idx]
        e = _find_best_exponent(sample)

        factor = _POWERS[e]
        with np.errstate(invalid="ignore", over="ignore"):
            encoded = np.round(values * factor).astype(np.int64)
        reconstructed = encoded.astype(np.float64) / factor

        exc_mask = ~np.isclose(values, reconstructed, rtol=0, atol=1e-9)
        exc_positions = np.where(exc_mask)[0].astype(np.int32)
        exc_values = values[exc_mask]

        # Frame-of-reference + bitpack the int64 column
        if len(encoded) > 0:
            ref = int(encoded.min())
            width = _bitpack_width(encoded)
        else:
            ref, width = 0, 0

        packed = _bitpack(encoded, width, ref)

        # Serialise exceptions
        exc_bytes = struct.pack(f"<{len(exc_positions)}i", *exc_positions)
        exc_bytes += struct.pack(f"<{len(exc_values)}d", *exc_values)

        header = struct.pack("<HiiBBI", e, ref, len(data), width, len(exc_positions), len(exc_bytes))
        payload = header + packed + exc_bytes

        return EncodedColumn(
            codec_name=self.name,
            data=payload,
            metadata={"exponent": e, "exceptions": int(exc_mask.sum())},
            original_dtype=str(data.dtype),
            original_len=len(data),
        )

    def decode(self, encoded: EncodedColumn) -> np.ndarray:
        data = encoded.data
        header_size = struct.calcsize("<HiiBBI")
        e, ref, n, width, n_exc, exc_bytes_len = struct.unpack_from("<HiiBBI", data)

        packed_start = header_size
        packed_end = packed_start + (len(data) - header_size - exc_bytes_len)
        packed = data[packed_start:packed_end]
        exc_raw = data[packed_end:]

        int_vals = _bitunpack(packed, n, width, ref)
        factor = _POWERS[e]
        result = int_vals.astype(np.float64) / factor

        # Restore exceptions
        if n_exc > 0:
            exc_pos = struct.unpack_from(f"<{n_exc}i", exc_raw)
            exc_vals = struct.unpack_from(f"<{n_exc}d", exc_raw, n_exc * 4)
            for pos, val in zip(exc_pos, exc_vals):
                result[pos] = val

        return result.astype(encoded.original_dtype)
