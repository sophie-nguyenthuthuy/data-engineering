"""Gorilla encoding for time-series data.

Two sub-codecs
--------------
GorillaFloat  – XOR-delta compression for float64 columns.
               (Gorilla VLDB 2015, §4.1, value stream)

GorillaDelta  – Delta-of-delta compression for monotone integer columns
               (timestamps, sequence numbers).
               (Gorilla VLDB 2015, §4.1, timestamp stream)
"""
from __future__ import annotations

import struct

import numpy as np

from ._bits import BitReader, BitWriter
from .base import Codec, EncodedColumn


# ---------------------------------------------------------------------------
# GorillaFloat
# ---------------------------------------------------------------------------

class GorillaFloatCodec(Codec):
    name = "gorilla_float"

    def supports_dtype(self, dtype: np.dtype) -> bool:
        return dtype.kind == "f"

    def encode(self, data: np.ndarray) -> EncodedColumn:
        values = data.astype(np.float64)
        n = len(values)
        bw = BitWriter()
        bw.write_bits(n, 32)

        if n == 0:
            return EncodedColumn(codec_name=self.name, data=bw.finish(),
                                 original_dtype=str(data.dtype), original_len=n)

        def f2i(f: float) -> int:
            return struct.unpack("<Q", struct.pack("<d", f))[0]

        prev = f2i(float(values[0]))
        bw.write_bits(prev, 64)

        # Track previous block: (leading_zeros, meaningful_bits)
        prev_lz = -1
        prev_mb = -1

        for i in range(1, n):
            cur = f2i(float(values[i]))
            xor = prev ^ cur

            if xor == 0:
                bw.write_bit(0)
            else:
                bw.write_bit(1)
                # Count leading zeros (cap at 63 so they fit in 6 bits)
                lz = min(63, 64 - xor.bit_length())
                # Count trailing zeros
                tz = 0
                tmp = xor
                while tmp & 1 == 0:
                    tz += 1
                    tmp >>= 1
                mb = 64 - lz - tz  # meaningful bits (1..64)

                # Reuse previous block when current XOR fits entirely within it
                reuse = (
                    prev_lz >= 0
                    and lz >= prev_lz
                    and tz >= (64 - prev_lz - prev_mb)
                )
                if reuse:
                    bw.write_bit(0)
                    prev_tz = 64 - prev_lz - prev_mb
                    relevant = (xor >> prev_tz) & ((1 << prev_mb) - 1)
                    bw.write_bits(relevant, prev_mb)
                else:
                    bw.write_bit(1)
                    bw.write_bits(lz, 6)        # 6 bits: leading zeros 0-63
                    bw.write_bits(mb - 1, 6)    # 6 bits: meaningful bits-1 (range 0-63, represents 1-64)
                    bw.write_bits(xor >> tz, mb)
                    prev_lz = lz
                    prev_mb = mb

            prev = cur

        return EncodedColumn(
            codec_name=self.name,
            data=bw.finish(),
            original_dtype=str(data.dtype),
            original_len=n,
        )

    def decode(self, encoded: EncodedColumn) -> np.ndarray:
        br = BitReader(encoded.data)
        n = br.read_bits(32)
        if n == 0:
            return np.array([], dtype=encoded.original_dtype)

        def i2f(b: int) -> float:
            return struct.unpack("<d", struct.pack("<Q", b & 0xFFFF_FFFF_FFFF_FFFF))[0]

        result = np.empty(n, dtype=np.float64)
        prev = br.read_bits(64)
        result[0] = i2f(prev)
        prev_lz = prev_mb = -1

        for i in range(1, n):
            if br.read_bit() == 0:
                result[i] = result[i - 1]
            else:
                if br.read_bit() == 0:
                    # Reuse previous block
                    lz, mb = prev_lz, prev_mb
                else:
                    lz = br.read_bits(6)
                    mb = br.read_bits(6) + 1    # stored as mb-1
                    prev_lz, prev_mb = lz, mb

                tz = 64 - lz - mb
                meaningful = br.read_bits(mb)
                xor = meaningful << tz
                cur = prev ^ xor
                result[i] = i2f(cur)
                prev = cur

        return result.astype(encoded.original_dtype)


# ---------------------------------------------------------------------------
# GorillaDelta (delta-of-delta for integers / timestamps)
# ---------------------------------------------------------------------------

def _write_dod(bw: BitWriter, dod: int) -> None:
    """Variable-length encoding of delta-of-delta."""
    if dod == 0:
        bw.write_bit(0)
    elif -63 <= dod <= 64:
        bw.write_bit(1); bw.write_bit(0)
        bw.write_bits(dod + 63, 7)   # offset 63 → range [0,127]
    elif -255 <= dod <= 256:
        bw.write_bit(1); bw.write_bit(1); bw.write_bit(0)
        bw.write_bits(dod + 255, 9)  # offset 255 → range [0,511]
    elif -2047 <= dod <= 2048:
        bw.write_bit(1); bw.write_bit(1); bw.write_bit(1); bw.write_bit(0)
        bw.write_bits(dod + 2047, 12)
    else:
        bw.write_bit(1); bw.write_bit(1); bw.write_bit(1); bw.write_bit(1)
        bw.write_bits(dod & 0xFFFF_FFFF_FFFF_FFFF, 64)


def _read_dod(br: BitReader) -> int:
    b0 = br.read_bit()
    if b0 == 0:
        return 0
    b1 = br.read_bit()
    if b1 == 0:
        return br.read_bits(7) - 63
    b2 = br.read_bit()
    if b2 == 0:
        return br.read_bits(9) - 255
    b3 = br.read_bit()
    if b3 == 0:
        return br.read_bits(12) - 2047
    raw = br.read_bits(64)
    if raw >= (1 << 63):
        raw -= 1 << 64
    return raw


class GorillaDeltaCodec(Codec):
    """Delta-of-delta encoding for monotone integer columns (e.g. timestamps)."""
    name = "gorilla_delta"

    def supports_dtype(self, dtype: np.dtype) -> bool:
        return dtype.kind in ("i", "u")

    def encode(self, data: np.ndarray) -> EncodedColumn:
        values = data.astype(np.int64)
        n = len(values)
        bw = BitWriter()
        bw.write_bits(n, 32)

        if n == 0:
            return EncodedColumn(codec_name=self.name, data=bw.finish(),
                                 original_dtype=str(data.dtype), original_len=n)

        # Store first value as-is (64 bits)
        v0 = int(values[0])
        bw.write_bits(v0 & 0xFFFF_FFFF_FFFF_FFFF, 64)

        if n == 1:
            return EncodedColumn(codec_name=self.name, data=bw.finish(),
                                 original_dtype=str(data.dtype), original_len=n)

        # Store first delta as-is (64 bits)
        prev_delta = int(values[1]) - v0
        bw.write_bits(prev_delta & 0xFFFF_FFFF_FFFF_FFFF, 64)

        for i in range(2, n):
            delta = int(values[i]) - int(values[i - 1])
            dod = delta - prev_delta
            _write_dod(bw, dod)
            prev_delta = delta

        return EncodedColumn(
            codec_name=self.name,
            data=bw.finish(),
            original_dtype=str(data.dtype),
            original_len=n,
        )

    def decode(self, encoded: EncodedColumn) -> np.ndarray:
        br = BitReader(encoded.data)
        n = br.read_bits(32)
        if n == 0:
            return np.array([], dtype=encoded.original_dtype)

        result = np.empty(n, dtype=np.int64)

        raw0 = br.read_bits(64)
        if raw0 >= (1 << 63):
            raw0 -= 1 << 64
        result[0] = raw0

        if n == 1:
            return result.astype(encoded.original_dtype)

        raw_delta = br.read_bits(64)
        if raw_delta >= (1 << 63):
            raw_delta -= 1 << 64
        prev_delta = raw_delta
        result[1] = result[0] + prev_delta

        for i in range(2, n):
            dod = _read_dod(br)
            delta = prev_delta + dod
            result[i] = result[i - 1] + delta
            prev_delta = delta

        return result.astype(encoded.original_dtype)
