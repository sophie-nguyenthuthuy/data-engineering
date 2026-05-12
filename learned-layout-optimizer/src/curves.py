"""Space-filling curves used as layout actions: Z-order and Hilbert.

These map d-dim integer coordinates to a 1-D index. Files are physically
sorted by the curve's index — improves locality for range queries that
touch a contiguous box in the original space.
"""
from __future__ import annotations

import numpy as np


def z_order_index(coords: np.ndarray, bits: int = 16) -> np.ndarray:
    """Interleave bits of each coordinate (Morton order)."""
    n, d = coords.shape
    out = np.zeros(n, dtype=np.uint64)
    for bit in range(bits):
        for dim in range(d):
            mask = ((coords[:, dim].astype(np.uint64) >> np.uint64(bit)) & np.uint64(1))
            out |= mask << np.uint64(bit * d + dim)
    return out


def hilbert_index(coords: np.ndarray, bits: int = 16) -> np.ndarray:
    """2-D Hilbert curve index (basic Skilling-style algorithm)."""
    n, d = coords.shape
    assert d == 2, "Hilbert variant in this module is 2-D"
    x = coords[:, 0].astype(np.int64).copy()
    y = coords[:, 1].astype(np.int64).copy()
    out = np.zeros(n, dtype=np.uint64)
    for s in range(bits - 1, -1, -1):
        s64 = np.int64(1 << s)
        rx = (x & s64) > 0
        ry = (y & s64) > 0
        out |= (((3 * rx.astype(np.uint64)) ^ ry.astype(np.uint64)) << np.uint64(2 * s))
        # Rotate
        flip = (ry == 0)
        flipx = flip & (rx == 1)
        new_x = np.where(flip, s64 - 1 - y, x)
        new_y = np.where(flip, s64 - 1 - x, y)
        x = np.where(flipx, new_x, new_x)
        y = np.where(flipx, new_y, new_y)
    return out


__all__ = ["z_order_index", "hilbert_index"]
