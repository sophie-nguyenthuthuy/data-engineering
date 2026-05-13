"""Space-filling curves used as layout actions.

Z-order (Morton, 1966): interleave the bits of each coordinate. Gives a 1-D
key whose contiguous ranges cover *square* boxes in d-D space, modulo the
well-known "jumps" between quadrants.

Hilbert curve (Hilbert, 1891): also maps d-D to 1-D, but every consecutive
pair of keys touches in d-D. Locality is strictly better than Z-order at
the cost of slightly more computation.

Both functions accept arrays of non-negative integer coordinates of shape
(n, d) and return an ``np.uint64`` array of length n. Coordinates must
fit in ``bits`` bits per dimension.

Reference for N-D Hilbert: Skilling (2004), "Programming the Hilbert
curve", AIP Conf. Proc. 707, 381–387.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from numpy.typing import NDArray


def _validate_coords(coords: NDArray[np.integer], bits: int) -> None:
    if coords.ndim != 2:
        raise ValueError(f"coords must be 2-D (got shape {coords.shape})")
    if bits < 1 or bits > 32:
        raise ValueError(f"bits must be in [1, 32] (got {bits})")
    if coords.size and coords.min() < 0:
        raise ValueError("coordinates must be non-negative")
    if coords.size and int(coords.max()) >= (1 << bits):
        raise ValueError(f"coordinate exceeds 2^{bits}; increase bits")


def z_order_index(coords: NDArray[np.integer], bits: int = 16) -> NDArray[np.uint64]:
    """Morton-interleaved 1-D index for non-negative integer coordinates.

    Layout-friendly property: rows with consecutive ``z_order_index`` values
    are close in *every* dimension. Consecutive in 1-D ⇏ adjacent in d-D
    (Z curve "jumps") — but the average locality is good for moderate d.
    """
    _validate_coords(coords, bits)
    n, d = coords.shape
    out = np.zeros(n, dtype=np.uint64)
    c64 = coords.astype(np.uint64, copy=False)
    for bit in range(bits):
        mask = np.uint64(1) << np.uint64(bit)
        for dim in range(d):
            shift = np.uint64(bit * d + dim)
            out |= ((c64[:, dim] & mask) >> np.uint64(bit)) << shift
    return out


def hilbert_index(coords: NDArray[np.integer], bits: int = 16) -> NDArray[np.uint64]:
    """Classical 2-D Hilbert curve index.

    Implements the iterative bit-by-bit rotate/reflect algorithm, vectorised
    across rows. Output is monotone along the Hilbert traversal of a
    ``2^bits × 2^bits`` grid.
    """
    _validate_coords(coords, bits)
    n, d = coords.shape
    if d != 2:
        raise ValueError(f"hilbert_index is 2-D only (got d={d}); use hilbert_index_nd")
    x = coords[:, 0].astype(np.int64).copy()
    y = coords[:, 1].astype(np.int64).copy()
    out = np.zeros(n, dtype=np.uint64)
    for s_int in range(bits - 1, -1, -1):
        s = np.int64(1 << s_int)
        rx = ((x & s) > 0).astype(np.int64)
        ry = ((y & s) > 0).astype(np.int64)
        out |= np.uint64(((3 * rx) ^ ry).astype(np.uint64)) << np.uint64(2 * s_int)
        # Reflect / rotate quadrant 0 and quadrant 3.
        flip = ry == 0
        new_x = np.where(flip & (rx == 1), s - 1 - y, np.where(flip, y, x))
        new_y = np.where(flip & (rx == 1), s - 1 - x, np.where(flip, x, y))
        x, y = new_x, new_y
    return out


def hilbert_index_nd(coords: NDArray[np.integer], bits: int = 16) -> NDArray[np.uint64]:
    """N-dimensional Hilbert index via Skilling's transposed-axes algorithm.

    Works for any ``d ≥ 1``. For ``d == 2`` it agrees with
    :func:`hilbert_index`. Keys span ``[0, 2^(d·bits))``.
    """
    _validate_coords(coords, bits)
    n, d = coords.shape
    if d * bits > 63:
        raise ValueError(f"d*bits = {d * bits} exceeds 63 (cannot fit in uint64)")
    x = coords.astype(np.int64, copy=True)

    # 1. Forward Gray-coded undo (Skilling's "untransposed" step).
    m = np.int64(1) << (bits - 1)
    q = m
    while q > 1:
        p = q - 1
        for i in range(d):
            mask = (x[:, i] & q) > 0
            if mask.any():
                x[mask, 0] ^= p  # exchange low bits
            no = ~mask
            if no.any():
                # swap low bits of x[:, i] and x[:, 0]
                t = (x[no, 0] ^ x[no, i]) & p
                x[no, 0] ^= t
                x[no, i] ^= t
        q >>= 1

    # 2. Gray encode.
    for i in range(1, d):
        x[:, i] ^= x[:, i - 1]
    t = np.zeros(n, dtype=np.int64)
    q = m
    while q > 1:
        mask = (x[:, d - 1] & q) > 0
        t[mask] = q - 1
        q >>= 1
    for i in range(d):
        x[:, i] ^= t

    # 3. Interleave into a single uint64 key (msb of dim 0 first).
    out = np.zeros(n, dtype=np.uint64)
    for bit in range(bits):
        src_bit = bits - 1 - bit
        for dim in range(d):
            bit_val = ((x[:, dim] >> src_bit) & 1).astype(np.uint64)
            shift = np.uint64((bits - 1 - bit) * d + (d - 1 - dim))
            out |= bit_val << shift
    return out


__all__ = ["hilbert_index", "hilbert_index_nd", "z_order_index"]
