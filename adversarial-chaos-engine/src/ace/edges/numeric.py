"""Numeric edge cases that break naive pipelines.

We deliberately separate ``int`` from ``float`` so a generator can sample
from one without accidentally widening a column type. Both lists include
the canonical "off by one past the boundary" values that random fuzzing
almost never produces on its own.
"""

from __future__ import annotations

import math
import sys

# Integer boundaries: signed-32, signed-64, off-by-one neighbours, and zero.
INT_EDGES: tuple[int, ...] = (
    0,
    1,
    -1,
    127,
    128,  # int8 overflow
    -128,
    -129,
    255,
    256,  # uint8 overflow
    32_767,
    32_768,  # int16 overflow
    -32_768,
    -32_769,
    65_535,
    65_536,  # uint16 overflow
    2**31 - 1,
    2**31,  # int32 overflow
    -(2**31),
    -(2**31) - 1,
    2**32 - 1,
    2**32,  # uint32 overflow
    2**63 - 1,
    -(2**63),
    sys.maxsize,
)

# IEEE-754 corner values.
FLOAT_EDGES: tuple[float, ...] = (
    0.0,
    -0.0,
    1.0,
    -1.0,
    math.inf,
    -math.inf,
    math.nan,
    sys.float_info.min,  # smallest positive normal
    -sys.float_info.min,
    sys.float_info.max,  # largest finite
    -sys.float_info.max,
    5e-324,  # smallest positive subnormal
    1.0 + sys.float_info.epsilon,  # one-ULP above 1
    1.0 - sys.float_info.epsilon / 2,  # just below 1
    1e-300,
    -1e-300,
)


def numeric_edges() -> list[float]:
    """All ``INT_EDGES`` and ``FLOAT_EDGES`` flattened into a single list."""
    return [float(x) for x in INT_EDGES] + list(FLOAT_EDGES)


__all__ = ["FLOAT_EDGES", "INT_EDGES", "numeric_edges"]
