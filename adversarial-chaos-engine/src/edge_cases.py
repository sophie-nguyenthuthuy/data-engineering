"""Library of edge-case seed values.

Static analysis would surface these — for now we hard-code well-known
boundary inputs that often trigger bugs.
"""
from __future__ import annotations

import math


# Numeric boundaries
INT_EDGES = [0, 1, -1,
             2**31 - 1, -(2**31), 2**31, 2**63 - 1, -(2**63)]

FLOAT_EDGES = [0.0, -0.0, 1.0, -1.0,
               math.inf, -math.inf, math.nan,
               1e-300, -1e-300,
               1.7976931348623157e+308, -1.7976931348623157e+308]

# String edge cases (Unicode, RTL, zero-width)
STRING_EDGES = [
    "",                  # empty
    " ",                 # single space
    "\x00",              # null byte
    "\n", "\r", "\t",
    "A" * 10_000,        # very long
    "café",              # combining char
    "‮",            # RTL override
    "​",            # zero-width space
    "﻿",            # BOM
    "👨‍👩‍👧",            # emoji ZWJ sequence
    "'); DROP TABLE x;--",
]

# DST boundary timestamps (Unix seconds, in NYC's 2024 spring forward)
DST_EDGES = [
    1710054000,          # 2024-03-10 06:00 UTC — start of US DST
    1710054001,
    1710053999,
    -2147483648,         # Y2038 lower bound
    2147483647,          # Y2038 upper bound
]


def numeric_edges():
    return list(INT_EDGES) + list(FLOAT_EDGES)


def string_edges():
    return list(STRING_EDGES)


def timestamp_edges():
    return list(DST_EDGES)


__all__ = ["numeric_edges", "string_edges", "timestamp_edges",
           "INT_EDGES", "FLOAT_EDGES", "STRING_EDGES", "DST_EDGES"]
