"""Timestamp edge cases: DST boundaries, Y2K38, leap second, Unix-epoch zero."""

from __future__ import annotations

# Unix-seconds values chosen to exercise common timestamp bugs.
TIMESTAMP_EDGES: tuple[int, ...] = (
    0,  # Unix epoch
    -1,  # 1 second before epoch
    1_710_054_000,  # 2024-03-10 07:00 UTC — US spring-forward boundary
    1_710_054_001,
    1_710_053_999,
    1_730_577_600,  # 2024-11-03 04:00 UTC — US fall-back boundary
    -2_147_483_648,  # int32 lower bound
    2_147_483_647,  # int32 upper bound (Y2K38)
    2_147_483_648,  # one past Y2K38
    1_483_228_800,  # 2017-01-01 00:00 UTC (post leap-second 2016-12-31)
    1_483_228_799,  # 2016-12-31 23:59:59 UTC (leap-second adjustment)
    253_402_300_799,  # 9999-12-31 23:59:59 UTC (max year in many libs)
)


def timestamp_edges() -> list[int]:
    return list(TIMESTAMP_EDGES)


__all__ = ["TIMESTAMP_EDGES", "timestamp_edges"]
