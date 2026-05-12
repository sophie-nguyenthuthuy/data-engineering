"""Temporal Join Engine — AS OF joins for out-of-order event streams."""

from .event import Event, JoinResult, STREAM_LEFT, STREAM_RIGHT
from .interval_tree import IntervalTree
from .join_engine import AsOfJoinEngine
from .watermark import WatermarkTracker

__all__ = [
    "AsOfJoinEngine",
    "Event",
    "IntervalTree",
    "JoinResult",
    "STREAM_LEFT",
    "STREAM_RIGHT",
    "WatermarkTracker",
]
