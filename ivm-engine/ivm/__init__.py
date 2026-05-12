"""Incremental View Maintenance Engine.

Quick start
-----------
    from ivm import IVMEngine
    import ivm.aggregates as agg
"""
from ivm.engine import IVMEngine
from ivm import aggregates as agg
from ivm.operators import (
    TumblingWindow,
    SlidingWindow,
    PartitionWindow,
)

__all__ = [
    "IVMEngine",
    "agg",
    "TumblingWindow",
    "SlidingWindow",
    "PartitionWindow",
]

__version__ = "0.1.0"
