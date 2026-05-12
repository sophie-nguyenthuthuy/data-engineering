"""Workload generators for benchmarking."""

from __future__ import annotations

from disagg.workload.scan import scan_workload, zipf_workload
from disagg.workload.tpcc import tpcc_workload

__all__ = ["scan_workload", "tpcc_workload", "zipf_workload"]
