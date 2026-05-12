"""Workload pattern definitions."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class WorkloadClass(str, Enum):
    OLAP_HEAVY = "olap_heavy"           # read-dominant, full scans, analytics
    OLTP_HEAVY = "oltp_heavy"           # write-dominant, point reads/updates
    MIXED = "mixed"                     # balanced read/write
    STREAMING_INGEST = "streaming_ingest"  # continuous small inserts, rare reads
    BATCH_UPDATE = "batch_update"       # periodic large-batch updates
    CDC = "cdc"                         # change-data-capture: many small updates + deletes


@dataclass(frozen=True)
class WorkloadProfile:
    name: str
    cls: WorkloadClass
    insert_weight: float      # fraction of ops that are inserts
    update_weight: float
    delete_weight: float
    full_scan_weight: float
    point_read_weight: float
    range_scan_weight: float
    rows_per_write: int       # avg rows per write operation
    update_fraction: float    # fraction of table updated per update op


# Canonical workload profiles
PROFILES: dict[str, WorkloadProfile] = {
    "olap": WorkloadProfile(
        name="olap",
        cls=WorkloadClass.OLAP_HEAVY,
        insert_weight=0.05,
        update_weight=0.02,
        delete_weight=0.01,
        full_scan_weight=0.60,
        point_read_weight=0.12,
        range_scan_weight=0.20,
        rows_per_write=10_000,
        update_fraction=0.02,
    ),
    "oltp": WorkloadProfile(
        name="oltp",
        cls=WorkloadClass.OLTP_HEAVY,
        insert_weight=0.25,
        update_weight=0.45,
        delete_weight=0.10,
        full_scan_weight=0.02,
        point_read_weight=0.15,
        range_scan_weight=0.03,
        rows_per_write=50,
        update_fraction=0.05,
    ),
    "mixed": WorkloadProfile(
        name="mixed",
        cls=WorkloadClass.MIXED,
        insert_weight=0.15,
        update_weight=0.20,
        delete_weight=0.05,
        full_scan_weight=0.25,
        point_read_weight=0.20,
        range_scan_weight=0.15,
        rows_per_write=1_000,
        update_fraction=0.10,
    ),
    "streaming": WorkloadProfile(
        name="streaming",
        cls=WorkloadClass.STREAMING_INGEST,
        insert_weight=0.85,
        update_weight=0.05,
        delete_weight=0.01,
        full_scan_weight=0.03,
        point_read_weight=0.04,
        range_scan_weight=0.02,
        rows_per_write=500,
        update_fraction=0.01,
    ),
    "batch_update": WorkloadProfile(
        name="batch_update",
        cls=WorkloadClass.BATCH_UPDATE,
        insert_weight=0.05,
        update_weight=0.70,
        delete_weight=0.10,
        full_scan_weight=0.08,
        point_read_weight=0.02,
        range_scan_weight=0.05,
        rows_per_write=50_000,
        update_fraction=0.30,
    ),
    "cdc": WorkloadProfile(
        name="cdc",
        cls=WorkloadClass.CDC,
        insert_weight=0.30,
        update_weight=0.50,
        delete_weight=0.15,
        full_scan_weight=0.01,
        point_read_weight=0.03,
        range_scan_weight=0.01,
        rows_per_write=200,
        update_fraction=0.08,
    ),
}
