"""Per-engine cost profile.

Calibrated by observation on real systems. All numbers are *relative*
runtime estimates (in arbitrary "cost units") + $ cost factors. The
absolute scale isn't important; only ratios matter for plan selection.

These defaults reflect rough community-known performance properties:
  - DuckDB: very fast on a single node; cheap setup; struggles when working
    set exceeds memory.
  - Spark: heavy setup; scales linearly with parallelism for I/O-bound.
  - dbt: warehouse SQL; high setup (cluster spinup), cheap per-byte once warm.
  - Flink: streaming-optimised; high setup; per-byte similar to Spark.
"""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class EngineProfile:
    """Per-engine cost calibration."""

    name: str
    setup_cost: float          # cost units to start a job/cluster
    per_byte_scan: float       # cost/byte for sequential scan
    per_byte_filter: float
    per_byte_agg: float
    per_byte_join: float       # cost/byte for hash join (both sides)
    memory_cap_bytes: float = float("inf")     # spill threshold
    spill_multiplier: float = 1.0              # cost multiplier when over cap
    dollar_per_cost_unit: float = 1e-4         # rough $ / cost unit

    def cost_with_memory(self, bytes_in: float, per_byte: float) -> float:
        """Apply spill penalty if working set exceeds cap."""
        penalty = self.spill_multiplier if bytes_in > self.memory_cap_bytes else 1.0
        return bytes_in * per_byte * penalty


ENGINE_PROFILES: dict[str, EngineProfile] = {
    "spark": EngineProfile(
        name="spark",
        setup_cost=30.0,
        per_byte_scan=1.0e-9,
        per_byte_filter=0.5e-9,
        per_byte_agg=1.5e-9,
        per_byte_join=5.0e-9,
        memory_cap_bytes=200e9,   # generous (cluster)
        spill_multiplier=1.5,
        dollar_per_cost_unit=2e-4,
    ),
    "dbt": EngineProfile(
        name="dbt",
        setup_cost=60.0,           # warehouse spinup
        per_byte_scan=0.5e-9,
        per_byte_filter=0.3e-9,
        per_byte_agg=0.8e-9,
        per_byte_join=2.0e-9,
        memory_cap_bytes=1e12,     # warehouse is huge
        spill_multiplier=1.2,
        dollar_per_cost_unit=5e-4, # warehouse $ are higher
    ),
    "duckdb": EngineProfile(
        name="duckdb",
        setup_cost=1.0,
        per_byte_scan=0.8e-9,
        per_byte_filter=0.4e-9,
        per_byte_agg=1.0e-9,
        per_byte_join=4.0e-9,
        memory_cap_bytes=8e9,      # single laptop, ~8 GB working set
        spill_multiplier=20.0,     # spills hurt a lot
        dollar_per_cost_unit=1e-5, # ~free (laptop)
    ),
    "flink": EngineProfile(
        name="flink",
        setup_cost=60.0,
        per_byte_scan=1.5e-9,
        per_byte_filter=0.7e-9,
        per_byte_agg=2.0e-9,
        per_byte_join=6.0e-9,
        memory_cap_bytes=100e9,
        spill_multiplier=1.5,
        dollar_per_cost_unit=3e-4,
    ),
}


class EngineOp(ABC):
    """Marker base for physical ops; concrete ops in physical_ops.py."""

    @property
    def engine(self) -> str:                           # pragma: no cover
        raise NotImplementedError

    @property
    def bytes_out(self) -> float:                      # pragma: no cover
        raise NotImplementedError
