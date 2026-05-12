"""Physical operators per engine, with cost-model annotations."""
from __future__ import annotations

from dataclasses import dataclass, field


# Cost units: arbitrary; use relative comparisons. $ cost / minute.
@dataclass
class PhysicalOp:
    kind: str
    engine: str
    cost: float                     # estimated runtime (seconds-ish)
    bytes_out: float                # estimated output bytes
    children: list = field(default_factory=list)
    output_engine: str = ""        # which engine holds the data after this op

    def __post_init__(self):
        if not self.output_engine:
            self.output_engine = self.engine

    def __repr__(self):
        if self.children:
            c = ",".join(repr(ch) for ch in self.children)
            return f"{self.engine}.{self.kind}({c})"
        return f"{self.engine}.{self.kind}"


# Cost factors per engine, per byte input (these are not real numbers — they
# illustrate the trade-offs: warehouse engines are cheap per byte for SQL but
# have high setup; Spark scales well; DuckDB is great for local files; Flink
# is for streams):
ENGINE_COSTS = {
    "spark":   {"setup": 30.0, "per_byte": 1.0e-9, "per_byte_join": 5.0e-9},
    "dbt":     {"setup": 60.0, "per_byte": 0.5e-9, "per_byte_join": 2.0e-9},
    # DuckDB: super-fast setup but single-node; per-byte costs grow if working
    # set exceeds memory. We model this by penalising large aggregates/joins.
    "duckdb":  {"setup": 1.0,  "per_byte": 0.8e-9, "per_byte_join": 4.0e-9,
                "memory_cap_bytes": 10_000_000_000, "spill_multiplier": 20.0},
    "flink":   {"setup": 60.0, "per_byte": 1.5e-9, "per_byte_join": 6.0e-9},
}


def _engine_memory_penalty(engine: str, bytes_in: float) -> float:
    """Penalty factor applied to per_byte when working set exceeds memory."""
    spec = ENGINE_COSTS[engine]
    cap = spec.get("memory_cap_bytes")
    if cap is None or bytes_in <= cap:
        return 1.0
    return spec.get("spill_multiplier", 1.0)

# Cross-engine conversion costs
CONVERSION_COSTS = {
    ("spark", "dbt"):    20.0,   # write to S3 + register external table
    ("dbt", "spark"):    10.0,
    ("spark", "duckdb"):  5.0,
    ("duckdb", "spark"):  3.0,
    ("flink", "dbt"):    30.0,
    ("flink", "duckdb"): 20.0,
    ("dbt", "flink"):    25.0,
    ("dbt", "duckdb"):    5.0,
    ("duckdb", "dbt"):    8.0,
    ("spark", "flink"):  20.0,
    ("flink", "spark"):  20.0,
    ("duckdb", "flink"): 25.0,
}


def conversion_cost(src: str, dst: str) -> float:
    if src == dst:
        return 0.0
    return CONVERSION_COSTS.get((src, dst), 100.0)


def filter_cost(engine: str, bytes_in: float, selectivity: float) -> tuple[float, float]:
    e = ENGINE_COSTS[engine]
    # Filter is streamable — no spill penalty
    cost = e["setup"] + bytes_in * e["per_byte"]
    return cost, bytes_in * selectivity


def aggregate_cost(engine: str, bytes_in: float, group_card: float) -> tuple[float, float]:
    e = ENGINE_COSTS[engine]
    penalty = _engine_memory_penalty(engine, bytes_in)
    cost = e["setup"] + bytes_in * e["per_byte"] * penalty
    bytes_out = group_card * 100
    return cost, bytes_out


def join_cost(engine: str, lbytes: float, rbytes: float) -> tuple[float, float]:
    e = ENGINE_COSTS[engine]
    penalty = _engine_memory_penalty(engine, lbytes + rbytes)
    cost = e["setup"] + (lbytes + rbytes) * e["per_byte_join"] * penalty
    bytes_out = max(lbytes, rbytes) * 0.5
    return cost, bytes_out


__all__ = ["PhysicalOp", "ENGINE_COSTS", "conversion_cost",
           "filter_cost", "aggregate_cost", "join_cost"]
