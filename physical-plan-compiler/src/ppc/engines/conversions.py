"""Cross-engine conversion costs.

A conversion = materializing the data in a format the destination engine can
read. Real options:
  - Spark   → dbt    : write to S3 Parquet, register external table
  - DuckDB  → Spark  : export local Parquet, upload, Spark reads
  - dbt    → DuckDB  : warehouse export to Parquet, DuckDB reads via httpfs
  - Flink   → dbt    : Kafka sink → batch dump → external table

Costs are fixed setup + linear per-byte.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class ConversionEdge:
    src: str
    dst: str
    setup: float          # cost units
    per_byte: float


@dataclass
class ConversionRegistry:
    edges: dict[tuple[str, str], ConversionEdge] = field(default_factory=dict)

    def register(self, edge: ConversionEdge) -> None:
        self.edges[(edge.src, edge.dst)] = edge

    def cost(self, src: str, dst: str, bytes_in: float) -> float:
        if src == dst:
            return 0.0
        e = self.edges.get((src, dst))
        if e is None:
            # Unknown → very expensive default
            return 1000.0 + bytes_in * 1e-8
        return e.setup + bytes_in * e.per_byte


def default_conversion_registry() -> ConversionRegistry:
    reg = ConversionRegistry()
    for src, dst, setup, per_byte in [
        ("spark",  "dbt",     20.0, 2.0e-9),
        ("dbt",    "spark",   10.0, 1.5e-9),
        ("spark",  "duckdb",   5.0, 2.0e-9),
        ("duckdb", "spark",    3.0, 1.0e-9),
        ("spark",  "flink",   20.0, 3.0e-9),
        ("flink",  "spark",   20.0, 3.0e-9),
        ("dbt",    "duckdb",   5.0, 2.0e-9),
        ("duckdb", "dbt",      8.0, 2.5e-9),
        ("dbt",    "flink",   25.0, 4.0e-9),
        ("flink",  "dbt",     30.0, 4.0e-9),
        ("duckdb", "flink",   25.0, 4.0e-9),
        ("flink",  "duckdb",  20.0, 3.0e-9),
    ]:
        reg.register(ConversionEdge(src=src, dst=dst, setup=setup, per_byte=per_byte))
    return reg
