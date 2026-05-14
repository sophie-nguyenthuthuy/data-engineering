"""postgres-vs-clickhouse-benchmark — cross-engine query benchmark harness."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

if TYPE_CHECKING:
    from pvc.benchmark import BenchmarkRunner, IterationResult, QueryResult
    from pvc.engines.base import Engine, EngineError
    from pvc.engines.injectable import InjectableEngine
    from pvc.engines.sqlite import SQLiteEngine
    from pvc.report import ComparisonReport, ReportRow, build_comparison
    from pvc.stats import LatencyStats, summarise
    from pvc.workloads.base import Query, Workload
    from pvc.workloads.nytaxi import NY_TAXI_QUERIES
    from pvc.workloads.tpch import TPCH_QUERIES


_LAZY: dict[str, tuple[str, str]] = {
    "Query": ("pvc.workloads.base", "Query"),
    "Workload": ("pvc.workloads.base", "Workload"),
    "TPCH_QUERIES": ("pvc.workloads.tpch", "TPCH_QUERIES"),
    "NY_TAXI_QUERIES": ("pvc.workloads.nytaxi", "NY_TAXI_QUERIES"),
    "Engine": ("pvc.engines.base", "Engine"),
    "EngineError": ("pvc.engines.base", "EngineError"),
    "SQLiteEngine": ("pvc.engines.sqlite", "SQLiteEngine"),
    "InjectableEngine": ("pvc.engines.injectable", "InjectableEngine"),
    "LatencyStats": ("pvc.stats", "LatencyStats"),
    "summarise": ("pvc.stats", "summarise"),
    "BenchmarkRunner": ("pvc.benchmark", "BenchmarkRunner"),
    "QueryResult": ("pvc.benchmark", "QueryResult"),
    "IterationResult": ("pvc.benchmark", "IterationResult"),
    "ComparisonReport": ("pvc.report", "ComparisonReport"),
    "ReportRow": ("pvc.report", "ReportRow"),
    "build_comparison": ("pvc.report", "build_comparison"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        from importlib import import_module

        m, attr = _LAZY[name]
        return getattr(import_module(m), attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "NY_TAXI_QUERIES",
    "TPCH_QUERIES",
    "BenchmarkRunner",
    "ComparisonReport",
    "Engine",
    "EngineError",
    "InjectableEngine",
    "IterationResult",
    "LatencyStats",
    "Query",
    "QueryResult",
    "ReportRow",
    "SQLiteEngine",
    "Workload",
    "__version__",
    "build_comparison",
    "summarise",
]
