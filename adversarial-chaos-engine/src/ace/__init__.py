"""adversarial-chaos-engine — targeted adversarial fuzzing for pipelines."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

if TYPE_CHECKING:
    from ace.bench import BenchmarkReport, run_benchmark
    from ace.edges.numeric import FLOAT_EDGES, INT_EDGES, numeric_edges
    from ace.edges.strings import STRING_EDGES, string_edges
    from ace.edges.timestamps import TIMESTAMP_EDGES, timestamp_edges
    from ace.generator import AdversarialGenerator
    from ace.invariants.catalog import Catalog, InvariantSpec, invariant
    from ace.invariants.checks import (
        column_no_nulls,
        column_value_range,
        distinct_count_preserved,
        monotone_increasing,
        row_count_preserved,
        sum_invariant,
    )
    from ace.regression import emit_pytest
    from ace.runner import Report, Runner, Violation
    from ace.shrinker import shrink_rows


_LAZY: dict[str, tuple[str, str]] = {
    "Catalog": ("ace.invariants.catalog", "Catalog"),
    "InvariantSpec": ("ace.invariants.catalog", "InvariantSpec"),
    "invariant": ("ace.invariants.catalog", "invariant"),
    "row_count_preserved": ("ace.invariants.checks", "row_count_preserved"),
    "sum_invariant": ("ace.invariants.checks", "sum_invariant"),
    "column_no_nulls": ("ace.invariants.checks", "column_no_nulls"),
    "column_value_range": ("ace.invariants.checks", "column_value_range"),
    "monotone_increasing": ("ace.invariants.checks", "monotone_increasing"),
    "distinct_count_preserved": ("ace.invariants.checks", "distinct_count_preserved"),
    "INT_EDGES": ("ace.edges.numeric", "INT_EDGES"),
    "FLOAT_EDGES": ("ace.edges.numeric", "FLOAT_EDGES"),
    "numeric_edges": ("ace.edges.numeric", "numeric_edges"),
    "STRING_EDGES": ("ace.edges.strings", "STRING_EDGES"),
    "string_edges": ("ace.edges.strings", "string_edges"),
    "TIMESTAMP_EDGES": ("ace.edges.timestamps", "TIMESTAMP_EDGES"),
    "timestamp_edges": ("ace.edges.timestamps", "timestamp_edges"),
    "AdversarialGenerator": ("ace.generator", "AdversarialGenerator"),
    "Runner": ("ace.runner", "Runner"),
    "Report": ("ace.runner", "Report"),
    "Violation": ("ace.runner", "Violation"),
    "shrink_rows": ("ace.shrinker", "shrink_rows"),
    "emit_pytest": ("ace.regression", "emit_pytest"),
    "BenchmarkReport": ("ace.bench", "BenchmarkReport"),
    "run_benchmark": ("ace.bench", "run_benchmark"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        from importlib import import_module

        module, attr = _LAZY[name]
        return getattr(import_module(module), attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "FLOAT_EDGES",
    "INT_EDGES",
    "STRING_EDGES",
    "TIMESTAMP_EDGES",
    "AdversarialGenerator",
    "BenchmarkReport",
    "Catalog",
    "InvariantSpec",
    "Report",
    "Runner",
    "Violation",
    "__version__",
    "column_no_nulls",
    "column_value_range",
    "distinct_count_preserved",
    "emit_pytest",
    "invariant",
    "monotone_increasing",
    "numeric_edges",
    "row_count_preserved",
    "run_benchmark",
    "shrink_rows",
    "string_edges",
    "sum_invariant",
    "timestamp_edges",
]
