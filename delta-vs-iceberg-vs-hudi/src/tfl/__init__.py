"""delta-vs-iceberg-vs-hudi — three mini table formats + workload comparison."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

if TYPE_CHECKING:
    from tfl.bench.compare import CompareReport, run_workload
    from tfl.bench.workload import CDCEvent, CDCOp, Workload
    from tfl.delta.action import Action, ActionType, FileEntry
    from tfl.delta.table import DeltaTable
    from tfl.hudi.table import HudiCoWTable, HudiMoRTable
    from tfl.iceberg.table import IcebergTable


_LAZY: dict[str, tuple[str, str]] = {
    "DeltaTable": ("tfl.delta.table", "DeltaTable"),
    "Action": ("tfl.delta.action", "Action"),
    "ActionType": ("tfl.delta.action", "ActionType"),
    "FileEntry": ("tfl.delta.action", "FileEntry"),
    "IcebergTable": ("tfl.iceberg.table", "IcebergTable"),
    "HudiCoWTable": ("tfl.hudi.table", "HudiCoWTable"),
    "HudiMoRTable": ("tfl.hudi.table", "HudiMoRTable"),
    "CDCEvent": ("tfl.bench.workload", "CDCEvent"),
    "CDCOp": ("tfl.bench.workload", "CDCOp"),
    "Workload": ("tfl.bench.workload", "Workload"),
    "CompareReport": ("tfl.bench.compare", "CompareReport"),
    "run_workload": ("tfl.bench.compare", "run_workload"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        from importlib import import_module

        m, attr = _LAZY[name]
        return getattr(import_module(m), attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "Action",
    "ActionType",
    "CDCEvent",
    "CDCOp",
    "CompareReport",
    "DeltaTable",
    "FileEntry",
    "HudiCoWTable",
    "HudiMoRTable",
    "IcebergTable",
    "Workload",
    "__version__",
    "run_workload",
]
