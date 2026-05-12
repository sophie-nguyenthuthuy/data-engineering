"""Physical Plan Compiler — Cascades-style cross-engine query optimizer.

Public API:
    from ppc import compile_sql, Optimizer
    compile_sql(sql, catalog) -> PhysicalPlan       # one-shot SQL → physical plan
    Optimizer(catalog).optimize(logical) -> PhysicalPlan
"""

from __future__ import annotations

__version__ = "0.1.0"

# Lazy re-exports: avoid importing heavy modules at package import time.
# Real names resolve via __getattr__ for both convenience and to keep
# `import ppc.ir` working before the rest of the package is built.

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - type-checking only
    from ppc.cascades.optimizer import Optimizer
    from ppc.frontend.sql import compile_sql
    from ppc.ir.logical import (
        LogicalAggregate,
        LogicalFilter,
        LogicalJoin,
        LogicalScan,
    )
    from ppc.ir.physical import PhysicalPlan


_LAZY_EXPORTS: dict[str, str] = {
    "Optimizer": "ppc.cascades.optimizer",
    "compile_sql": "ppc.frontend.sql",
    "PhysicalPlan": "ppc.ir.physical",
    "LogicalScan": "ppc.ir.logical",
    "LogicalFilter": "ppc.ir.logical",
    "LogicalAggregate": "ppc.ir.logical",
    "LogicalJoin": "ppc.ir.logical",
}


def __getattr__(name: str) -> Any:
    mod_path = _LAZY_EXPORTS.get(name)
    if mod_path is None:
        raise AttributeError(f"module 'ppc' has no attribute {name!r}")
    import importlib

    mod = importlib.import_module(mod_path)
    return getattr(mod, name)


__all__ = [
    "Optimizer",
    "PhysicalPlan",
    "LogicalAggregate",
    "LogicalFilter",
    "LogicalJoin",
    "LogicalScan",
    "compile_sql",
]
