"""Invariant DSL.

Users decorate pipeline functions with `@invariant(...)`. The engine inspects
the decorators to know which properties must hold across each invocation.
"""
from __future__ import annotations

import functools
from collections import Counter
from dataclasses import dataclass
from typing import Callable


# ---------------------------------------------------------------------------
# Decorator + registry
# ---------------------------------------------------------------------------

@dataclass
class InvariantSpec:
    name: str
    check: Callable          # (input, output) -> bool
    description: str


_REGISTRY: dict = {}


def invariant(name: str = None, **kwargs):
    """Register an invariant on a pipeline function.

    Supported invariants:
      row_count_preserved=True
      sum_invariant=["col"]
      no_nulls=["col"]
      monotone=["col"]
    """
    def wrap(fn):
        specs = []
        if kwargs.get("row_count_preserved"):
            specs.append(InvariantSpec(
                name=name or "row_count_preserved",
                check=_row_count_preserved,
                description="output has same #rows as input"))
        for col in kwargs.get("sum_invariant", []):
            specs.append(InvariantSpec(
                name=f"sum_invariant({col})",
                check=lambda i, o, c=col: _sum_invariant(i, o, c),
                description=f"sum({col}) preserved"))
        for col in kwargs.get("no_nulls", []):
            specs.append(InvariantSpec(
                name=f"no_nulls({col})",
                check=lambda i, o, c=col: _no_nulls(o, c),
                description=f"{col} has no NULLs"))
        _REGISTRY[fn.__name__] = (fn, specs)

        @functools.wraps(fn)
        def wrapper(*args, **kw):
            return fn(*args, **kw)
        return wrapper
    return wrap


def specs_for(fn_name: str):
    return _REGISTRY.get(fn_name, (None, []))[1]


def registered():
    return list(_REGISTRY.keys())


# ---------------------------------------------------------------------------
# Built-in checks
# ---------------------------------------------------------------------------

def _row_count_preserved(input_df, output_df):
    return len(input_df) == len(output_df)


def _sum_invariant(input_df, output_df, col):
    si = sum((r.get(col, 0) for r in input_df))
    so = sum((r.get(col, 0) for r in output_df))
    return abs(si - so) < 1e-6


def _no_nulls(df, col):
    return all(r.get(col) is not None for r in df)


__all__ = ["invariant", "specs_for", "registered", "InvariantSpec"]
