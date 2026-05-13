"""Builtin invariant checks.

Each invariant is a callable ``check(input_frame, output_frame) → bool``
plus enough metadata to (a) name the violation in regression output and
(b) tell the adversarial generator which columns the invariant cares
about, so input synthesis can be biased toward the columns that
matter.

Supported invariants:

  * ``row_count_preserved=True`` — ``len(out) == len(in)``.
  * ``sum_invariant=["col"]`` — ``sum(in[col]) == sum(out[col])``.
  * ``no_nulls=["col"]`` — every row in ``out`` has a non-``None`` value
    for ``col``.
  * ``value_range={"col": (lo, hi)}`` — every value in ``out[col]`` lies
    in ``[lo, hi]``.
  * ``monotone=["col"]`` — values of ``out[col]`` are weakly increasing.
  * ``distinct_count_preserved=["col"]`` — number of distinct ``col``
    values is the same in input and output.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ace.invariants.catalog import Frame, InvariantSpec


# --------------------------------------------------------------- check fns


def row_count_preserved(in_frame: Frame, out_frame: Frame) -> bool:
    return len(in_frame) == len(out_frame)


def sum_invariant(col: str) -> Any:
    """Closure-builder: returns a check that pins ``sum(col)`` across in/out."""

    def _check(in_frame: Frame, out_frame: Frame) -> bool:
        si = _sum_numeric(in_frame, col)
        so = _sum_numeric(out_frame, col)
        if math.isnan(si) or math.isnan(so):
            return False
        return math.isclose(si, so, rel_tol=1e-9, abs_tol=1e-9)

    return _check


def column_no_nulls(col: str) -> Any:
    def _check(_in: Frame, out_frame: Frame) -> bool:
        return all(r.get(col) is not None for r in out_frame)

    return _check


def column_value_range(col: str, lo: float, hi: float) -> Any:
    if hi < lo:
        raise ValueError(f"value_range hi {hi} < lo {lo}")

    def _check(_in: Frame, out_frame: Frame) -> bool:
        for r in out_frame:
            v = r.get(col)
            if not isinstance(v, int | float):
                return False
            if math.isnan(float(v)):
                return False
            if not (lo <= float(v) <= hi):
                return False
        return True

    return _check


def monotone_increasing(col: str) -> Any:
    def _check(_in: Frame, out_frame: Frame) -> bool:
        prev: float | None = None
        for r in out_frame:
            v = r.get(col)
            if not isinstance(v, int | float):
                return False
            if prev is not None and float(v) < prev:
                return False
            prev = float(v)
        return True

    return _check


def distinct_count_preserved(col: str) -> Any:
    def _check(in_frame: Frame, out_frame: Frame) -> bool:
        return _distinct(in_frame, col) == _distinct(out_frame, col)

    return _check


# ----------------------------------------------------------- spec builder


def build_specs_from_kwargs(kwargs: dict[str, Any]) -> Iterable[InvariantSpec]:
    """Translate the decorator kwargs into :class:`InvariantSpec` objects."""
    from ace.invariants.catalog import InvariantSpec

    specs: list[InvariantSpec] = []

    if kwargs.get("row_count_preserved"):
        specs.append(
            InvariantSpec(
                name="row_count_preserved",
                check=row_count_preserved,
                description="output has the same number of rows as input",
                columns=(),
            )
        )

    for col in kwargs.get("sum_invariant", []) or []:
        specs.append(
            InvariantSpec(
                name=f"sum_invariant({col})",
                check=sum_invariant(col),
                description=f"sum of {col!r} is preserved",
                columns=(col,),
            )
        )

    for col in kwargs.get("no_nulls", []) or []:
        specs.append(
            InvariantSpec(
                name=f"no_nulls({col})",
                check=column_no_nulls(col),
                description=f"output's {col!r} has no nulls",
                columns=(col,),
            )
        )

    for col, (lo, hi) in (kwargs.get("value_range") or {}).items():
        specs.append(
            InvariantSpec(
                name=f"value_range({col})",
                check=column_value_range(col, lo, hi),
                description=f"every {col!r} lies in [{lo}, {hi}]",
                columns=(col,),
            )
        )

    for col in kwargs.get("monotone", []) or []:
        specs.append(
            InvariantSpec(
                name=f"monotone({col})",
                check=monotone_increasing(col),
                description=f"{col!r} is weakly increasing in the output",
                columns=(col,),
            )
        )

    for col in kwargs.get("distinct_count_preserved", []) or []:
        specs.append(
            InvariantSpec(
                name=f"distinct_count_preserved({col})",
                check=distinct_count_preserved(col),
                description=f"distinct {col!r} count preserved",
                columns=(col,),
            )
        )

    return specs


# ------------------------------------------------------------------ helpers


def _sum_numeric(frame: Frame, col: str) -> float:
    """Sum numeric values; propagate NaN so sum_invariant can reject NaN-y inputs."""
    total = 0.0
    for r in frame:
        v = r.get(col)
        if isinstance(v, int | float):
            fv = float(v)
            if math.isnan(fv):
                return math.nan
            total += fv
    return total


def _distinct(frame: Frame, col: str) -> int:
    return len({r.get(col) for r in frame})


__all__ = [
    "build_specs_from_kwargs",
    "column_no_nulls",
    "column_value_range",
    "distinct_count_preserved",
    "monotone_increasing",
    "row_count_preserved",
    "sum_invariant",
]
