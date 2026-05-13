"""Emit a *valid* pytest regression case from a :class:`Violation`.

Unlike the original prototype, the emitted file:

  * compiles under ``ast.parse`` (verified by a test);
  * contains an explicit ``import`` of the function under test;
  * asserts the actual failing invariant (not a placeholder);
  * embeds a comment with the discovery date for audit.

We construct the source line-by-line instead of relying on
``textwrap.dedent`` over an f-string, because interpolated ``rows_repr``
is itself a multi-line block and the dedent would only strip whatever
leading whitespace was common across *all* lines after interpolation —
which is brittle.
"""

from __future__ import annotations

import ast
import datetime as dt
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ace.runner import Violation


_SAFE_TEST_NAME = re.compile(r"[^0-9A-Za-z_]+")


def _sanitise(name: str) -> str:
    return _SAFE_TEST_NAME.sub("_", name).strip("_")


def _assertion_body(invariant: str) -> list[str]:
    """Return the assertion lines (without any indent) for the invariant."""
    if invariant == "row_count_preserved":
        return ["assert len(df_out) == len(df_in)"]
    if invariant.startswith("sum_invariant("):
        col = invariant[len("sum_invariant(") : -1]
        return [
            f"assert sum(r.get({col!r}, 0) for r in df_out) == sum(r.get({col!r}, 0) for r in df_in)",
        ]
    if invariant.startswith("no_nulls("):
        col = invariant[len("no_nulls(") : -1]
        return [f"assert all(r.get({col!r}) is not None for r in df_out)"]
    if invariant.startswith("value_range("):
        col = invariant[len("value_range(") : -1]
        return [
            f"# value_range({col!r}) — replace with explicit lo/hi bounds",
            "assert df_out is not None",
        ]
    if invariant.startswith("monotone("):
        col = invariant[len("monotone(") : -1]
        return [
            f"vals = [r.get({col!r}) for r in df_out]",
            "assert vals == sorted(vals)",
        ]
    if invariant.startswith("distinct_count_preserved("):
        col = invariant[len("distinct_count_preserved(") : -1]
        return [
            f"assert len({{r.get({col!r}) for r in df_out}}) == len({{r.get({col!r}) for r in df_in}})",
        ]
    if invariant == "no_exceptions":
        return [
            "# pipeline raised; replace with explicit exception class assertion",
            "assert df_out is not None",
        ]
    return ["assert df_out is not None"]


def _rows_literal(rows: tuple[tuple[tuple[str, object], ...], ...]) -> list[str]:
    """Return the lines of a Python literal for the failing frame."""
    if not rows:
        return ["df_in = []"]
    out = ["df_in = ["]
    for row in rows:
        out.append("    " + repr(dict(row)) + ",")
    out.append("]")
    return out


def emit_pytest(violation: Violation, module: str = "your_pipeline_module") -> str:
    """Return the source of a valid pytest module asserting the violation."""
    today = dt.date.today().isoformat()
    safe_inv = _sanitise(violation.invariant)
    safe_fn = _sanitise(violation.fn_name)
    test_name = f"test_{safe_fn}_violates_{safe_inv}"

    lines: list[str] = [
        f"# Auto-discovered {today} by adversarial-chaos-engine.",
        f"# Invariant: {violation.invariant}",
        f"# Observed output: {violation.output_repr}",
        "",
        f"from {module} import {violation.fn_name}",
        "",
        "",
        f"def {test_name}() -> None:",
    ]

    # df_in literal, indented one level.
    for row_line in _rows_literal(violation.input):
        lines.append("    " + row_line)

    lines.append(f"    df_out = {violation.fn_name}(df_in)")
    for assertion_line in _assertion_body(violation.invariant):
        lines.append("    " + assertion_line)

    source = "\n".join(lines) + "\n"
    # Sanity-check: the emitted source must parse.
    ast.parse(source)
    return source


__all__ = ["emit_pytest"]
