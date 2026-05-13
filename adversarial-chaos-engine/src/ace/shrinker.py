"""Counterexample shrinker for failing frames.

Given a frame ``rows`` that violates ``check``, find a smaller frame
that still violates it. We use Zeller-style delta debugging: repeatedly
try removing one row at a time; keep any subset that still fails.

The result is a minimal-in-rows counterexample; column shrinking would
be a useful follow-on but is intentionally out of scope here so the
shrinker stays predictable and fast.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from ace.invariants.catalog import Frame, Pipeline


def shrink_rows(
    fn: Pipeline,
    rows: Frame,
    check: Callable[[Frame, Frame], bool],
    *,
    max_passes: int = 50,
) -> Frame:
    """Iteratively drop rows while ``check`` still reports failure.

    The contract: ``check(input, output)`` must return ``False`` on
    the failing input. We return the smallest sub-multiset of ``rows``
    for which ``check`` is still ``False``.
    """
    if max_passes < 1:
        raise ValueError("max_passes must be ≥ 1")
    # Bail out on inputs the check is already happy with — there's
    # nothing to shrink.
    current: Frame = list(rows)
    try:
        if check(current, fn(current)):
            return current
    except Exception:
        # An exception counts as a failure; we still attempt to shrink.
        pass

    for _ in range(max_passes):
        progress = False
        for idx in range(len(current)):
            candidate = current[:idx] + current[idx + 1 :]
            if not candidate:
                continue
            try:
                if not check(candidate, fn(candidate)):
                    current = candidate
                    progress = True
                    break
            except Exception:
                current = candidate
                progress = True
                break
        if not progress:
            break
    return current


__all__ = ["shrink_rows"]
