"""Vector clocks for per-entity causal consistency.

A *vector clock* for an entity is a mapping ``component → counter``. The
component-wise order it induces is a partial order:

    a ≤ b   iff   ∀c. a[c] ≤ b[c]
    a < b   iff   a ≤ b ∧ a ≠ b
    a ‖ b   (concurrent) otherwise

Lamport (1978) used these to order events in a distributed system; we
use them to pick a single "snapshot moment" out of the recorded history
of a feature store.

All helpers treat a *missing* component as having counter 0 — this keeps
the partial order well-defined over the union of component sets and
matches the standard convention.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TypeAlias

VectorClock: TypeAlias = Mapping[str, int]


def _validate(clock: VectorClock) -> None:
    for k, v in clock.items():
        if not isinstance(k, str):
            raise TypeError("vector-clock keys must be strings")
        if not isinstance(v, int) or v < 0:
            raise ValueError(f"vector-clock counter for {k!r} must be a non-negative int")


def dominates(a: VectorClock, b: VectorClock) -> bool:
    """Return ``True`` when ``a ≥ b`` component-wise."""
    _validate(a)
    _validate(b)
    keys = set(a) | set(b)
    return all(a.get(k, 0) >= b.get(k, 0) for k in keys)


def equal(a: VectorClock, b: VectorClock) -> bool:
    """Component-wise equality (with missing entries treated as 0)."""
    _validate(a)
    _validate(b)
    keys = set(a) | set(b)
    return all(a.get(k, 0) == b.get(k, 0) for k in keys)


def lt(a: VectorClock, b: VectorClock) -> bool:
    """``a`` strictly precedes ``b`` (a ≤ b ∧ a ≠ b)."""
    return dominates(b, a) and not equal(a, b)


def concurrent(a: VectorClock, b: VectorClock) -> bool:
    """Neither clock dominates the other."""
    return not dominates(a, b) and not dominates(b, a)


def pointwise_max(*clocks: VectorClock) -> dict[str, int]:
    """Component-wise maximum (join in the vector-clock lattice)."""
    out: dict[str, int] = {}
    for c in clocks:
        _validate(c)
        for k, v in c.items():
            cur = out.get(k, 0)
            if v > cur:
                out[k] = v
    return out


def bump(clock: VectorClock, component: str) -> dict[str, int]:
    """Increment one component's counter; returns a new dict."""
    _validate(clock)
    if not component:
        raise ValueError("component must be a non-empty string")
    out: dict[str, int] = {k: int(v) for k, v in clock.items()}
    out[component] = out.get(component, 0) + 1
    return out


__all__ = [
    "VectorClock",
    "bump",
    "concurrent",
    "dominates",
    "equal",
    "lt",
    "pointwise_max",
]
