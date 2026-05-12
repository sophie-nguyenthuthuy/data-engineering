"""Vector clocks for per-entity causal consistency.

A vector clock for entity E is a dict { component_name → int }. Causality:

    vc1 ≤ vc2  iff  ∀ c: vc1[c] ≤ vc2[c]
    vc1 < vc2  iff  vc1 ≤ vc2 and vc1 != vc2
    vc1 || vc2 (concurrent) otherwise

For our serving guarantee, the resolver picks a vc* such that *every*
returned feature has a write-clock dominated by vc*.
"""
from __future__ import annotations

from dataclasses import dataclass, field


def dominates(a: dict, b: dict) -> bool:
    """True if a ≥ b component-wise (defaulting missing components to 0)."""
    keys = set(a) | set(b)
    return all(a.get(k, 0) >= b.get(k, 0) for k in keys)


def equal(a: dict, b: dict) -> bool:
    keys = set(a) | set(b)
    return all(a.get(k, 0) == b.get(k, 0) for k in keys)


def lt(a: dict, b: dict) -> bool:
    return dominates(b, a) and not equal(a, b)


def concurrent(a: dict, b: dict) -> bool:
    return not dominates(a, b) and not dominates(b, a)


def pointwise_max(*clocks) -> dict:
    out: dict = {}
    for c in clocks:
        for k, v in c.items():
            out[k] = max(out.get(k, 0), v)
    return out


def bump(clock: dict, component: str) -> dict:
    """Increment one component's counter."""
    out = dict(clock)
    out[component] = out.get(component, 0) + 1
    return out


__all__ = ["dominates", "equal", "lt", "concurrent", "pointwise_max", "bump"]
