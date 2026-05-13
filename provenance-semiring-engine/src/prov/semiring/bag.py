"""Bag semiring (N, +, *, 0, 1) — multiset counting."""

from __future__ import annotations

from prov.semiring.base import Semiring


class BagSemiring(Semiring[int]):
    """Multiset counting: ⊕ = +, ⊗ = *.

    Tracks how many derivations exist for each output tuple.
    """

    def zero(self) -> int:
        return 0

    def one(self) -> int:
        return 1

    def plus(self, a: int, b: int) -> int:
        return a + b

    def times(self, a: int, b: int) -> int:
        return a * b


__all__ = ["BagSemiring"]
