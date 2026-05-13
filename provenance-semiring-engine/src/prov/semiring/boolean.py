"""Boolean semiring ({0,1}, ∨, ∧, 0, 1) — set semantics."""

from __future__ import annotations

from prov.semiring.base import Semiring


class BooleanSemiring(Semiring[bool]):
    """Set semantics: ⊕ = OR, ⊗ = AND.

    Tracks whether an output tuple has any derivation at all."""

    def zero(self) -> bool:
        return False

    def one(self) -> bool:
        return True

    def plus(self, a: bool, b: bool) -> bool:
        return a or b

    def times(self, a: bool, b: bool) -> bool:
        return a and b


__all__ = ["BooleanSemiring"]
