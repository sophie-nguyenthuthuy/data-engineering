"""Abstract commutative semiring (K, ⊕, ⊗, 0, 1).

Axioms (all instances must satisfy):
  - (K, ⊕, 0) is a commutative monoid: associative, commutative, identity 0
  - (K, ⊗, 1) is a commutative monoid: associative, commutative, identity 1
  - ⊗ distributes over ⊕: a ⊗ (b ⊕ c) = (a ⊗ b) ⊕ (a ⊗ c)
  - 0 ⊗ a = 0 (absorbing element)

These are checked by Hypothesis property tests across every instance.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import Iterable

T = TypeVar("T")


class Semiring(ABC, Generic[T]):
    """Commutative semiring base."""

    @abstractmethod
    def zero(self) -> T: ...

    @abstractmethod
    def one(self) -> T: ...

    @abstractmethod
    def plus(self, a: T, b: T) -> T: ...

    @abstractmethod
    def times(self, a: T, b: T) -> T: ...

    # ---- Convenience -----------------------------------------------------

    def sum(self, xs: Iterable[T]) -> T:
        acc = self.zero()
        for x in xs:
            acc = self.plus(acc, x)
        return acc

    def product(self, xs: Iterable[T]) -> T:
        acc = self.one()
        for x in xs:
            acc = self.times(acc, x)
        return acc

    def name(self) -> str:
        return type(self).__name__


__all__ = ["Semiring"]
