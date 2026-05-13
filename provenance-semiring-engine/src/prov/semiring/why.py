"""Why-provenance semiring (2^(2^X), ∪, ⨯, ∅, {()}).

Each annotation is a **set of witnesses**, where a witness is a
**set of input tokens** that together derive the output tuple.

  - ⊕ (union): take the union of witness sets
  - ⊗ (witness-conjunction): for each pair (w_a, w_b), produce w_a ∪ w_b
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from prov.semiring.base import Semiring

if TYPE_CHECKING:
    from collections.abc import Hashable

# Witness = frozenset[token]
# Annotation = frozenset[Witness]


class WhyProvenance(Semiring[frozenset]):
    """Set-of-witness-sets provenance."""

    def zero(self) -> frozenset:
        return frozenset()

    def one(self) -> frozenset:
        return frozenset({frozenset()})

    def plus(self, a: frozenset, b: frozenset) -> frozenset:
        return a | b

    def times(self, a: frozenset, b: frozenset) -> frozenset:
        return frozenset(w1 | w2 for w1 in a for w2 in b)

    @staticmethod
    def singleton(token: Hashable) -> frozenset:
        """Annotation for a base tuple with one witness containing one token."""
        return frozenset({frozenset({token})})

    @staticmethod
    def witnesses(annotation: frozenset) -> list[frozenset]:
        return sorted(annotation, key=lambda w: sorted(map(repr, w)))


__all__ = ["WhyProvenance"]
