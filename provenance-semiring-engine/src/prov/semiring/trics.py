"""TriCS — quasi-semiring for probabilistic databases (independent-OR).

  ⊕ p q = 1 - (1-p)(1-q)        (probability of union of independent events)
  ⊗ p q = p * q                  (probability of independent intersection)

⚠️  TriCS is **not a true semiring**: ⊗ does not distribute over ⊕ in
general (e.g. a=0.5, b=c=1.0: a⊗(b⊕c) = 0.5 ≠ 0.75 = (a⊗b)⊕(a⊗c)).
It gives correct marginals **only when events are independent**.

For exact probabilities under arbitrary correlation, evaluate the
how-provenance polynomial via `prov.lineage.exact_probability`.
"""

from __future__ import annotations

from prov.semiring.base import Semiring


class TriCS(Semiring[float]):
    """Probabilistic semiring (independent-OR)."""

    def zero(self) -> float:
        return 0.0

    def one(self) -> float:
        return 1.0

    def plus(self, a: float, b: float) -> float:
        return 1.0 - (1.0 - a) * (1.0 - b)

    def times(self, a: float, b: float) -> float:
        return a * b


__all__ = ["TriCS"]
