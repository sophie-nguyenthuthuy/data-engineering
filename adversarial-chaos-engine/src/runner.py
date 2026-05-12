"""Adversarial runner.

Combines:
  - edge-case seed library
  - Hypothesis property-based generation
  - Invariant catalog

For each registered pipeline function, generate adversarial inputs and run
the function; flag violations.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Callable

from .edge_cases import numeric_edges, string_edges
from .invariants import _REGISTRY, specs_for, InvariantSpec


@dataclass
class Violation:
    fn_name: str
    invariant: str
    input: object
    output: object


@dataclass
class Runner:
    seed: int = 0
    violations: list = field(default_factory=list)

    def run_all(self, trials_per_fn: int = 100) -> list[Violation]:
        rng = random.Random(self.seed)
        for fn_name, (fn, specs) in _REGISTRY.items():
            if fn is None:
                continue
            for _ in range(trials_per_fn):
                inp = self._generate(rng)
                try:
                    out = fn(inp)
                except Exception:
                    # Exceptions are themselves violations
                    self.violations.append(Violation(
                        fn_name=fn_name, invariant="no_exceptions",
                        input=inp, output="<exception>"))
                    continue
                for spec in specs:
                    if not spec.check(inp, out):
                        self.violations.append(Violation(
                            fn_name=fn_name, invariant=spec.name,
                            input=inp, output=out))
                        break
        return self.violations

    def _generate(self, rng: random.Random) -> list[dict]:
        """Generate an adversarial input — a list of rows."""
        n = rng.randint(0, 10)
        rows = []
        for _ in range(n):
            row = {
                "amount": rng.choice(numeric_edges() + [rng.uniform(-100, 100)]),
                "name":   rng.choice(string_edges() + [f"u{rng.randint(0, 100)}"]),
                "id":     rng.randint(0, 1000),
            }
            rows.append(row)
        return rows


__all__ = ["Runner", "Violation"]
