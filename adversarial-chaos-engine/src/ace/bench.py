"""Benchmark targeted-adversarial vs. pure-random fuzzing.

We register a fixed set of *seeded buggy* pipelines, run both
generators against them, and report (a) bugs found and (b) the
hit-rate ratio. On the buggy pipelines below targeted generation
should reliably win, because each pipeline's bug is keyed to one of
the standard edge-case categories.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

from ace.generator import AdversarialGenerator
from ace.invariants.catalog import Catalog, Frame

# --------------------------------------------------------------- bug zoo


def _buggy_abs_amount(frame: Frame) -> Frame:
    """abs() over `amount` changes the sum whenever any input is negative."""
    return [
        {**r, "amount": abs(r["amount"])} if isinstance(r.get("amount"), int | float) else r
        for r in frame
    ]


def _buggy_drop_negative(frame: Frame) -> Frame:
    """Drops rows with negative `amount` — violates row_count_preserved."""
    return [r for r in frame if (r.get("amount") or 0) >= 0]


def _buggy_default_zero(frame: Frame) -> Frame:
    """Replaces missing `name` with empty string — violates no_nulls when None."""
    out: Frame = []
    for r in frame:
        name = r.get("name")
        out.append({**r, "name": "" if name is None else name})
    # BUG: produces empty-string sentinels but does NOT enforce no-null
    # downstream: if upstream sends literal None we won't catch it here.
    if any(r.get("name") is None for r in frame):
        out.append({"name": None})
    return out


# ------------------------------------------------------------- benchmark


@dataclass(frozen=True, slots=True)
class BenchmarkReport:
    """Comparison of targeted vs. random fuzzing."""

    n_trials: int
    targeted_bugs: int
    random_bugs: int

    @property
    def speedup(self) -> float:
        if self.random_bugs == 0:
            return float("inf") if self.targeted_bugs else 1.0
        return self.targeted_bugs / self.random_bugs


def _register_zoo(catalog: Catalog) -> None:
    catalog.invariant(sum_invariant=["amount"])(_buggy_abs_amount)
    catalog.invariant(row_count_preserved=True)(_buggy_drop_negative)
    catalog.invariant(no_nulls=["name"])(_buggy_default_zero)


def _count_bugs(catalog: Catalog, generator: AdversarialGenerator, trials: int) -> int:
    """Run ``generator`` for ``trials`` against every pipeline; count
    failing trials (de-duped by (function, invariant))."""
    seen: set[tuple[str, str]] = set()
    for fn_name, fn, specs in catalog.entries():
        for _ in range(trials):
            frame = (
                generator.generate(catalog.referenced_columns())
                if generator.edge_fraction > 0
                else generator.generate_random()
            )
            try:
                out = fn(frame)
            except Exception:
                seen.add((fn_name, "no_exceptions"))
                continue
            for spec in specs:
                if (fn_name, spec.name) in seen:
                    continue
                if not spec.check(frame, out):
                    seen.add((fn_name, spec.name))
    return len(seen)


def run_benchmark(trials: int = 200, seed: int = 0) -> BenchmarkReport:
    """Build the bug zoo and compare targeted vs. random fuzzers."""
    cat = Catalog()
    _register_zoo(cat)
    targeted_gen = AdversarialGenerator(edge_fraction=0.9, rng=random.Random(seed))
    random_gen = AdversarialGenerator(edge_fraction=0.0, rng=random.Random(seed + 1))
    return BenchmarkReport(
        n_trials=trials,
        targeted_bugs=_count_bugs(cat, targeted_gen, trials),
        random_bugs=_count_bugs(cat, random_gen, trials),
    )


__all__ = ["BenchmarkReport", "run_benchmark"]
