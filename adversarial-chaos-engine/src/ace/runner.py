"""Adversarial runner.

Coordinates :class:`AdversarialGenerator`, the :class:`Catalog` of
registered pipelines + invariants, and the row-:func:`shrink_rows`
post-processor. Returns a structured :class:`Report` with one
:class:`Violation` per (function, invariant) failure.

The runner is intentionally synchronous and deterministic: every
violation is reproducible from ``(seed, trial_index)`` so the
regression emitter can hand-stamp it onto a pytest case.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ace.generator import AdversarialGenerator
from ace.shrinker import shrink_rows

if TYPE_CHECKING:
    from collections.abc import Callable

    from ace.invariants.catalog import Catalog, Frame, Pipeline


@dataclass(frozen=True, slots=True)
class Violation:
    """Single failed-invariant observation."""

    fn_name: str
    invariant: str
    input: tuple[tuple[tuple[str, object], ...], ...]
    output_repr: str
    is_exception: bool = False

    def input_as_frame(self) -> Frame:
        return [dict(row) for row in self.input]


def _freeze_frame(frame: Frame) -> tuple[tuple[tuple[str, object], ...], ...]:
    """Hashable, repr-stable snapshot of a frame for storage in a Violation."""
    return tuple(tuple(sorted(row.items())) for row in frame)


@dataclass
class Report:
    """Runner output across all trials."""

    n_trials: int
    n_pipelines: int
    violations: list[Violation] = field(default_factory=list)

    def failing(self) -> list[Violation]:
        return [v for v in self.violations if not v.is_exception]

    def exceptions(self) -> list[Violation]:
        return [v for v in self.violations if v.is_exception]

    def by_pipeline(self) -> dict[str, list[Violation]]:
        out: dict[str, list[Violation]] = {}
        for v in self.violations:
            out.setdefault(v.fn_name, []).append(v)
        return out


@dataclass
class Runner:
    """Per-catalog adversarial runner."""

    catalog: Catalog
    generator: AdversarialGenerator | None = None
    seed: int = 0
    shrink: bool = True

    def __post_init__(self) -> None:
        if self.generator is None:
            self.generator = AdversarialGenerator(rng=random.Random(self.seed))

    def run(self, trials: int = 100) -> Report:
        if trials < 1:
            raise ValueError("trials must be ≥ 1")
        targeted = self.catalog.referenced_columns()
        violations: list[Violation] = []
        entries = self.catalog.entries()
        for fn_name, fn, specs in entries:
            seen_for_this_fn: set[str] = set()
            for _ in range(trials):
                frame = self._gen().generate(targeted)
                try:
                    out = fn(frame)
                except Exception as exc:
                    if "exception" not in seen_for_this_fn:
                        seen_for_this_fn.add("exception")
                        minimised = self._maybe_shrink(fn, frame, _always_fail)
                        violations.append(
                            Violation(
                                fn_name=fn_name,
                                invariant="no_exceptions",
                                input=_freeze_frame(minimised),
                                output_repr=f"<{type(exc).__name__}: {exc}>",
                                is_exception=True,
                            )
                        )
                    continue
                for spec in specs:
                    if spec.name in seen_for_this_fn:
                        continue
                    if not spec.check(frame, out):
                        seen_for_this_fn.add(spec.name)
                        minimised = self._maybe_shrink(fn, frame, spec.check)
                        try:
                            replay = fn(minimised)
                            output_repr = repr(replay)
                        except Exception as exc:
                            output_repr = f"<{type(exc).__name__}: {exc}>"
                        violations.append(
                            Violation(
                                fn_name=fn_name,
                                invariant=spec.name,
                                input=_freeze_frame(minimised),
                                output_repr=output_repr,
                                is_exception=False,
                            )
                        )
                        break
        return Report(n_trials=trials, n_pipelines=len(entries), violations=violations)

    # ----------------------------------------------------------- internals

    def _gen(self) -> AdversarialGenerator:
        assert self.generator is not None
        return self.generator

    def _maybe_shrink(
        self,
        fn: Pipeline,
        frame: Frame,
        check: Callable[[Frame, Frame], bool],
    ) -> Frame:
        if not self.shrink:
            return frame
        return shrink_rows(fn, frame, check)


def _always_fail(_in: Frame, _out: Frame) -> bool:
    """Check used during shrinking when the original failure is an
    exception — the shrinker only needs to know "still failing"."""
    return False


__all__ = ["Report", "Runner", "Violation"]
