"""Shifted-workload benchmark generator and runners.

Produces a synthetic but realistic dataset + a query stream whose
predicate distribution shifts every ``shift_every`` queries. Lets us
compare static layouts (one-time tuning) against an online agent.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np

from llo.replay.pages import expected_pages
from llo.workload.profile import Query

if TYPE_CHECKING:
    from numpy.typing import NDArray

    from llo.policy.bandit import Action


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    """Aggregated pages-scanned metrics for a single layout strategy."""

    label: str
    mean_pages: float
    total_queries: int


def make_dataset(
    n_rows: int = 4096,
    columns: tuple[str, ...] = ("a", "b", "c", "d"),
    domain: int = 256,
    seed: int = 0,
) -> tuple[NDArray[np.int64], list[str]]:
    """Synthetic integer-valued table over a fixed domain."""
    rng = np.random.default_rng(seed)
    data = rng.integers(0, domain, size=(n_rows, len(columns)), dtype=np.int64)
    return data, list(columns)


def make_shifted_workload(
    columns: list[str],
    n_queries: int = 800,
    shift_every: int = 200,
    box_half_width: int = 20,
    domain: int = 256,
    seed: int = 1,
) -> list[Query]:
    """Box queries that rotate which two columns they range-filter."""
    rng = random.Random(seed)
    queries: list[Query] = []
    pairs = [(columns[i], columns[(i + 1) % len(columns)]) for i in range(len(columns))]
    for i in range(n_queries):
        phase = (i // shift_every) % len(pairs)
        c1, c2 = pairs[phase]
        v1 = rng.randint(0, domain - 1)
        v2 = rng.randint(0, domain - 1)
        preds = {
            c1: (
                "range",
                float(max(v1 - box_half_width, 0)),
                float(min(v1 + box_half_width, domain - 1)),
            ),
            c2: (
                "range",
                float(max(v2 - box_half_width, 0)),
                float(min(v2 + box_half_width, domain - 1)),
            ),
        }
        queries.append(Query(predicates=preds))  # type: ignore[arg-type]
    return queries


def evaluate_static(
    data: NDArray[np.int64],
    cols: list[str],
    action: Action,
    workload: list[Query],
    label: str,
) -> BenchmarkResult:
    """Evaluate a single static layout against the whole workload."""
    return BenchmarkResult(
        label=label,
        mean_pages=expected_pages(data, cols, action, workload),
        total_queries=len(workload),
    )


__all__ = ["BenchmarkResult", "evaluate_static", "make_dataset", "make_shifted_workload"]
