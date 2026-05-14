"""Cross-engine comparison report."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pvc.benchmark import QueryResult


@dataclass(frozen=True, slots=True)
class ReportRow:
    """One row of the comparison table — per (query, engine) entry."""

    query_id: str
    engine: str
    p50: float
    p95: float
    speedup_vs_baseline: float  # baseline.p50 / engine.p50


@dataclass(frozen=True, slots=True)
class ComparisonReport:
    """Cross-engine comparison."""

    baseline: str
    rows: tuple[ReportRow, ...]

    def by_query(self) -> dict[str, list[ReportRow]]:
        out: dict[str, list[ReportRow]] = {}
        for r in self.rows:
            out.setdefault(r.query_id, []).append(r)
        return out

    def winners(self) -> dict[str, str]:
        """Engine with the best p50 per query."""
        out: dict[str, str] = {}
        for query_id, rows in self.by_query().items():
            best = min(rows, key=lambda r: r.p50)
            out[query_id] = best.engine
        return out


def build_comparison(results: list[QueryResult], baseline: str) -> ComparisonReport:
    if not results:
        raise ValueError("results must be non-empty")
    if not any(baseline in r.by_engine for r in results):
        raise ValueError(f"baseline {baseline!r} not present in any result")
    rows: list[ReportRow] = []
    for r in results:
        base = r.by_engine.get(baseline)
        for engine, ir in r.by_engine.items():
            speedup = 1.0 if base is None or ir.stats.p50 == 0 else base.stats.p50 / ir.stats.p50
            rows.append(
                ReportRow(
                    query_id=r.query_id,
                    engine=engine,
                    p50=ir.stats.p50,
                    p95=ir.stats.p95,
                    speedup_vs_baseline=speedup,
                )
            )
    return ComparisonReport(baseline=baseline, rows=tuple(rows))


__all__ = ["ComparisonReport", "ReportRow", "build_comparison"]
