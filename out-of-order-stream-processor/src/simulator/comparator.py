from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

from .replay import StreamReplay, ReplayConfig, ReplayMetrics


@dataclass
class StrategyResult:
    config: ReplayConfig
    metrics: ReplayMetrics

    def __repr__(self) -> str:
        return (
            f"StrategyResult(name={self.config.name!r}, "
            f"completeness={self.metrics.completeness:.2%}, "
            f"avg_latency={self.metrics.avg_output_latency:.3f}s)"
        )


class WhatIfComparator:
    """
    Runs multiple watermark/window/policy configurations against the same
    historical event stream and produces a comparative report.

    This answers questions like:
      "If I used DynamicPerKeyWatermark(p=90) instead of FixedLag(30s),
       how much latency would I save, and how many more events would I miss?"

    Example
    -------
    ::

        comparator = WhatIfComparator(historical_events)
        comparator.add(ReplayConfig("fixed_30s", FixedLagWatermark(30), ...))
        comparator.add(ReplayConfig("dynamic_p95", DynamicPerKeyWatermark(95), ...))
        report = comparator.run()
        print(report.summary_table())
        best = report.best_by("completeness")
    """

    def __init__(self, events: list, sort_by_processing_time: bool = True) -> None:
        self._replay = StreamReplay(events, sort_by_processing_time)
        self._configs: list[ReplayConfig] = []

    def add(self, config: ReplayConfig) -> "WhatIfComparator":
        self._configs.append(config)
        return self

    def run(self) -> "ComparisonReport":
        results = []
        for cfg in self._configs:
            metrics = self._replay.run(cfg)
            results.append(StrategyResult(config=cfg, metrics=metrics))
        return ComparisonReport(results)


class ComparisonReport:
    """Holds results from all strategies and provides analysis helpers."""

    def __init__(self, results: list[StrategyResult]) -> None:
        self.results = results

    def best_by(self, metric: str) -> StrategyResult:
        """
        Return the strategy with the best value for the given metric.

        Supported metrics: 'completeness' (higher is better),
        'avg_output_latency' (lower is better),
        'dropped_events' (lower is better).
        """
        maximize = {"completeness", "events_in_windows"}
        key_fn = lambda r: getattr(r.metrics, metric)
        if metric in maximize:
            return max(self.results, key=key_fn)
        return min(self.results, key=key_fn)

    def pareto_frontier(self) -> list[StrategyResult]:
        """
        Return strategies on the completeness-vs-latency Pareto frontier.

        A strategy is Pareto-dominant if no other strategy is strictly better
        in both completeness AND avg_output_latency simultaneously.
        """
        frontier = []
        for candidate in self.results:
            dominated = False
            for other in self.results:
                if other is candidate:
                    continue
                if (
                    other.metrics.completeness >= candidate.metrics.completeness
                    and other.metrics.avg_output_latency
                    <= candidate.metrics.avg_output_latency
                    and (
                        other.metrics.completeness > candidate.metrics.completeness
                        or other.metrics.avg_output_latency
                        < candidate.metrics.avg_output_latency
                    )
                ):
                    dominated = True
                    break
            if not dominated:
                frontier.append(candidate)
        return frontier

    def summary_table(self) -> str:
        header = (
            f"{'Strategy':<25} {'Completeness':>13} {'Avg Latency':>13} "
            f"{'Late':>7} {'Dropped':>9} {'Restatements':>14}"
        )
        sep = "-" * len(header)
        rows = [header, sep]
        for sr in self.results:
            m = sr.metrics
            rows.append(
                f"{sr.config.name:<25} "
                f"{m.completeness:>12.2%} "
                f"{m.avg_output_latency:>12.3f}s "
                f"{m.late_events:>7} "
                f"{m.dropped_events:>9} "
                f"{m.restated_windows:>14}"
            )
        rows.append(sep)
        rows.append(
            "* Completeness = fraction of events captured in at least one "
            "emitted window result"
        )
        return "\n".join(rows)

    def per_strategy_detail(self) -> str:
        sections = []
        for sr in self.results:
            sections.append(sr.metrics.summary())
            sections.append("")
        return "\n".join(sections)
