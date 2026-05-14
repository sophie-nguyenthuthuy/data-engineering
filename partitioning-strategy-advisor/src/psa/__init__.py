"""partitioning-strategy-advisor — recommend partition + bucketing from query log."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

if TYPE_CHECKING:
    from psa.cardinality import CardinalityEstimate, estimate_cardinality
    from psa.parser import ParsedQuery, parse_query
    from psa.profile import ColumnUsage, Profiler, QueryProfile
    from psa.recommender import (
        BucketRecommendation,
        PartitionRecommendation,
        recommend,
    )
    from psa.skew import SkewReport, detect_skew


_LAZY: dict[str, tuple[str, str]] = {
    "ParsedQuery": ("psa.parser", "ParsedQuery"),
    "parse_query": ("psa.parser", "parse_query"),
    "ColumnUsage": ("psa.profile", "ColumnUsage"),
    "QueryProfile": ("psa.profile", "QueryProfile"),
    "Profiler": ("psa.profile", "Profiler"),
    "CardinalityEstimate": ("psa.cardinality", "CardinalityEstimate"),
    "estimate_cardinality": ("psa.cardinality", "estimate_cardinality"),
    "SkewReport": ("psa.skew", "SkewReport"),
    "detect_skew": ("psa.skew", "detect_skew"),
    "PartitionRecommendation": ("psa.recommender", "PartitionRecommendation"),
    "BucketRecommendation": ("psa.recommender", "BucketRecommendation"),
    "recommend": ("psa.recommender", "recommend"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        from importlib import import_module

        m, attr = _LAZY[name]
        return getattr(import_module(m), attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "BucketRecommendation",
    "CardinalityEstimate",
    "ColumnUsage",
    "ParsedQuery",
    "PartitionRecommendation",
    "Profiler",
    "QueryProfile",
    "SkewReport",
    "__version__",
    "detect_skew",
    "estimate_cardinality",
    "parse_query",
    "recommend",
]
