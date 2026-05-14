"""Partition + bucket recommender.

Given a :class:`QueryProfile` plus per-column cardinality + skew, we
recommend one partition column and zero-or-one bucket column with a
power-of-two bucket count.

Heuristics — explicit so they're easy to reason about:

  **Partition pick**:
    score = filter_count
            − heavy_penalty if cardinality > target_partitions × 10
            − skew_penalty if column is skewed
    A column with high filter frequency AND moderate cardinality
    AND no skew wins.

  **Bucket pick**:
    Best join column, with bucket count = next power of two ≥
    sqrt(estimated_distinct), capped at 1024. Bucketing on a column
    you never join on is useless; we refuse if join_count == 0.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from psa.cardinality import CardinalityEstimate
    from psa.profile import QueryProfile
    from psa.skew import SkewReport


@dataclass(frozen=True, slots=True)
class PartitionRecommendation:
    """One column the planner should partition by."""

    column: str | None
    score: float
    reason: str


@dataclass(frozen=True, slots=True)
class BucketRecommendation:
    """One column the planner should bucket by + the bucket count."""

    column: str | None
    bucket_count: int
    reason: str


def _next_pow2(n: int) -> int:
    if n <= 1:
        return 1
    return 1 << (n - 1).bit_length()


def recommend(
    profile: QueryProfile,
    *,
    cardinalities: dict[str, CardinalityEstimate] | None = None,
    skews: dict[str, SkewReport] | None = None,
    target_partitions: int = 200,
) -> tuple[PartitionRecommendation, BucketRecommendation]:
    if target_partitions < 1:
        raise ValueError("target_partitions must be ≥ 1")
    cardinalities = cardinalities or {}
    skews = skews or {}

    # ---------------------------------------------------------- partition
    best_part_col: str | None = None
    best_part_score = float("-inf")
    best_part_reason = "no column with filter usage"
    for usage in profile.columns:
        if usage.filter_count == 0:
            continue
        card = cardinalities.get(usage.name)
        skew = skews.get(usage.name)
        score = float(usage.filter_count)
        reasons: list[str] = [f"filter_count={usage.filter_count}"]
        if card is not None:
            if card.estimated_distinct > target_partitions * 10:
                score -= 50.0
                reasons.append(f"cardinality={card.estimated_distinct} (penalty)")
            else:
                reasons.append(f"cardinality={card.estimated_distinct}")
        if skew is not None and skew.is_skewed():
            score -= 25.0
            reasons.append("skewed (penalty)")
        if score > best_part_score:
            best_part_score = score
            best_part_col = usage.name
            best_part_reason = ", ".join(reasons)

    part_rec = PartitionRecommendation(
        column=best_part_col,
        score=best_part_score if best_part_col is not None else 0.0,
        reason=best_part_reason if best_part_col is not None else "no column with filter usage",
    )

    # ------------------------------------------------------------ bucket
    join_cols = [u for u in profile.columns if u.join_count > 0]
    if not join_cols:
        return part_rec, BucketRecommendation(
            column=None, bucket_count=0, reason="no join columns in workload"
        )
    best = max(join_cols, key=lambda u: u.join_count)
    card = cardinalities.get(best.name)
    distinct = card.estimated_distinct if card is not None else max(64, best.join_count * 4)
    raw = max(8, math.isqrt(distinct))
    buckets = min(1024, _next_pow2(raw))
    bucket_rec = BucketRecommendation(
        column=best.name,
        bucket_count=buckets,
        reason=f"join_count={best.join_count}, est_distinct={distinct} → {buckets} buckets",
    )
    return part_rec, bucket_rec


__all__ = ["BucketRecommendation", "PartitionRecommendation", "recommend"]
