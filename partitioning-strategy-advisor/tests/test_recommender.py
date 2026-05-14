"""Partition + bucket recommender tests."""

from __future__ import annotations

import pytest

from psa.cardinality import CardinalityEstimate
from psa.profile import Profiler
from psa.recommender import recommend
from psa.skew import SkewReport


def _profile(queries):
    p = Profiler()
    for q in queries:
        p.add(q)
    return p.build()


def test_recommender_picks_most_filtered_column():
    profile = _profile(
        [
            "SELECT * FROM o WHERE country = 'US'",
            "SELECT * FROM o WHERE country = 'CA'",
            "SELECT * FROM o WHERE country = 'US'",
            "SELECT * FROM o WHERE status = 'ok'",
        ]
    )
    part, _ = recommend(profile)
    assert part.column == "country"


def test_recommender_penalises_high_cardinality():
    profile = _profile(
        [
            "SELECT * FROM o WHERE user_id = 1",
            "SELECT * FROM o WHERE country = 'US'",
        ]
    )
    cards = {
        "user_id": CardinalityEstimate("user_id", 100, 100, 100_000),
        "country": CardinalityEstimate("country", 100, 20, 20),
    }
    part, _ = recommend(profile, cardinalities=cards, target_partitions=200)
    # user_id has 1 filter, country has 1 too — but user_id is heavily
    # penalised for cardinality so country must win on tie + penalty.
    assert part.column == "country"


def test_recommender_penalises_skewed_column():
    profile = _profile(["SELECT * FROM o WHERE a = 1", "SELECT * FROM o WHERE b = 2"])
    cards = {
        "a": CardinalityEstimate("a", 100, 50, 50),
        "b": CardinalityEstimate("b", 100, 50, 50),
    }
    skews = {
        "a": SkewReport(
            name="a", n=100, distinct=10, coefficient_of_variation=2.5, top_3_share=0.9
        ),
        "b": SkewReport(
            name="b", n=100, distinct=10, coefficient_of_variation=0.1, top_3_share=0.1
        ),
    }
    part, _ = recommend(profile, cardinalities=cards, skews=skews)
    assert part.column == "b"


def test_recommender_returns_none_when_no_filter_usage():
    profile = _profile(["SELECT * FROM o JOIN c ON o.cid = c.cid"])  # only join columns
    part, _ = recommend(profile)
    assert part.column is None


def test_bucket_recommender_picks_most_joined_column():
    profile = _profile(
        [
            "SELECT * FROM o JOIN c ON o.cid = c.cid",
            "SELECT * FROM o JOIN c ON o.cid = c.cid",
            "SELECT * FROM o JOIN p ON o.pid = p.pid",
        ]
    )
    _, bucket = recommend(profile)
    assert bucket.column == "cid"


def test_bucket_recommender_picks_power_of_two():
    profile = _profile(["SELECT * FROM o JOIN c ON o.cid = c.cid"])
    cards = {"cid": CardinalityEstimate("cid", 1000, 1000, 1_000_000)}
    _, bucket = recommend(profile, cardinalities=cards)
    # next_pow2(sqrt(1_000_000) = 1000) = 1024 (which the recommender caps to 1024 too).
    assert bucket.bucket_count == 1024


def test_bucket_recommender_returns_none_without_joins():
    profile = _profile(["SELECT * FROM o WHERE country = 'US'"])
    _, bucket = recommend(profile)
    assert bucket.column is None
    assert bucket.bucket_count == 0


def test_recommender_rejects_zero_target_partitions():
    profile = _profile(["SELECT * FROM o WHERE a = 1"])
    with pytest.raises(ValueError):
        recommend(profile, target_partitions=0)


def test_recommender_bucket_count_floor_is_eight():
    profile = _profile(["SELECT * FROM o JOIN c ON o.cid = c.cid"])
    cards = {"cid": CardinalityEstimate("cid", 10, 10, 10)}
    _, bucket = recommend(profile, cardinalities=cards)
    # next_pow2(max(8, isqrt(10))) = next_pow2(8) = 8.
    assert bucket.bucket_count >= 8
