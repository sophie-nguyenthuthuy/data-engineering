"""Tests for the strategy recommender."""

import pytest

from cow_mor_bench.recommender.engine import Recommendation, recommend_from_params


def test_olap_recommends_cow():
    rec = recommend_from_params(
        write_ratio=0.05,
        update_fraction_of_table=0.01,
        avg_batch_rows=5000,
        full_scan_ratio=0.8,
        point_read_ratio=0.1,
        table_name="analytics_events",
    )
    assert rec.recommended == Recommendation.COW


def test_streaming_recommends_mor():
    rec = recommend_from_params(
        write_ratio=0.90,
        update_fraction_of_table=0.01,
        avg_batch_rows=200,
        full_scan_ratio=0.05,
        point_read_ratio=0.05,
        table_name="event_stream",
    )
    assert rec.recommended in (Recommendation.MOR, Recommendation.MOR_WITH_COMPACTION)


def test_recommendation_has_reasoning():
    rec = recommend_from_params(0.3, 0.1, 1000, 0.3, 0.3)
    assert len(rec.reasoning) > 0


def test_recommendation_confidence_in_range():
    rec = recommend_from_params(0.5, 0.05, 500, 0.5, 0.3)
    assert 0.0 < rec.confidence <= 1.0


def test_recommendation_has_compaction_cost():
    rec = recommend_from_params(0.5, 0.1, 1000, 0.3, 0.3)
    assert rec.compaction_cost is not None
