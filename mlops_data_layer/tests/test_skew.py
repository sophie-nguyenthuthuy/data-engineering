"""Tests for the SkewDetector."""
from __future__ import annotations
import numpy as np
import pandas as pd
import pytest

from src.skew.detector import SkewDetector
from src.models import (
    FeatureType, TrainingSnapshot, FeatureStats, DriftStatus, FeatureDefinition
)
from src.features.registry import FeatureRegistry


@pytest.fixture
def registry():
    reg = FeatureRegistry.__new__(FeatureRegistry)
    reg._registry = {
        "txn_amount": FeatureDefinition(name="txn_amount", feature_type=FeatureType.NUMERICAL),
        "txn_channel": FeatureDefinition(name="txn_channel", feature_type=FeatureType.CATEGORICAL),
    }
    return reg


@pytest.fixture
def snapshot():
    rng = np.random.default_rng(10)
    amounts = rng.normal(100, 20, 2000)
    hist_counts, hist_edges = np.histogram(amounts, bins=10)
    return TrainingSnapshot(
        model_name="fraud_model",
        model_version="v1",
        row_count=2000,
        feature_stats=[
            FeatureStats(
                feature_name="txn_amount",
                feature_type=FeatureType.NUMERICAL,
                count=2000,
                null_count=0,
                null_fraction=0.0,
                mean=float(amounts.mean()),
                std=float(amounts.std()),
                histogram_edges=hist_edges.tolist(),
                histogram_counts=hist_counts.tolist(),
            ),
            FeatureStats(
                feature_name="txn_channel",
                feature_type=FeatureType.CATEGORICAL,
                count=2000,
                null_count=0,
                null_fraction=0.0,
                value_counts={"web": 800, "mobile": 600, "pos": 400, "api": 200},
            ),
        ],
    )


class TestSkewDetector:
    def test_no_skew_similar_data(self, registry, snapshot):
        rng = np.random.default_rng(10)
        serving_df = pd.DataFrame({
            "txn_amount": rng.normal(100, 20, 500).tolist(),
            "txn_channel": (["web"] * 200 + ["mobile"] * 150 + ["pos"] * 100 + ["api"] * 50),
        })
        detector = SkewDetector(registry)
        report = detector.detect(snapshot, serving_df)
        assert report.overall_status in (DriftStatus.NO_DRIFT, DriftStatus.WARNING)
        assert report.serving_window_size == 500

    def test_skew_detected_large_shift(self, registry, snapshot):
        rng = np.random.default_rng(99)
        serving_df = pd.DataFrame({
            "txn_amount": rng.normal(500, 20, 500).tolist(),  # 4x higher mean
            "txn_channel": ["api"] * 500,                      # completely different mix
        })
        detector = SkewDetector(registry)
        report = detector.detect(snapshot, serving_df)
        assert report.overall_status == DriftStatus.DRIFT_DETECTED
        assert report.skewed_feature_count >= 1

    def test_empty_serving_df(self, registry, snapshot):
        detector = SkewDetector(registry)
        report = detector.detect(snapshot, pd.DataFrame())
        assert report.overall_status == DriftStatus.INSUFFICIENT_DATA
        assert report.serving_window_size == 0

    def test_report_has_feature_results(self, registry, snapshot):
        rng = np.random.default_rng(10)
        serving_df = pd.DataFrame({
            "txn_amount": rng.normal(100, 20, 300).tolist(),
            "txn_channel": ["web"] * 300,
        })
        detector = SkewDetector(registry)
        report = detector.detect(snapshot, serving_df)
        names = [r.feature_name for r in report.feature_results]
        assert "txn_amount" in names
        assert "txn_channel" in names
