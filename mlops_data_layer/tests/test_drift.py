"""Tests for statistical drift tests and the DriftDetector."""
from __future__ import annotations
import numpy as np
import pandas as pd
import pytest

from src.drift.tests import DriftTestSuite
from src.drift.detector import DriftDetector, _reconstruct_ref_samples
from src.models import (
    FeatureType, TrainingSnapshot, FeatureStats, DriftStatus, FeatureDefinition
)
from src.features.registry import FeatureRegistry


# ---------------------------------------------------------------------------
# DriftTestSuite — KS test
# ---------------------------------------------------------------------------

class TestKSTest:
    def test_identical_distributions_no_drift(self):
        rng = np.random.default_rng(0)
        data = rng.normal(0, 1, 500)
        result = DriftTestSuite.ks_test(data, data.copy())
        assert not result.drifted
        assert result.pvalue > 0.05

    def test_different_distributions_drift(self):
        rng = np.random.default_rng(1)
        ref = rng.normal(0, 1, 500)
        cur = rng.normal(5, 1, 500)   # mean shifted by 5σ
        result = DriftTestSuite.ks_test(ref, cur)
        assert result.drifted
        assert result.pvalue < 0.05

    def test_too_few_samples_no_drift(self):
        result = DriftTestSuite.ks_test([1, 2], [3, 4])
        assert not result.drifted
        assert result.pvalue == 1.0


# ---------------------------------------------------------------------------
# DriftTestSuite — PSI
# ---------------------------------------------------------------------------

class TestPSI:
    def test_same_distribution_low_psi(self):
        rng = np.random.default_rng(2)
        data = rng.normal(0, 1, 1000)
        result = DriftTestSuite.psi(data[:500], data[500:])
        assert result.score < 0.1
        assert not result.drifted

    def test_shifted_distribution_high_psi(self):
        rng = np.random.default_rng(3)
        ref = rng.normal(0, 1, 1000)
        cur = rng.normal(4, 1, 1000)
        result = DriftTestSuite.psi(ref, cur, threshold=0.2)
        assert result.drifted

    def test_empty_arrays(self):
        result = DriftTestSuite.psi(np.array([]), np.array([]))
        assert result.score == 0.0
        assert not result.drifted


# ---------------------------------------------------------------------------
# DriftTestSuite — JS Divergence
# ---------------------------------------------------------------------------

class TestJSDivergence:
    def test_identical_near_zero(self):
        rng = np.random.default_rng(4)
        data = rng.normal(0, 1, 500)
        result = DriftTestSuite.js_divergence(data, data)
        assert result.divergence < 0.05

    def test_very_different_high_js(self):
        rng = np.random.default_rng(5)
        ref = rng.normal(0, 0.5, 500)
        cur = rng.normal(10, 0.5, 500)
        result = DriftTestSuite.js_divergence(ref, cur, threshold=0.1)
        assert result.drifted

    def test_bounded_zero_to_one(self):
        rng = np.random.default_rng(6)
        ref = rng.normal(0, 1, 200)
        cur = rng.normal(100, 1, 200)
        result = DriftTestSuite.js_divergence(ref, cur)
        assert 0.0 <= result.divergence <= 1.0


# ---------------------------------------------------------------------------
# DriftTestSuite — Chi-Squared
# ---------------------------------------------------------------------------

class TestChi2Test:
    def test_same_categories_no_drift(self):
        cats = pd.Series(["a"] * 100 + ["b"] * 100 + ["c"] * 50)
        result = DriftTestSuite.chi2_test(cats, cats.copy())
        assert not result.drifted

    def test_new_dominant_category_drift(self):
        ref = pd.Series(["a"] * 200 + ["b"] * 200)
        cur = pd.Series(["b"] * 380 + ["a"] * 20)   # b now dominates
        result = DriftTestSuite.chi2_test(ref, cur)
        assert result.drifted


# ---------------------------------------------------------------------------
# DriftDetector
# ---------------------------------------------------------------------------

@pytest.fixture
def registry():
    reg = FeatureRegistry.__new__(FeatureRegistry)
    reg._registry = {
        "age": FeatureDefinition(name="age", feature_type=FeatureType.NUMERICAL),
        "segment": FeatureDefinition(name="segment", feature_type=FeatureType.CATEGORICAL),
    }
    return reg


@pytest.fixture
def training_snapshot():
    rng = np.random.default_rng(42)
    age_vals = rng.normal(35, 10, 1000)
    return TrainingSnapshot(
        model_name="fraud_model",
        model_version="v1",
        row_count=1000,
        feature_stats=[
            FeatureStats(
                feature_name="age",
                feature_type=FeatureType.NUMERICAL,
                count=1000,
                null_count=0,
                null_fraction=0.0,
                mean=float(age_vals.mean()),
                std=float(age_vals.std()),
                histogram_edges=list(np.histogram(age_vals, bins=10)[1]),
                histogram_counts=list(np.histogram(age_vals, bins=10)[0]),
            ),
            FeatureStats(
                feature_name="segment",
                feature_type=FeatureType.CATEGORICAL,
                count=1000,
                null_count=0,
                null_fraction=0.0,
                value_counts={"bronze": 400, "silver": 300, "gold": 200, "platinum": 100},
                cardinality=4,
            ),
        ],
    )


class TestDriftDetector:
    def test_no_drift_similar_distribution(self, registry, training_snapshot):
        rng = np.random.default_rng(42)
        serving_df = pd.DataFrame({
            "age": rng.normal(35, 10, 600).tolist(),
            "segment": (["bronze"] * 240 + ["silver"] * 180 + ["gold"] * 120 + ["platinum"] * 60),
        })
        detector = DriftDetector(registry)
        report = detector.detect(training_snapshot, serving_df)
        # With very similar data drift should not be detected
        assert report.overall_status in (DriftStatus.NO_DRIFT, DriftStatus.WARNING)
        assert report.total_feature_count == 2

    def test_drift_detected_large_shift(self, registry, training_snapshot):
        rng = np.random.default_rng(99)
        serving_df = pd.DataFrame({
            "age": rng.normal(70, 3, 600).tolist(),   # completely different age range
            "segment": ["bronze"] * 600,               # all one category
        })
        detector = DriftDetector(registry)
        report = detector.detect(training_snapshot, serving_df)
        assert report.overall_status == DriftStatus.DRIFT_DETECTED
        assert report.drifted_feature_count >= 1

    def test_insufficient_data(self, registry, training_snapshot):
        serving_df = pd.DataFrame({"age": [25, 30], "segment": ["gold", "silver"]})
        detector = DriftDetector(registry)
        report = detector.detect(training_snapshot, serving_df)
        # All features insufficient
        assert all(
            r.status == DriftStatus.INSUFFICIENT_DATA for r in report.feature_results
        )

    def test_empty_serving_df(self, registry, training_snapshot):
        detector = DriftDetector(registry)
        report = detector.detect(training_snapshot, pd.DataFrame())
        assert report.total_feature_count == 0


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

class TestReconstructRefSamples:
    def test_from_histogram(self):
        stats = FeatureStats(
            feature_name="x",
            feature_type=FeatureType.NUMERICAL,
            count=100,
            null_count=0,
            null_fraction=0.0,
            mean=0.0,
            std=1.0,
            histogram_edges=[0.0, 1.0, 2.0, 3.0],
            histogram_counts=[30, 50, 20],
        )
        samples = _reconstruct_ref_samples(stats)
        assert len(samples) == 100

    def test_from_mean_std_fallback(self):
        stats = FeatureStats(
            feature_name="x",
            feature_type=FeatureType.NUMERICAL,
            count=100,
            null_count=0,
            null_fraction=0.0,
            mean=5.0,
            std=2.0,
        )
        samples = _reconstruct_ref_samples(stats)
        assert len(samples) == 200   # fallback generates 200 samples
