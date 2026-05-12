"""Tests for the drift detection engine."""
import numpy as np
import pytest

from feature_store.drift_detector import DriftDetector, _psi
from feature_store.registry import FeatureType


def make_detector(**kwargs):
    return DriftDetector(
        threshold_psi=kwargs.get("threshold_psi", 0.2),
        threshold_ks=kwargs.get("threshold_ks", 0.05),
        threshold_js=kwargs.get("threshold_js", 0.1),
    )


class TestPSI:
    def test_identical_distributions_zero_psi(self):
        rng = np.random.default_rng(42)
        data = rng.normal(0, 1, 1000)
        psi = _psi(data, data.copy())
        assert psi < 0.05

    def test_large_shift_high_psi(self):
        rng = np.random.default_rng(42)
        base = rng.normal(0, 1, 1000)
        shifted = rng.normal(5, 1, 1000)  # 5-sigma shift
        psi = _psi(base, shifted)
        assert psi > 0.2


class TestDriftDetector:
    def _make_continuous_training(self, n=2000, mean=0.0, std=1.0):
        import pandas as pd
        rng = np.random.default_rng(0)
        return pd.DataFrame({"feat_a": rng.normal(mean, std, n)})

    def test_no_drift_identical(self):
        detector = make_detector()
        import pandas as pd
        rng = np.random.default_rng(1)
        training_df = pd.DataFrame({"feat_a": rng.normal(0, 1, 2000)})
        production_vals = {"feat_a": rng.normal(0, 1, 1000).tolist()}
        report = detector.compare(training_df, production_vals, {"feat_a": FeatureType.CONTINUOUS})
        assert report.overall_drift_score == 0.0
        assert len(report.drifted_features) == 0

    def test_drift_detected_large_shift(self):
        detector = make_detector()
        import pandas as pd
        rng = np.random.default_rng(2)
        training_df = pd.DataFrame({"feat_a": rng.normal(0, 1, 2000)})
        # Production has severely shifted distribution
        production_vals = {"feat_a": rng.normal(10, 1, 1000).tolist()}
        report = detector.compare(training_df, production_vals, {"feat_a": FeatureType.CONTINUOUS})
        assert "feat_a" in report.drifted_features
        assert report.overall_drift_score > 0.0

    def test_categorical_no_drift(self):
        detector = make_detector()
        import pandas as pd
        cats = ["a", "b", "c", "d"]
        rng = np.random.default_rng(3)
        training_vals = rng.choice(cats, 2000, p=[0.4, 0.3, 0.2, 0.1]).tolist()
        production_vals = rng.choice(cats, 1000, p=[0.4, 0.3, 0.2, 0.1]).tolist()
        training_df = pd.DataFrame({"cat_feat": training_vals})
        report = detector.compare(
            training_df,
            {"cat_feat": production_vals},
            {"cat_feat": FeatureType.CATEGORICAL},
        )
        assert len(report.drifted_features) == 0

    def test_categorical_drift_detected(self):
        detector = make_detector()
        import pandas as pd
        cats = ["a", "b", "c", "d"]
        rng = np.random.default_rng(4)
        training_vals = rng.choice(cats, 2000, p=[0.4, 0.3, 0.2, 0.1]).tolist()
        # Production is heavily skewed toward "d"
        production_vals = rng.choice(cats, 1000, p=[0.05, 0.05, 0.05, 0.85]).tolist()
        training_df = pd.DataFrame({"cat_feat": training_vals})
        report = detector.compare(
            training_df,
            {"cat_feat": production_vals},
            {"cat_feat": FeatureType.CATEGORICAL},
        )
        assert "cat_feat" in report.drifted_features

    def test_insufficient_production_samples_skipped(self):
        detector = make_detector()
        import pandas as pd
        training_df = pd.DataFrame({"feat_a": np.random.normal(0, 1, 2000)})
        production_vals = {"feat_a": [1.0, 2.0]}  # too few samples
        report = detector.compare(training_df, production_vals, {"feat_a": FeatureType.CONTINUOUS})
        assert len(report.feature_results) == 0

    def test_report_to_dict_serialisable(self):
        detector = make_detector()
        import json
        import pandas as pd
        rng = np.random.default_rng(5)
        training_df = pd.DataFrame({"feat_a": rng.normal(0, 1, 500)})
        production_vals = {"feat_a": rng.normal(0, 1, 500).tolist()}
        report = detector.compare(training_df, production_vals, {"feat_a": FeatureType.CONTINUOUS})
        serialised = json.dumps(report.to_dict())
        assert "feat_a" in serialised


class TestRetrainingTrigger:
    def test_no_trigger_below_threshold(self):
        from feature_store.drift_detector import DriftReport
        from feature_store.retraining_trigger import RetrainingTrigger

        trigger = RetrainingTrigger()
        report = DriftReport(
            generated_at=0.0,
            drifted_features=[],
            feature_results=[],
            overall_drift_score=0.10,
        )
        assert not trigger.should_trigger(report)

    def test_trigger_above_threshold(self):
        from feature_store.drift_detector import DriftReport
        from feature_store.retraining_trigger import RetrainingTrigger

        trigger = RetrainingTrigger()
        report = DriftReport(
            generated_at=0.0,
            drifted_features=["feat_a", "feat_b"],
            feature_results=[],
            overall_drift_score=0.50,
        )
        assert trigger.should_trigger(report)
