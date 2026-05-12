"""Tests for transforms, feature store, and registry."""
from __future__ import annotations
import pytest
import numpy as np
import pandas as pd
from unittest.mock import AsyncMock, MagicMock

from src.features.transforms import (
    FillMissing, MinMaxScaler, StandardScaler,
    OrdinalEncoder, ClipOutliers, DropNullRows,
    LambdaStep, TransformPipeline,
)
from src.features.registry import FeatureRegistry
from src.features.store import FeatureStore, _compute_column_stats, _infer_type
from src.models import FeatureType, FeatureDefinition


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_df() -> pd.DataFrame:
    np.random.seed(0)
    return pd.DataFrame({
        "age": [25.0, 35.0, np.nan, 45.0, 55.0, 28.0],
        "income": [50000.0, 75000.0, 60000.0, np.nan, 90000.0, 55000.0],
        "segment": ["bronze", "gold", "silver", None, "platinum", "bronze"],
    })


@pytest.fixture
def clean_df() -> pd.DataFrame:
    return pd.DataFrame({
        "age": [25.0, 30.0, 35.0, 40.0, 45.0, 50.0],
        "income": [40000.0, 50000.0, 60000.0, 70000.0, 80000.0, 90000.0],
    })


# ---------------------------------------------------------------------------
# TransformSteps
# ---------------------------------------------------------------------------

class TestFillMissing:
    def test_fills_numerical_with_mean(self, sample_df):
        step = FillMissing(strategy="mean").fit(sample_df)
        out = step.transform(sample_df)
        assert out["age"].isna().sum() == 0
        assert abs(out["age"].iloc[2] - sample_df["age"].mean()) < 1e-6

    def test_fills_categorical_with_mode(self, sample_df):
        step = FillMissing().fit(sample_df)
        out = step.transform(sample_df)
        assert out["segment"].isna().sum() == 0

    def test_no_nulls_unchanged(self, clean_df):
        step = FillMissing().fit(clean_df)
        out = step.transform(clean_df)
        pd.testing.assert_frame_equal(out, clean_df)


class TestMinMaxScaler:
    def test_output_range_zero_to_one(self, clean_df):
        step = MinMaxScaler().fit(clean_df)
        out = step.transform(clean_df)
        for col in out.select_dtypes(include="number").columns:
            assert out[col].min() >= 0.0 - 1e-9
            assert out[col].max() <= 1.0 + 1e-9

    def test_single_value_column(self):
        df = pd.DataFrame({"x": [5.0, 5.0, 5.0]})
        step = MinMaxScaler().fit(df)
        out = step.transform(df)
        assert (out["x"] == 0.0).all()


class TestStandardScaler:
    def test_zero_mean_unit_std(self, clean_df):
        step = StandardScaler().fit(clean_df)
        out = step.transform(clean_df)
        for col in out.columns:
            assert abs(out[col].mean()) < 1e-9
            assert abs(out[col].std() - 1.0) < 0.1   # small n correction

    def test_unseen_columns_ignored(self, clean_df):
        step = StandardScaler(columns=["age"]).fit(clean_df)
        out = step.transform(clean_df)
        # income should be unchanged
        pd.testing.assert_series_equal(out["income"], clean_df["income"])


class TestOrdinalEncoder:
    def test_encodes_categories(self, sample_df):
        df = sample_df.fillna("UNKNOWN")
        step = OrdinalEncoder().fit(df)
        out = step.transform(df)
        assert pd.api.types.is_integer_dtype(out["segment"])

    def test_unseen_category_gets_minus_one(self):
        df_train = pd.DataFrame({"cat": ["a", "b", "c"]})
        df_test = pd.DataFrame({"cat": ["a", "UNSEEN"]})
        step = OrdinalEncoder().fit(df_train)
        out = step.transform(df_test)
        assert out["cat"].iloc[1] == -1


class TestClipOutliers:
    def test_clips_extremes(self):
        df = pd.DataFrame({"x": [1.0, 2.0, 3.0, 100.0, -100.0] * 20})
        step = ClipOutliers(p_low=5.0, p_high=95.0).fit(df)
        out = step.transform(df)
        assert out["x"].max() <= df["x"].quantile(0.95) + 1e-6
        assert out["x"].min() >= df["x"].quantile(0.05) - 1e-6


class TestDropNullRows:
    def test_drops_nulls(self, sample_df):
        step = DropNullRows(columns=["age"]).fit(sample_df)
        out = step.transform(sample_df)
        assert out["age"].isna().sum() == 0
        assert len(out) == len(sample_df) - 1  # one null row dropped

    def test_no_nulls_same_length(self, clean_df):
        step = DropNullRows().fit(clean_df)
        out = step.transform(clean_df)
        assert len(out) == len(clean_df)


class TestTransformPipeline:
    def test_fit_transform_chain(self, sample_df):
        pipeline = TransformPipeline([
            FillMissing(),
            StandardScaler(columns=["age", "income"]),
        ])
        out = pipeline.fit_transform(sample_df)
        assert out["age"].isna().sum() == 0

    def test_transform_without_fit_raises(self, sample_df):
        pipeline = TransformPipeline([FillMissing()])
        with pytest.raises(RuntimeError, match="fit"):
            pipeline.transform(sample_df)

    def test_lambda_step(self, clean_df):
        fn = lambda df: df.assign(age_doubled=df["age"] * 2)
        pipeline = TransformPipeline([LambdaStep(fn, "double_age")])
        out = pipeline.fit_transform(clean_df)
        assert "age_doubled" in out.columns
        assert (out["age_doubled"] == out["age"] * 2).all()


# ---------------------------------------------------------------------------
# FeatureRegistry
# ---------------------------------------------------------------------------

class TestFeatureRegistry:
    def test_loads_yaml(self, tmp_path):
        yml = tmp_path / "features.yml"
        yml.write_text("""
features:
  - name: age
    feature_type: numerical
    description: customer age
  - name: segment
    feature_type: categorical
""")
        reg = FeatureRegistry(str(yml))
        assert len(reg) == 2
        assert reg.get("age").feature_type == FeatureType.NUMERICAL
        assert reg.get("segment").feature_type == FeatureType.CATEGORICAL

    def test_get_missing_returns_none(self, tmp_path):
        yml = tmp_path / "features.yml"
        yml.write_text("features: []")
        reg = FeatureRegistry(str(yml))
        assert reg.get("nonexistent") is None

    def test_get_or_raise(self, tmp_path):
        yml = tmp_path / "features.yml"
        yml.write_text("features: []")
        reg = FeatureRegistry(str(yml))
        with pytest.raises(KeyError):
            reg.get_or_raise("missing")

    def test_numerical_categorical_filter(self, tmp_path):
        yml = tmp_path / "features.yml"
        yml.write_text("""
features:
  - name: a
    feature_type: numerical
  - name: b
    feature_type: numerical
  - name: c
    feature_type: categorical
""")
        reg = FeatureRegistry(str(yml))
        assert len(reg.numerical_features()) == 2
        assert len(reg.categorical_features()) == 1


# ---------------------------------------------------------------------------
# Stats computation
# ---------------------------------------------------------------------------

class TestComputeStats:
    def test_numerical_stats(self):
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        stats = _compute_column_stats(s, "x", FeatureType.NUMERICAL)
        assert stats.mean == pytest.approx(3.0)
        assert stats.min == 1.0
        assert stats.max == 5.0
        assert stats.histogram_edges is not None

    def test_categorical_stats(self):
        s = pd.Series(["a", "a", "b", "c", "a"])
        stats = _compute_column_stats(s, "cat", FeatureType.CATEGORICAL)
        assert stats.top_value == "a"
        assert stats.cardinality == 3
        assert stats.value_counts["a"] == 3

    def test_null_fraction(self):
        s = pd.Series([1.0, None, 3.0, None])
        stats = _compute_column_stats(s, "x", FeatureType.NUMERICAL)
        assert stats.null_fraction == pytest.approx(0.5)

    def test_infer_type_numeric(self):
        assert _infer_type(pd.Series([1.0, 2.0])) == FeatureType.NUMERICAL

    def test_infer_type_categorical(self):
        assert _infer_type(pd.Series(["a", "b"])) == FeatureType.CATEGORICAL
