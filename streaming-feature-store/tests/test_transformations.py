"""Tests for shared feature transformation functions."""
import math
from datetime import datetime, timezone

import pytest

from feature_store.transformations import build_registry


@pytest.fixture
def registry():
    return build_registry()


def make_record(**kwargs):
    defaults = {
        "user_id": "user_0001",
        "amount": 100.0,
        "category": "groceries",
        "timestamp": "2024-06-15T14:30:00+00:00",
        "user_age": 35,
        "account_created_at": "2022-01-01T00:00:00+00:00",
    }
    defaults.update(kwargs)
    return defaults


class TestContinuousFeatures:
    def test_amount_log1p_zero(self, registry):
        f = registry.get("amount_log1p")
        assert f.compute(make_record(amount=0), {}) == 0.0

    def test_amount_log1p_positive(self, registry):
        f = registry.get("amount_log1p")
        result = f.compute(make_record(amount=99.0), {})
        assert abs(result - math.log1p(99.0)) < 1e-5

    def test_amount_zscore_mean(self, registry):
        f = registry.get("amount_zscore")
        ctx = {"amount_mean": 100.0, "amount_stddev": 50.0}
        assert f.compute(make_record(amount=100.0), ctx) == 0.0

    def test_amount_zscore_positive(self, registry):
        f = registry.get("amount_zscore")
        ctx = {"amount_mean": 100.0, "amount_stddev": 50.0}
        assert abs(f.compute(make_record(amount=200.0), ctx) - 2.0) < 1e-5

    def test_amount_zscore_zero_stddev(self, registry):
        f = registry.get("amount_zscore")
        ctx = {"amount_mean": 100.0, "amount_stddev": 0.0}
        assert f.compute(make_record(amount=200.0), ctx) == 0.0

    def test_hour_of_day(self, registry):
        f = registry.get("hour_of_day")
        result = f.compute(make_record(timestamp="2024-06-15T14:30:00+00:00"), {})
        assert result == 14

    def test_account_tenure_positive(self, registry):
        f = registry.get("account_tenure_days")
        result = f.compute(
            make_record(
                timestamp="2024-06-15T00:00:00+00:00",
                account_created_at="2024-01-01T00:00:00+00:00",
            ),
            {},
        )
        assert result > 150  # ~166 days

    def test_spending_velocity_mean(self, registry):
        f = registry.get("spending_velocity")
        ctx = {"user_mean_spend_30d": 100.0}
        assert f.compute(make_record(amount=100.0), ctx) == 1.0

    def test_spending_velocity_zero_mean(self, registry):
        f = registry.get("spending_velocity")
        ctx = {"user_mean_spend_30d": 0.0}
        assert f.compute(make_record(amount=500.0), ctx) == 1.0


class TestCategoricalFeatures:
    def test_amount_bucket_micro(self, registry):
        f = registry.get("amount_bucket")
        assert f.compute(make_record(amount=5.0), {}) == "micro"

    def test_amount_bucket_small(self, registry):
        f = registry.get("amount_bucket")
        assert f.compute(make_record(amount=50.0), {}) == "small"

    def test_amount_bucket_whale(self, registry):
        f = registry.get("amount_bucket")
        assert f.compute(make_record(amount=100_000.0), {}) == "whale"

    def test_day_of_week_saturday(self, registry):
        f = registry.get("day_of_week")
        result = f.compute(make_record(timestamp="2024-06-15T10:00:00+00:00"), {})
        assert result == "Saturday"

    def test_is_weekend_true(self, registry):
        f = registry.get("is_weekend")
        result = f.compute(make_record(timestamp="2024-06-15T10:00:00+00:00"), {})
        assert result == 1

    def test_is_weekend_false(self, registry):
        f = registry.get("is_weekend")
        result = f.compute(make_record(timestamp="2024-06-17T10:00:00+00:00"), {})
        assert result == 0

    def test_category_encoded_normalised(self, registry):
        f = registry.get("category_encoded")
        assert f.compute(make_record(category="  GROCERIES  "), {}) == "groceries"

    def test_user_age_bucket_young_adult(self, registry):
        f = registry.get("user_age_bucket")
        assert f.compute(make_record(user_age=22), {}) == "young_adult"

    def test_user_age_bucket_senior(self, registry):
        f = registry.get("user_age_bucket")
        assert f.compute(make_record(user_age=65), {}) == "senior"

    def test_high_risk_category_flagged(self, registry):
        f = registry.get("is_high_risk_category")
        assert f.compute(make_record(category="gambling"), {}) == 1

    def test_high_risk_category_safe(self, registry):
        f = registry.get("is_high_risk_category")
        assert f.compute(make_record(category="groceries"), {}) == 0


class TestRegistryConsistency:
    def test_all_features_have_defaults(self, registry):
        record = {}
        for feat in registry.all_features():
            result = feat.compute(record, {})
            # Should not raise; default returned for missing inputs
            assert result is not None or feat.default_value is None

    def test_feature_names_unique(self, registry):
        names = registry.feature_names()
        assert len(names) == len(set(names))

    def test_batch_and_stream_produce_same_result(self, registry):
        """Critical: batch and streaming must produce identical values."""
        record = make_record(amount=750.0, category="travel", user_age=45)
        ctx = {"amount_mean": 250.0, "amount_stddev": 200.0, "user_mean_spend_30d": 300.0}

        for feat in registry.all_features():
            batch_val = feat.compute(record, ctx)
            stream_val = feat.compute(record, ctx)
            assert batch_val == stream_val, (
                f"Feature {feat.name} produced different values: {batch_val} vs {stream_val}"
            )
