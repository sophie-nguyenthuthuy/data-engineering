"""Tests for the batch processor and offline store integration."""
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from feature_store.batch_processor import BatchProcessor
from feature_store.offline_store import OfflineStore


def make_raw_df(n: int = 100) -> pd.DataFrame:
    import numpy as np
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "user_id": [f"user_{i:04d}" for i in range(n)],
        "amount": rng.lognormal(5.5, 1.0, n),
        "category": rng.choice(["groceries", "dining", "travel", "retail"], n).tolist(),
        "timestamp": ["2024-06-15T12:00:00+00:00"] * n,
        "user_age": rng.integers(20, 70, n).tolist(),
        "account_created_at": ["2022-01-01T00:00:00+00:00"] * n,
    })


@pytest.fixture
def tmp_offline_store(tmp_path):
    return OfflineStore(base_path=str(tmp_path))


@pytest.fixture
def processor(tmp_offline_store):
    return BatchProcessor(offline_store=tmp_offline_store)


def test_process_returns_correct_columns(processor):
    raw_df = make_raw_df(50)
    features_df = processor.process(raw_df)
    assert "entity_id" in features_df.columns
    assert "event_timestamp" in features_df.columns
    assert "amount_log1p" in features_df.columns
    assert "amount_bucket" in features_df.columns
    assert len(features_df) == 50


def test_process_no_nulls_in_output(processor):
    raw_df = make_raw_df(50)
    features_df = processor.process(raw_df)
    # All features should have values (defaults fill in)
    assert features_df.isnull().sum().sum() == 0


def test_global_stats_computed(processor):
    raw_df = make_raw_df(200)
    stats = processor.compute_global_stats(raw_df)
    assert "amount_mean" in stats
    assert "amount_stddev" in stats
    assert stats["amount_mean"] > 0


def test_run_full_pipeline_persists(processor, tmp_offline_store):
    raw_df = make_raw_df(100)
    features_df = processor.run_full_pipeline(raw_df, label="test")
    assert len(features_df) == 100
    # Should be readable back from offline store
    loaded = tmp_offline_store.read_partition("training_test")
    assert len(loaded) == 100


def test_offline_store_append(tmp_offline_store):
    df1 = pd.DataFrame({"a": [1, 2, 3]})
    df2 = pd.DataFrame({"a": [4, 5, 6]})
    tmp_offline_store.write_batch(df1, partition="test")
    tmp_offline_store.write_batch(df2, partition="test")
    result = tmp_offline_store.read_partition("test")
    assert len(result) == 6


def test_offline_store_stats_roundtrip(tmp_offline_store):
    stats = {"amount_mean": 250.0, "amount_stddev": 150.0}
    tmp_offline_store.write_stats(stats, "global")
    loaded = tmp_offline_store.read_stats("global")
    assert abs(loaded["amount_mean"] - 250.0) < 1e-6
