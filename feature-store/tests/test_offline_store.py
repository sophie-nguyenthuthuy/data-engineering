"""Offline Parquet store tests."""
from __future__ import annotations

from datetime import datetime, timezone, date

import pandas as pd
import pytest

from feature_store.offline.parquet_store import OfflineStore


@pytest.fixture()
def store(tmp_path):
    return OfflineStore(
        base_path=tmp_path / "offline",
        write_batch_size=5,     # low threshold to force flushes
        row_group_size=1024,
    )


class TestWrite:
    def test_write_and_read_roundtrip(self, store):
        ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        store.write("users", "u1", {"score": 0.9, "count": 5}, event_timestamp=ts)
        store.flush()
        df = store.read("users")
        assert len(df) == 1
        assert df.iloc[0]["entity_id"] == "u1"
        assert abs(df.iloc[0]["score"] - 0.9) < 1e-5

    def test_batch_write_triggers_flush(self, store):
        records = [
            (f"u{i}", {"score": float(i)}, None)
            for i in range(10)  # above write_batch_size=5
        ]
        store.write_batch("users", records)
        store.flush()
        df = store.read("users")
        assert len(df) == 10

    def test_multiple_groups(self, store):
        store.write("users", "u1", {"score": 1.0})
        store.write("items", "i1", {"pop": 0.5})
        store.flush()
        assert len(store.read("users")) == 1
        assert len(store.read("items")) == 1
        assert store.read("nonexistent").empty


class TestFilters:
    def test_date_filter(self, store):
        ts_jun = datetime(2024, 6, 1, tzinfo=timezone.utc)
        ts_jul = datetime(2024, 7, 1, tzinfo=timezone.utc)
        store.write("g", "e1", {"v": 1}, event_timestamp=ts_jun)
        store.write("g", "e2", {"v": 2}, event_timestamp=ts_jul)
        store.flush()

        df = store.read("g", end_date=date(2024, 6, 30))
        assert len(df) == 1
        assert df.iloc[0]["entity_id"] == "e1"

    def test_entity_filter(self, store):
        for i in range(5):
            store.write("g", f"u{i}", {"v": i})
        store.flush()
        df = store.read("g", entity_ids=["u0", "u2"])
        assert set(df["entity_id"].tolist()) == {"u0", "u2"}


class TestPointInTime:
    def test_pit_join_no_leakage(self, store):
        # Feature recorded at T=6, label at T=5 → join should yield no feature
        ts_feature = datetime(2024, 1, 6, tzinfo=timezone.utc)
        store.write("users", "u1", {"score": 0.9}, event_timestamp=ts_feature)
        store.flush()

        entity_df = pd.DataFrame([
            {"entity_id": "u1", "label_timestamp": datetime(2024, 1, 5, tzinfo=timezone.utc)}
        ])
        result = store.point_in_time_join(entity_df, "users", ["score"])
        # score should be NaN because feature postdates the label
        assert pd.isna(result["score"].iloc[0])

    def test_pit_join_with_valid_feature(self, store):
        ts_feature = datetime(2024, 1, 4, tzinfo=timezone.utc)
        store.write("users", "u1", {"score": 0.7}, event_timestamp=ts_feature)
        store.flush()

        entity_df = pd.DataFrame([
            {"entity_id": "u1", "label_timestamp": datetime(2024, 1, 5, tzinfo=timezone.utc)}
        ])
        result = store.point_in_time_join(entity_df, "users", ["score"])
        assert abs(result["score"].iloc[0] - 0.7) < 1e-5


class TestStats:
    def test_stats_empty(self, store):
        stats = store.get_stats("nonexistent")
        assert stats["row_count"] == 0

    def test_stats_populated(self, store):
        for i in range(3):
            store.write("g", f"u{i}", {"v": i})
        store.flush()
        stats = store.get_stats("g")
        assert stats["row_count"] == 3
        assert stats["entity_count"] == 3
