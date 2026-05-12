"""Tests for SchemaStore schema diffing and event logging."""

from datetime import datetime, timedelta

from pipeline_rca.models import ChangeCategoryKind
from pipeline_rca.monitors.schema_monitor import SchemaStore


class TestSchemaStore:
    def setup_method(self):
        self.store = SchemaStore(":memory:")
        self.t0 = datetime(2024, 3, 1, 12, 0, 0)

    def test_no_changes_on_first_snapshot(self):
        cols = [{"name": "id", "type": "INT64"}, {"name": "value", "type": "FLOAT64"}]
        changes = self.store.snapshot_columns("my_table", cols, self.t0)
        assert changes == []

    def test_detects_column_added(self):
        cols_v1 = [{"name": "id", "type": "INT64"}]
        cols_v2 = [{"name": "id", "type": "INT64"}, {"name": "extra", "type": "STRING"}]
        self.store.snapshot_columns("t", cols_v1, self.t0)
        changes = self.store.snapshot_columns("t", cols_v2, self.t0 + timedelta(days=1))
        assert len(changes) == 1
        assert changes[0].kind == ChangeCategoryKind.COLUMN_ADDED
        assert changes[0].column == "extra"

    def test_detects_column_dropped(self):
        cols_v1 = [{"name": "id", "type": "INT64"}, {"name": "gone", "type": "STRING"}]
        cols_v2 = [{"name": "id", "type": "INT64"}]
        self.store.snapshot_columns("t", cols_v1, self.t0)
        changes = self.store.snapshot_columns("t", cols_v2, self.t0 + timedelta(hours=1))
        assert any(c.kind == ChangeCategoryKind.COLUMN_DROPPED for c in changes)

    def test_detects_type_change(self):
        cols_v1 = [{"name": "amount", "type": "INT64"}]
        cols_v2 = [{"name": "amount", "type": "FLOAT64"}]
        self.store.snapshot_columns("t", cols_v1, self.t0)
        changes = self.store.snapshot_columns("t", cols_v2, self.t0 + timedelta(hours=2))
        assert any(c.kind == ChangeCategoryKind.TYPE_CHANGED for c in changes)

    def test_no_changes_identical_snapshot(self):
        cols = [{"name": "id", "type": "INT64"}]
        self.store.snapshot_columns("t", cols, self.t0)
        changes = self.store.snapshot_columns("t", cols, self.t0 + timedelta(hours=1))
        assert changes == []

    def test_record_pipeline_event(self):
        event = self.store.record_pipeline_event(
            "orders", ChangeCategoryKind.PIPELINE_FAILURE, {"job": "daily_load"}, self.t0
        )
        assert event.kind == ChangeCategoryKind.PIPELINE_FAILURE

    def test_get_recent_changes(self):
        self.store.record_pipeline_event(
            "user_events", ChangeCategoryKind.LATE_DATA, {}, self.t0
        )
        since = self.t0 - timedelta(hours=1)
        changes = self.store.get_recent_changes(["user_events"], since=since)
        assert len(changes) == 1
        assert changes[0].table == "user_events"
