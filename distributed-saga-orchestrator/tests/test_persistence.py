"""
Tests for SagaStore (SQLite persistence layer).

Covers:
- save / load round-trip for all fields
- upsert semantics (second save updates, not duplicates)
- list_by_status
- list_by_type
- loading a nonexistent saga returns None
- context and step_records survive JSON serialisation
- in-memory and file-based stores
"""

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import pytest

from saga.persistence import SagaRecord, SagaStatus, SagaStore
from saga.step import StepStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_record(saga_id: str = "test-1", status: SagaStatus = SagaStatus.RUNNING) -> SagaRecord:
    return SagaRecord(
        saga_id=saga_id,
        saga_type="unit_test",
        status=status,
        context={"key": "value", "num": 42},
        step_records=[
            {"name": "StepA", "status": StepStatus.COMPLETED.value, "output": {"x": 1}},
            {"name": "StepB", "status": StepStatus.PENDING.value, "output": {}},
        ],
    )


# ---------------------------------------------------------------------------
# Basic save / load
# ---------------------------------------------------------------------------

def test_save_and_load_round_trip() -> None:
    store = SagaStore()
    rec = _make_record()
    store.save(rec)

    loaded = store.load(rec.saga_id)
    assert loaded is not None
    assert loaded.saga_id == rec.saga_id
    assert loaded.saga_type == rec.saga_type
    assert loaded.status == SagaStatus.RUNNING
    assert loaded.context == {"key": "value", "num": 42}
    assert len(loaded.step_records) == 2
    assert loaded.step_records[0]["name"] == "StepA"


def test_load_nonexistent_returns_none() -> None:
    store = SagaStore()
    assert store.load("does-not-exist") is None


def test_save_updates_existing_record() -> None:
    store = SagaStore()
    rec = _make_record()
    store.save(rec)

    rec.status = SagaStatus.COMPLETED
    rec.context["extra"] = "added"
    store.save(rec)

    loaded = store.load(rec.saga_id)
    assert loaded.status == SagaStatus.COMPLETED
    assert loaded.context["extra"] == "added"

    # Should still be just one row
    all_records = store.list_by_type("unit_test")
    assert len(all_records) == 1


def test_updated_at_advances_on_second_save() -> None:
    store = SagaStore()
    rec = _make_record()
    store.save(rec)
    first_updated = store.load(rec.saga_id).updated_at

    time.sleep(0.01)
    store.save(rec)
    second_updated = store.load(rec.saga_id).updated_at

    assert second_updated >= first_updated


# ---------------------------------------------------------------------------
# Nullable / optional fields
# ---------------------------------------------------------------------------

def test_failure_fields_saved_and_loaded() -> None:
    store = SagaStore()
    rec = _make_record(status=SagaStatus.COMPENSATED)
    rec.failure_step = "StepB"
    rec.failure_reason = "timeout on DB call"
    rec.compensation_errors = [{"step": "StepA", "error": "network error"}]
    rec.completed_at = time.time()
    store.save(rec)

    loaded = store.load(rec.saga_id)
    assert loaded.failure_step == "StepB"
    assert loaded.failure_reason == "timeout on DB call"
    assert loaded.compensation_errors[0]["step"] == "StepA"
    assert loaded.completed_at is not None


def test_null_optional_fields_round_trip() -> None:
    store = SagaStore()
    rec = _make_record()
    assert rec.failure_step is None
    assert rec.completed_at is None
    store.save(rec)

    loaded = store.load(rec.saga_id)
    assert loaded.failure_step is None
    assert loaded.completed_at is None


# ---------------------------------------------------------------------------
# list_by_status
# ---------------------------------------------------------------------------

def test_list_by_status_returns_matching_records() -> None:
    store = SagaStore()
    r1 = _make_record("s1", SagaStatus.RUNNING)
    r2 = _make_record("s2", SagaStatus.COMPLETED)
    r3 = _make_record("s3", SagaStatus.RUNNING)
    for r in (r1, r2, r3):
        store.save(r)

    running = store.list_by_status(SagaStatus.RUNNING)
    ids = {r.saga_id for r in running}
    assert ids == {"s1", "s3"}


def test_list_by_status_empty_when_none_match() -> None:
    store = SagaStore()
    store.save(_make_record("x", SagaStatus.COMPLETED))
    assert store.list_by_status(SagaStatus.COMPENSATING) == []


# ---------------------------------------------------------------------------
# list_by_type
# ---------------------------------------------------------------------------

def test_list_by_type_filters_correctly() -> None:
    store = SagaStore()

    r1 = _make_record("t1")
    r1.saga_type = "pipeline_A"
    r2 = _make_record("t2")
    r2.saga_type = "pipeline_B"
    r3 = _make_record("t3")
    r3.saga_type = "pipeline_A"
    for r in (r1, r2, r3):
        store.save(r)

    a_records = store.list_by_type("pipeline_A")
    assert len(a_records) == 2
    assert all(r.saga_type == "pipeline_A" for r in a_records)


# ---------------------------------------------------------------------------
# Complex context / step_records serialisation
# ---------------------------------------------------------------------------

def test_nested_context_survives_serialisation() -> None:
    store = SagaStore()
    rec = _make_record()
    rec.context = {
        "nested": {"a": [1, 2, 3], "b": {"c": True}},
        "unicode": "héllo wörld",
        "float": 3.14159,
    }
    store.save(rec)

    loaded = store.load(rec.saga_id)
    assert loaded.context["nested"]["a"] == [1, 2, 3]
    assert loaded.context["unicode"] == "héllo wörld"
    assert abs(loaded.context["float"] - 3.14159) < 1e-9


def test_step_records_with_full_fields_survive() -> None:
    store = SagaStore()
    rec = _make_record()
    now = time.time()
    rec.step_records = [
        {
            "name": "FullStep",
            "status": StepStatus.COMPENSATED.value,
            "output": {"result": "ok"},
            "error_message": None,
            "attempts": 3,
            "started_at": now - 2.0,
            "completed_at": now - 1.0,
            "compensated_at": now,
        }
    ]
    store.save(rec)

    loaded = store.load(rec.saga_id)
    sr = loaded.step_records[0]
    assert sr["status"] == StepStatus.COMPENSATED.value
    assert sr["attempts"] == 3
    assert sr["output"] == {"result": "ok"}


# ---------------------------------------------------------------------------
# File-based store
# ---------------------------------------------------------------------------

def test_file_based_store_persists_across_reopen() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"

        store1 = SagaStore(db_path)
        rec = _make_record("file-saga", SagaStatus.COMPLETED)
        store1.save(rec)
        store1.close()

        store2 = SagaStore(db_path)
        loaded = store2.load("file-saga")
        assert loaded is not None
        assert loaded.status == SagaStatus.COMPLETED
        store2.close()


def test_multiple_sagas_isolated_in_same_store() -> None:
    store = SagaStore()
    for i in range(10):
        rec = _make_record(f"saga-{i}", SagaStatus.COMPLETED)
        rec.context = {"index": i}
        store.save(rec)

    for i in range(10):
        loaded = store.load(f"saga-{i}")
        assert loaded is not None
        assert loaded.context["index"] == i
