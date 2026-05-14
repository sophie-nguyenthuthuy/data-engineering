"""Hudi CoW + MoR tests."""

from __future__ import annotations

import pytest

from tfl.hudi.table import HudiCoWTable, HudiMoRTable, TimelineAction

# ---------------------------------------------------------------- CoW


def test_cow_upsert_rewrites_base():
    t = HudiCoWTable()
    t.upsert("g1", "base-v1.parquet")
    t.upsert("g1", "base-v2.parquet")
    assert t.read("g1") == "base-v2.parquet"
    assert t.write_amplification() == 2  # two ADDs, both for the same group


def test_cow_upsert_rejects_empty_args():
    t = HudiCoWTable()
    with pytest.raises(ValueError):
        t.upsert("", "p")
    with pytest.raises(ValueError):
        t.upsert("g", "")


def test_cow_timeline_tracks_removed_files():
    t = HudiCoWTable()
    t.upsert("g1", "v1.parquet")
    t.upsert("g1", "v2.parquet")
    timeline = t.timeline()
    assert len(timeline) == 2
    assert timeline[0].files_removed == ()
    assert timeline[1].files_removed == ("v1.parquet",)


def test_cow_files_only_returns_live_base_files():
    t = HudiCoWTable()
    t.upsert("g1", "v1.parquet")
    t.upsert("g2", "g2-v1.parquet")
    t.upsert("g1", "v2.parquet")  # rewrites g1
    assert t.files() == sorted(["v2.parquet", "g2-v1.parquet"])


# ---------------------------------------------------------------- MoR


def test_mor_insert_base_then_append_logs():
    t = HudiMoRTable()
    t.insert_base("g1", "g1-base.parquet")
    t.append_log("g1", "g1-log-1.avro")
    t.append_log("g1", "g1-log-2.avro")
    base, logs = t.read_paths("g1")
    assert base == "g1-base.parquet"
    assert logs == ("g1-log-1.avro", "g1-log-2.avro")
    assert t.write_amplification() == 3  # 1 base + 2 logs


def test_mor_insert_base_rejects_existing_group():
    t = HudiMoRTable()
    t.insert_base("g1", "p")
    with pytest.raises(ValueError):
        t.insert_base("g1", "p2")


def test_mor_append_log_requires_base():
    t = HudiMoRTable()
    with pytest.raises(ValueError):
        t.append_log("g1", "log")


def test_mor_compaction_folds_logs_into_new_base():
    t = HudiMoRTable()
    t.insert_base("g1", "v1.parquet")
    t.append_log("g1", "g1-log-1.avro")
    t.append_log("g1", "g1-log-2.avro")
    t.compact("g1", "v2.parquet")
    base, logs = t.read_paths("g1")
    assert base == "v2.parquet"
    assert logs == ()
    # Timeline records a COMPACTION event with the right files removed.
    last = t.timeline()[-1]
    assert last.action is TimelineAction.COMPACTION
    assert set(last.files_removed) == {"v1.parquet", "g1-log-1.avro", "g1-log-2.avro"}


def test_mor_compact_unknown_group_raises():
    t = HudiMoRTable()
    with pytest.raises(ValueError):
        t.compact("g1", "v2.parquet")


def test_mor_all_groups_lists_known_keys():
    t = HudiMoRTable()
    t.insert_base("g1", "g1.parquet")
    t.insert_base("g2", "g2.parquet")
    assert set(t.all_groups()) == {"g1", "g2"}
