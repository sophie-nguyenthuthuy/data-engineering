"""Staging zone + runner integration tests."""

from __future__ import annotations

import datetime as dt
import json

from msc.manifest import Manifest
from msc.naming import NamingConvention, StagedKey
from msc.runner import Runner
from msc.sources.base import Record, Source
from msc.staging.zone import StagingZone


class _StaticSource(Source):
    """Test double: emits a fixed list of records."""

    kind = "fake"

    def __init__(self, records, dataset="orders"):
        self._records = list(records)
        self.dataset = dataset

    def fetch(self):
        yield from self._records


def test_staging_zone_writes_jsonl(tmp_path):
    zone = StagingZone(root=tmp_path)
    key = StagedKey(source="csv", dataset="orders", partition="2026/05/13", run_id="r1")
    src = [Record(source_id="r1", fields={"a": 1}), Record(source_id="r2", fields={"a": 2})]
    report = zone.write(key, src)
    assert report.row_count == 2
    text = (tmp_path / key.path()).read_text()
    lines = [json.loads(line) for line in text.splitlines()]
    assert lines[0]["source_id"] == "r1"
    assert lines[1]["fields"] == {"a": 2}


def test_staging_zone_exists_returns_true_after_write(tmp_path):
    zone = StagingZone(root=tmp_path)
    key = StagedKey(source="csv", dataset="orders", partition="2026/05/13", run_id="r1")
    zone.write(key, [Record(source_id="r1", fields={"a": 1})])
    assert zone.exists(key)


def test_staging_zone_list_paths_skips_tmp_files(tmp_path):
    zone = StagingZone(root=tmp_path)
    key = StagedKey(source="csv", dataset="orders", partition="2026/05/13", run_id="r1")
    zone.write(key, [Record(source_id="r1")])
    # Drop a stray .tmp file by hand to simulate an interrupted write.
    (tmp_path / "csv" / "orders" / "2026" / "05" / "13" / "ghost.jsonl.tmp").write_text("x")
    paths = zone.list_paths()
    assert key.path() in paths
    assert all(not p.endswith(".tmp") for p in paths)


def test_runner_writes_through_to_staging(tmp_path):
    zone = StagingZone(root=tmp_path / "stg")
    m = Manifest(path=tmp_path / "manifest.jsonl")
    src = _StaticSource(
        [Record(source_id="r1", fields={"a": 1}), Record(source_id="r2", fields={"a": 2})],
        dataset="orders",
    )
    result = Runner(zone=zone, manifest=m).ingest(src)
    assert result.row_count == 2
    assert not result.skipped
    assert zone.exists(StagedKey.parse(result.staged_path))


def test_runner_idempotent_on_replay(tmp_path):
    zone = StagingZone(root=tmp_path / "stg")
    m = Manifest(path=tmp_path / "manifest.jsonl")
    src = _StaticSource([Record(source_id="r1")], dataset="orders")
    now = dt.datetime(2026, 5, 13, 12, tzinfo=dt.timezone.utc)
    r1 = Runner(zone=zone, manifest=m, now=now).ingest(src, run_id="fixed")
    r2 = Runner(zone=zone, manifest=m, now=now).ingest(src, run_id="fixed")
    assert r1.skipped is False
    assert r2.skipped is True
    assert r1.staged_path == r2.staged_path


def test_runner_records_manifest_entry(tmp_path):
    zone = StagingZone(root=tmp_path / "stg")
    m = Manifest(path=tmp_path / "manifest.jsonl")
    src = _StaticSource([Record(source_id="r1"), Record(source_id="r2")], dataset="orders")
    Runner(zone=zone, manifest=m).ingest(src)
    entries = m.entries()
    assert len(entries) == 1
    assert entries[0].row_count == 2
    assert entries[0].source == "fake"


def test_runner_uses_naming_convention(tmp_path):
    zone = StagingZone(root=tmp_path / "stg")
    m = Manifest(path=tmp_path / "manifest.jsonl")
    src = _StaticSource([Record(source_id="r1")], dataset="My Orders!")
    when = dt.datetime(2026, 5, 13, 12, tzinfo=dt.timezone.utc)
    r = Runner(zone=zone, manifest=m, naming=NamingConvention(), now=when).ingest(
        src, run_id="rid1"
    )
    key = StagedKey.parse(r.staged_path)
    assert key.source == "fake"
    assert key.dataset == "my_orders"
    assert key.partition == "2026/05/13"
    assert key.run_id == "rid1"


def test_runner_ingest_many_returns_per_source_results(tmp_path):
    zone = StagingZone(root=tmp_path / "stg")
    m = Manifest(path=tmp_path / "manifest.jsonl")
    sources = [
        _StaticSource([Record(source_id="r1")], dataset="a"),
        _StaticSource([Record(source_id="r1"), Record(source_id="r2")], dataset="b"),
    ]
    results = Runner(zone=zone, manifest=m).ingest_many(sources)
    assert [r.row_count for r in results] == [1, 2]
