"""FileEvent + Manifest tests."""

from __future__ import annotations

import pytest

from ifw.events import EventKind, FileEvent
from ifw.manifest import Manifest, ManifestEntry


def _evt(**over):
    base = dict(
        bucket="b",
        key="k",
        size=1,
        last_modified_ms=1_000,
        etag="e1",
        kind=EventKind.CREATED,
    )
    base.update(over)
    return FileEvent(**base)


def test_file_event_validates_bucket_key():
    with pytest.raises(ValueError):
        _evt(bucket="")
    with pytest.raises(ValueError):
        _evt(key="")


def test_file_event_validates_size_and_ts():
    with pytest.raises(ValueError):
        _evt(size=-1)
    with pytest.raises(ValueError):
        _evt(last_modified_ms=-1)


def test_file_event_validates_etag():
    with pytest.raises(ValueError):
        _evt(etag="")


def test_dedupe_key_combines_bucket_key_etag():
    assert _evt(bucket="b", key="k", etag="e1").dedupe_key() == "b/k#e1"


def test_manifest_records_and_reads_back(tmp_path):
    m = Manifest(path=tmp_path / "mf.jsonl")
    m.record(
        ManifestEntry(
            dedupe_key="b/k#e1",
            bucket="b",
            key="k",
            etag="e1",
            last_modified_ms=5_000,
            processed_at_ms=10_000,
        )
    )
    entries = m.entries()
    assert len(entries) == 1
    assert entries[0].dedupe_key == "b/k#e1"


def test_manifest_keys_and_watermark(tmp_path):
    m = Manifest(path=tmp_path / "mf.jsonl")
    m.record(ManifestEntry("b/k1#e", "b", "k1", "e", last_modified_ms=10, processed_at_ms=20))
    m.record(ManifestEntry("b/k2#e", "b", "k2", "e", last_modified_ms=30, processed_at_ms=40))
    assert m.keys() == {"b/k1#e", "b/k2#e"}
    assert m.watermark_ms() == 30


def test_manifest_creates_parent_dir(tmp_path):
    p = tmp_path / "a" / "b" / "mf.jsonl"
    Manifest(path=p)
    assert p.exists()


def test_manifest_watermark_empty_is_zero(tmp_path):
    m = Manifest(path=tmp_path / "mf.jsonl")
    assert m.watermark_ms() == 0
