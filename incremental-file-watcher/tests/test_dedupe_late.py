"""Dedupe + late-arrival detector tests."""

from __future__ import annotations

import pytest

from ifw.dedupe import Deduplicator
from ifw.events import FileEvent
from ifw.late import LateArrivalDetector
from ifw.manifest import Manifest, ManifestEntry


def _evt(key="k", lm=0, etag="e1") -> FileEvent:
    return FileEvent(bucket="b", key=key, size=1, last_modified_ms=lm, etag=etag)


# -------------------------------------------------------------- Deduplicator


def test_dedupe_new_event_is_new():
    d = Deduplicator()
    assert d.is_new(_evt())


def test_dedupe_remember_makes_subsequent_seen():
    d = Deduplicator()
    e = _evt()
    d.remember(e)
    assert not d.is_new(e)


def test_dedupe_different_etag_not_deduped():
    d = Deduplicator()
    d.remember(_evt(etag="v1"))
    assert d.is_new(_evt(etag="v2"))


def test_dedupe_from_manifest_rehydrates(tmp_path):
    m = Manifest(path=tmp_path / "mf.jsonl")
    m.record(ManifestEntry("b/k#e1", "b", "k", "e1", last_modified_ms=10, processed_at_ms=20))
    d = Deduplicator.from_manifest(m)
    assert not d.is_new(_evt())


# --------------------------------------------------------- LateArrivalDetector


def test_late_validates_args():
    with pytest.raises(ValueError):
        LateArrivalDetector(watermark_ms=-1)
    with pytest.raises(ValueError):
        LateArrivalDetector(grace_ms=-1)


def test_late_initial_event_never_late():
    d = LateArrivalDetector(watermark_ms=0, grace_ms=0)
    assert not d.is_late(_evt(lm=100))


def test_late_event_below_watermark_minus_grace():
    d = LateArrivalDetector(watermark_ms=10_000, grace_ms=1_000)
    # 8_000 + 1_000 = 9_000 < 10_000 → late
    assert d.is_late(_evt(lm=8_000))


def test_late_event_within_grace_window_is_not_late():
    d = LateArrivalDetector(watermark_ms=10_000, grace_ms=1_000)
    # 9_500 + 1_000 = 10_500 ≥ 10_000 → not late
    assert not d.is_late(_evt(lm=9_500))


def test_late_update_only_advances_watermark():
    d = LateArrivalDetector(watermark_ms=10_000, grace_ms=0)
    d.update(_evt(lm=5_000))  # backwards, ignored
    assert d.watermark_ms == 10_000
    d.update(_evt(lm=20_000))
    assert d.watermark_ms == 20_000
