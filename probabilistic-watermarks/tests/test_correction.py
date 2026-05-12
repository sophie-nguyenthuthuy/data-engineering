"""Correction stream + window state."""

from __future__ import annotations

from pwm.correction.stream import CorrectionStream
from pwm.correction.window import TumblingWindowState


def test_window_add_accumulates():
    w = TumblingWindowState(window_size=10.0)
    ws, v = w.add("k", 1.0, 5, lambda old, new: old + new)
    assert ws == 0.0
    assert v == 5
    ws, v = w.add("k", 3.0, 7, lambda old, new: old + new)
    assert v == 12


def test_window_partitions_by_size():
    w = TumblingWindowState(window_size=10.0)
    w.add("k", 5.0, 1, lambda old, new: old + new)
    w.add("k", 12.0, 1, lambda old, new: old + new)
    assert w.value("k", 0.0) == 1
    assert w.value("k", 10.0) == 1


def test_close_marks_window():
    w = TumblingWindowState(window_size=10.0)
    w.add("k", 5.0, 1, lambda old, new: old + new)
    w.close("k", 0.0)
    assert w.is_closed("k", 0.0)


def test_correction_emitted_for_closed_window():
    cs = CorrectionStream(window=TumblingWindowState(window_size=10.0))
    seen: list = []
    cs.on_correction(seen.append)

    cs.window.add("k", 5.0, 100, lambda old, new: old + new)
    cs.window.close("k", 0.0)
    rec = cs.submit_late("k", 5.0, 50, agg_fn=lambda old, new: old + new)
    assert rec is not None
    assert rec.old_value == 100
    assert rec.new_value == 150
    assert len(seen) == 1


def test_no_correction_for_open_window():
    cs = CorrectionStream(window=TumblingWindowState(window_size=10.0))
    cs.window.add("k", 5.0, 100, lambda old, new: old + new)
    # NOT closed yet
    rec = cs.submit_late("k", 5.0, 50, agg_fn=lambda old, new: old + new)
    assert rec is None
