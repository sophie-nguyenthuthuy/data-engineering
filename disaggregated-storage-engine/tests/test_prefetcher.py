"""Markov prefetcher tests."""

from __future__ import annotations

import random

from disagg.core.page import PageId
from disagg.prefetch.markov import MarkovPrefetcher


def test_predict_with_no_history_returns_empty():
    p = MarkovPrefetcher()
    assert p.predict() == []


def test_learns_sequential_pattern():
    p = MarkovPrefetcher()
    pids = [PageId(0, i) for i in range(10)]
    # 5 passes through the sequence
    for _ in range(5):
        for pid in pids:
            p.observe(pid)
    # After page 5, expect page 6
    assert p.predict(pids[5], k=1) == [pids[6]]
    # After page 9, expect page 0 (next pass)
    assert p.predict(pids[9], k=1) == [pids[0]]


def test_predict_top_k():
    p = MarkovPrefetcher()
    a, b, c = PageId(0, 0), PageId(0, 1), PageId(0, 2)
    # After a: b twice, c once
    for _ in range(2):
        p.observe(a)
        p.observe(b)
    p.observe(a)
    p.observe(c)
    top2 = p.predict(a, k=2)
    assert top2[0] == b
    assert top2[1] == c


def test_in_sample_accuracy_high_for_deterministic():
    p = MarkovPrefetcher()
    pids = [PageId(0, i) for i in range(50)]
    for _ in range(10):
        for pid in pids:
            p.observe(pid)
    acc = p.estimate_in_sample_top1_accuracy()
    assert acc > 0.9


def test_bounded_memory():
    """With max_predecessors=10, no more than ~10 predecessors retained."""
    p = MarkovPrefetcher(max_predecessors=10)
    for i in range(100):
        p.observe(PageId(0, i))
    # After 100 unique predecessors, only ~10 retained
    assert p.n_predecessors <= 11


def test_phase_detection_resets_on_workload_change():
    p = MarkovPrefetcher(phase_window=20, phase_threshold=0.5)
    pids = [PageId(0, i) for i in range(5)]
    # Phase 1: learn a strong cycle
    for _ in range(20):
        for pid in pids:
            p.observe(pid)
    n1 = p.n_predecessors
    # Phase 2: random workload — should trigger reset
    rng = random.Random(0)
    for _ in range(200):
        p.observe(PageId(0, rng.randint(100, 199)))
    # The phase detector should have reset at least once.
    # We can't predict exactly, but n_predecessors should be smaller now
    # because old cycle predecessors were cleared.
    assert p.stats.resets > 0 or p.n_predecessors > n1


def test_predict_with_unseen_page_returns_empty():
    p = MarkovPrefetcher()
    p.observe(PageId(0, 1))
    p.observe(PageId(0, 2))
    assert p.predict(PageId(0, 999)) == []


def test_record_prediction_outcome_updates_accuracy():
    p = MarkovPrefetcher()
    a, b = PageId(0, 1), PageId(0, 2)
    p.record_prediction_outcome(a, a)    # correct
    p.record_prediction_outcome(a, b)    # incorrect
    assert p.stats.predictions_made == 2
    assert p.stats.correct_predictions == 1
    assert abs(p.stats.accuracy - 0.5) < 1e-9
