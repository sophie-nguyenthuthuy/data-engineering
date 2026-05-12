"""Tests for drift detectors."""
import numpy as np
import pytest

from lsm_learned.drift.detector import ADWINDetector, KSWindowDetector


def test_adwin_no_drift_stable():
    rng = np.random.default_rng(0)
    det = ADWINDetector(delta=0.002, min_window=30)
    signals = []
    for v in rng.normal(10, 1, size=500):
        s = det.add(float(v))
        if s:
            signals.append(s)
    # ADWIN at δ=0.002 on 500 samples may produce a handful of false positives; ≤5 is acceptable
    assert len(signals) <= 5, f"too many false drift signals on stable stream: {len(signals)}"


def test_adwin_detects_mean_shift():
    rng = np.random.default_rng(1)
    det = ADWINDetector(delta=0.002, min_window=30)
    detected = False
    # Phase 1: mean=5
    for v in rng.normal(5, 1, size=200):
        det.add(float(v))
    # Phase 2: mean=50 (obvious shift)
    for v in rng.normal(50, 1, size=300):
        s = det.add(float(v))
        if s:
            detected = True
            break
    assert detected, "ADWIN failed to detect a 10x mean shift"


def test_ks_no_drift():
    rng = np.random.default_rng(2)
    det = KSWindowDetector(ref_size=200, recent_size=100, alpha=0.005)
    signals = []
    for v in rng.normal(0, 1, size=600):
        s = det.add(float(v))
        if s:
            signals.append(s)
    assert len(signals) == 0, "KS raised drift on stable distribution"


def test_ks_detects_distribution_change():
    rng = np.random.default_rng(3)
    det = KSWindowDetector(ref_size=300, recent_size=150, alpha=0.01)
    detected = False
    # Reference: N(0,1)
    for v in rng.normal(0, 1, size=400):
        det.add(float(v))
    # Shifted: N(10,1)
    for v in rng.normal(10, 1, size=400):
        s = det.add(float(v))
        if s:
            detected = True
            break
    assert detected, "KS failed to detect a large mean shift"


def test_adwin_window_shrinks_on_drift():
    rng = np.random.default_rng(4)
    det = ADWINDetector(delta=0.002, min_window=20)
    for v in rng.normal(0, 1, size=300):
        det.add(float(v))
    size_before = det.window_size
    # Inject extreme values
    for v in rng.normal(1000, 1, size=200):
        det.add(float(v))
    # Window should have been trimmed
    assert det.window_size < size_before + 200
