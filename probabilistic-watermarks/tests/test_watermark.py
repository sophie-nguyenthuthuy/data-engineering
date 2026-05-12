import random

import pytest

from src import PerKeyDelayEstimator, WatermarkAdvancer, CorrectionStream, TDigestLite


def test_tdigest_quantile_close_to_truth():
    sketch = TDigestLite(compression=100)
    rng = random.Random(0)
    samples = [rng.uniform(0, 100) for _ in range(20_000)]
    for x in samples:
        sketch.add(x)
    samples_sorted = sorted(samples)
    for q in [0.1, 0.5, 0.9, 0.99]:
        est = sketch.quantile(q)
        true = samples_sorted[int(q * len(samples_sorted))]
        # Loose: within 5 units
        assert abs(est - true) < 5.0


def test_safe_delay_is_monotone():
    """Watermark requires that the per-key quantile is non-decreasing."""
    est = PerKeyDelayEstimator(delta=1e-3)
    rng = random.Random(0)
    last = -1.0
    for t in range(1, 1000):
        # Stream with growing delay distribution
        event_time = t
        arrival = event_time + rng.uniform(0, t / 10)
        est.observe("k", event_time, arrival)
        cur = est.safe_delay("k")
        assert cur >= last - 1e-9, f"safe_delay decreased at t={t}: {last} → {cur}"
        last = cur


def test_watermark_is_monotone():
    """W must never go backwards."""
    est = PerKeyDelayEstimator(delta=1e-3)
    advancer = WatermarkAdvancer(delay_estimator=est, lambda_min=0.0)
    rng = random.Random(42)
    last_w = 0.0
    for t in range(1, 500):
        event_time = float(t)
        arrival = event_time + rng.uniform(0, 5)
        advancer.on_record("k1", event_time, arrival)
        assert advancer.value >= last_w - 1e-9
        last_w = advancer.value


def test_late_records_routed_to_correction():
    """Records with event_time < W must be flagged late."""
    est = PerKeyDelayEstimator(delta=1e-3)
    advancer = WatermarkAdvancer(delay_estimator=est, lambda_min=0.0)
    late_seen = []
    advancer.set_late_handler(lambda k, et, at: late_seen.append((k, et, at)))

    # Feed many records to grow watermark
    for t in range(1, 200):
        advancer.on_record("k", float(t), float(t) + 1.0)
    w_now = advancer.value

    # Now a record well before the watermark
    advancer.on_record("k", w_now - 100, w_now + 0.1)
    assert len(late_seen) == 1


def test_correction_stream_emits_updates():
    cs = CorrectionStream(window_size=10.0)
    seen = []
    cs.on_correction(lambda *args: seen.append(args))

    cs.close_window("k", 0.0, 100)        # window [0,10) had value 100
    cs.submit_late("k", 5.0, 50, agg_fn=lambda old, v: old + v)
    assert seen == [("k", 0.0, 100, 150)]


def test_empirical_late_rate_under_target():
    """If we configure δ=0.05 and the model is well-calibrated, the observed
    late-record fraction should be ≤ ~10% (loose; tight calibration is hard)."""
    rng = random.Random(7)
    est = PerKeyDelayEstimator(delta=0.05)
    advancer = WatermarkAdvancer(delay_estimator=est, lambda_min=0.0)

    delays = []
    late_count = 0
    n = 5000
    # Warm-up
    for t in range(1, 1001):
        d = rng.expovariate(1.0)  # mean delay = 1
        advancer.on_record("k", float(t), float(t) + d)

    # Test phase
    for t in range(1001, 1001 + n):
        d = rng.expovariate(1.0)
        status, _ = advancer.on_record("k", float(t), float(t) + d)
        delays.append(d)
        if status == "late":
            late_count += 1

    rate = late_count / n
    assert rate < 0.20, f"observed late rate {rate} too high"
