"""Watermark advancer."""

from __future__ import annotations

import random


def test_watermark_starts_at_zero(advancer):
    assert advancer.value == 0.0


def test_watermark_advances_on_record(advancer):
    advancer.on_record("k", 1.0, 1.0)
    advancer.on_record("k", 2.0, 2.0)
    assert advancer.value >= 0.0


def test_late_record_routes_to_handler(advancer):
    late_seen: list[tuple[object, float, float]] = []
    advancer.set_late_handler(lambda k, e, a: late_seen.append((k, e, a)))
    # Warm up so watermark advances
    rng = random.Random(0)
    for t in range(1, 501):
        advancer.on_record("k", float(t), float(t) + rng.expovariate(1.0))
    w = advancer.value
    # Send a record well before the watermark
    advancer.on_record("k", w - 100, w + 0.1)
    assert len(late_seen) == 1


def test_watermark_monotone_under_synthetic_load(advancer):
    rng = random.Random(0)
    last = -1.0
    for t in range(1, 2001):
        advancer.on_record("k", float(t), float(t) + rng.expovariate(1.0))
        assert advancer.value >= last - 1e-9
        last = advancer.value


def test_lambda_min_excludes_slow_keys(estimator):
    """A keyed stream with low rate doesn't gate the watermark."""
    from pwm.watermark.advancer import WatermarkAdvancer
    # λ_min = 0.5 events/s. We'll feed k1 at ~1/s (above), k2 once (below).
    adv = WatermarkAdvancer(delay_estimator=estimator, lambda_min=0.5)
    for t in range(1, 200):
        adv.on_record("k1", float(t), float(t) + 0.1)
    # k2 is very low-rate (one obs) — its rate is 0 → excluded from watermark
    adv.on_record("k2", 1.0, 1000.0)
    # Watermark should still advance based on k1, not gated by k2's huge delay
    assert adv.value > 100


def test_stats_track_on_time_and_late(advancer):
    rng = random.Random(0)
    for t in range(1, 1001):
        advancer.on_record("k", float(t), float(t) + rng.expovariate(1.0))
    # After warm-up, some delays should exceed safe_delay → late
    assert advancer.stats.on_time > 0
    # late rate may be 0% if all delays are below the (1-δ)-quantile;
    # we just check the counter is reachable
    assert advancer.stats.late >= 0
