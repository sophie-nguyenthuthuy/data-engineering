"""Workload generators."""

from __future__ import annotations

import statistics

from pwm.workload import (
    bimodal_workload,
    exponential_delay_workload,
    lognormal_delay_workload,
    pareto_delay_workload,
)


def test_exponential_mean():
    delays = [a - e for k, e, a in exponential_delay_workload(n_events=5000, lambda_rate=2.0)]
    mean = statistics.mean(delays)
    # Mean of Exp(2) = 0.5
    assert 0.3 < mean < 0.7


def test_lognormal_no_negatives():
    delays = [a - e for k, e, a in lognormal_delay_workload(n_events=1000)]
    assert all(d >= 0 for d in delays)


def test_pareto_heavy_tail():
    delays = [a - e for k, e, a in pareto_delay_workload(n_events=5000, alpha=1.5, seed=0)]
    # Heavy tail: max significantly larger than median
    delays_sorted = sorted(delays)
    median = delays_sorted[len(delays) // 2]
    p99 = delays_sorted[int(0.99 * len(delays))]
    assert p99 > 5 * median


def test_bimodal_has_two_regimes():
    """With p_heavy=0.1 and a very wide separation, p99 should dwarf p50."""
    delays = [a - e for k, e, a in bimodal_workload(
        n_events=10_000, p_heavy=0.1,
        mu_light=0.0, sigma_light=0.3,
        mu_heavy=4.0, sigma_heavy=0.5,
        seed=0,
    )]
    delays_sorted = sorted(delays)
    p50 = delays_sorted[5000]
    p99 = delays_sorted[9900]
    assert p99 > 10 * p50, f"p99={p99} vs p50={p50}"


def test_multiple_keys_distribute():
    keys_seen = set()
    for k, _, _ in exponential_delay_workload(n_events=1000, n_keys=10, seed=0):
        keys_seen.add(k)
    assert keys_seen == set(range(10))
