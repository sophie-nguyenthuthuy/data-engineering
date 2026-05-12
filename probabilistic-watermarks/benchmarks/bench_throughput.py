"""Watermark advance throughput across delay-distribution types."""

from __future__ import annotations

import time

from pwm.watermark.advancer import WatermarkAdvancer
from pwm.watermark.estimator import PerKeyDelayEstimator
from pwm.workload import (
    bimodal_workload,
    exponential_delay_workload,
    lognormal_delay_workload,
    pareto_delay_workload,
)


def bench(name: str, gen) -> dict:
    est = PerKeyDelayEstimator(delta=0.01)
    adv = WatermarkAdvancer(delay_estimator=est, lambda_min=0.0)

    n = 0
    start = time.perf_counter()
    for k, e, a in gen:
        adv.on_record(k, e, a)
        n += 1
    elapsed = time.perf_counter() - start

    return {
        "workload": name,
        "events": n,
        "ms": elapsed * 1000,
        "qps": n / elapsed,
        "late_rate": adv.stats.late_rate,
        "final_w": adv.value,
    }


def main() -> None:
    n = 50_000
    print(f"{'workload':<14} {'events':>8} {'ms':>10} {'qps':>10} {'late%':>7} {'final W':>10}")
    for name, gen in [
        ("exp",       exponential_delay_workload(n, lambda_rate=1.0)),
        ("lognormal", lognormal_delay_workload(n, mu=0.0, sigma=0.5)),
        ("pareto",    pareto_delay_workload(n, alpha=2.0)),
        ("bimodal",   bimodal_workload(n, p_heavy=0.05, mu_heavy=3.0)),
    ]:
        r = bench(name, gen)
        print(f"{r['workload']:<14} {r['events']:>8} {r['ms']:>10.1f} "
              f"{r['qps']:>10,.0f} {r['late_rate'] * 100:>7.2f} {r['final_w']:>10.1f}")


if __name__ == "__main__":
    main()
