"""Empirical lateness calibration: how close is the observed late-rate to δ?

For a well-calibrated watermark, after warm-up the actual late rate should
be within ~2× the configured target δ.
"""

from __future__ import annotations

import random

from pwm.watermark.advancer import WatermarkAdvancer
from pwm.watermark.estimator import PerKeyDelayEstimator


def measure(delta: float, source: str, n_warmup: int, n_measure: int, distribution: str) -> dict:
    est = PerKeyDelayEstimator(delta=delta, source=source)  # type: ignore[arg-type]
    adv = WatermarkAdvancer(delay_estimator=est, lambda_min=0.0)
    rng = random.Random(0)

    if distribution == "exp":
        def sample():
            return rng.expovariate(1.0)
    elif distribution == "lognormal":
        def sample():
            return rng.lognormvariate(0.0, 0.5)
    elif distribution == "pareto":
        def sample():
            return max(0.0, rng.paretovariate(2.0) - 1.0)
    else:
        raise ValueError(distribution)

    for t in range(1, n_warmup + 1):
        adv.on_record("k", float(t), float(t) + sample())

    adv.stats.on_time = 0
    adv.stats.late = 0
    for t in range(n_warmup + 1, n_warmup + n_measure + 1):
        adv.on_record("k", float(t), float(t) + sample())

    return {
        "delta": delta, "source": source, "distribution": distribution,
        "n_measure": n_measure,
        "observed_late_rate": adv.stats.late_rate,
        "delta_vs_observed": adv.stats.late_rate / max(delta, 1e-9),
    }


def main() -> None:
    print(f"{'δ':>6} {'source':<10} {'dist':<10} {'observed':>10} {'observed/δ':>10}")
    for delta in (0.01, 0.05, 0.1):
        for src in ("tdigest", "lognormal"):
            for dist in ("exp", "lognormal", "pareto"):
                r = measure(delta, src, n_warmup=2000, n_measure=5000, distribution=dist)
                print(f"{r['delta']:>6} {r['source']:<10} {r['distribution']:<10} "
                      f"{r['observed_late_rate'] * 100:>9.2f}% {r['delta_vs_observed']:>10.2f}")


if __name__ == "__main__":
    main()
