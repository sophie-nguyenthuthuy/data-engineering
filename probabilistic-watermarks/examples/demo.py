"""Demo: stream with heavy-tail delays; show watermark advance and corrections."""
from __future__ import annotations

import random

from src import PerKeyDelayEstimator, WatermarkAdvancer, CorrectionStream


def main():
    rng = random.Random(0)
    est = PerKeyDelayEstimator(delta=0.01)
    advancer = WatermarkAdvancer(delay_estimator=est, lambda_min=0.0)
    correction = CorrectionStream(window_size=10.0)

    on_time = late = 0
    advancer.set_late_handler(lambda k, et, at: globals().__setitem__("late_count", globals().get("late_count", 0) + 1))

    print("Simulating stream with lognormal delays...")
    for t in range(1, 5001):
        # 95% normal delay (lognormal small), 5% heavy tail (lognormal big)
        if rng.random() < 0.05:
            d = rng.lognormvariate(2.0, 1.0)
        else:
            d = rng.lognormvariate(0.0, 0.5)
        status, w = advancer.on_record("k", float(t), float(t) + d)
        if status == "on-time" or status == "ontime":
            on_time += 1
        elif status == "late":
            late += 1
        if t % 1000 == 0:
            sd = est.safe_delay("k")
            print(f"  t={t}: watermark={w:.2f}  safe_delay(k)={sd:.2f}  "
                  f"on-time={on_time}  late={late}")

    print(f"\nFinal: on-time={on_time}, late={late}, "
          f"late_rate={late/(on_time+late)*100:.2f}%")
    print(f"Target late rate (δ): {est.delta*100:.2f}%")
    print(f"Final watermark: {advancer.value:.2f}")


if __name__ == "__main__":
    main()
