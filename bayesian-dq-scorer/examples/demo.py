"""
demo.py — End-to-end demonstration of BayesianDQScorer.

Simulates 20 batches of data with:
  - Gradual completeness degradation (batch 10-14)
  - Sudden freshness outage (batch 16-17)
  - Persistent uniqueness issues (batch 12+)

Run:
    python examples/demo.py
"""

import random
from datetime import datetime, timedelta, timezone

import numpy as np

from bayesian_dq import BayesianDQScorer
from bayesian_dq.models import BatchObservation, DQDimension
from bayesian_dq.visualization import DQVisualizer

random.seed(42)
np.random.seed(42)


def make_obs(dim: DQDimension, successes: int, total: int, batch_id: str) -> BatchObservation:
    return BatchObservation(
        dimension=dim,
        successes=successes,
        total=total,
        batch_id=batch_id,
        timestamp=datetime.now(timezone.utc),
    )


def simulate_batch(batch_num: int, total_rows: int = 1000) -> dict[DQDimension, BatchObservation]:
    bid = f"batch_{batch_num:03d}"

    # Completeness: good baseline, degrades batch 10-14, recovers
    if 10 <= batch_num <= 14:
        completeness_rate = np.random.beta(3, 20)  # ~0.13 — bad
    else:
        completeness_rate = np.random.beta(95, 5)  # ~0.95 — good

    # Freshness: good baseline, outage batch 16-17
    if batch_num in (16, 17):
        freshness_rate = np.random.beta(1, 50)   # ~0.02 — outage
    else:
        freshness_rate = np.random.beta(97, 3)   # ~0.97 — good

    # Uniqueness: good early, degrades from batch 12 onwards (key collision bug)
    if batch_num >= 12:
        uniqueness_rate = np.random.beta(80, 20)  # ~0.80 — duplicate issue
    else:
        uniqueness_rate = np.random.beta(99, 1)   # ~0.99 — good

    return {
        DQDimension.COMPLETENESS: make_obs(
            DQDimension.COMPLETENESS,
            int(completeness_rate * total_rows),
            total_rows,
            bid,
        ),
        DQDimension.FRESHNESS: make_obs(
            DQDimension.FRESHNESS,
            int(freshness_rate * total_rows),
            total_rows,
            bid,
        ),
        DQDimension.UNIQUENESS: make_obs(
            DQDimension.UNIQUENESS,
            int(uniqueness_rate * total_rows),
            total_rows,
            bid,
        ),
    }


def main():
    scorer = BayesianDQScorer(
        health_thresholds={
            DQDimension.COMPLETENESS: 0.90,
            DQDimension.FRESHNESS:    0.90,
            DQDimension.UNIQUENESS:   0.95,
        },
        alert_thresholds={
            DQDimension.COMPLETENESS: 0.20,
            DQDimension.FRESHNESS:    0.20,
            DQDimension.UNIQUENESS:   0.20,
        },
        alert_cooldown=2,
    )

    print("=" * 65)
    print("  Bayesian Data Quality Scorer — Demo")
    print("=" * 65)

    for i in range(1, 21):
        obs = simulate_batch(i)
        result = scorer.score_batch(f"batch_{i:03d}", obs)
        p = result.p_healthy

        flag = "  [ALERT]" if result.alerts_fired else ""
        print(
            f"Batch {i:>3} | "
            f"Completeness P(healthy)={p.get(DQDimension.COMPLETENESS, 0):.3f}  "
            f"Freshness P(healthy)={p.get(DQDimension.FRESHNESS, 0):.3f}  "
            f"Uniqueness P(healthy)={p.get(DQDimension.UNIQUENESS, 0):.3f}"
            f"{flag}"
        )
        for alert in result.alerts_fired:
            print(f"        >> {alert.message}")

    print()
    print("Credible Intervals (95%) after all batches:")
    for dim, (lo, hi) in scorer.credible_intervals(0.95).items():
        post = scorer.current_posteriors[dim]
        print(f"  {dim.value:<14} [{lo:.3f}, {hi:.3f}]  mean={post.mean:.3f}  std={post.std:.4f}")

    print()
    print(f"Total alerts fired: {len(scorer.alert_manager.history)}")

    # Visualization
    viz = DQVisualizer(scorer.scorers)
    print("\nRendering dashboard (close window to exit)…")
    try:
        viz.dashboard(scorer.history, alert_threshold=0.20)
    except Exception as exc:
        print(f"  (Visualization skipped in headless env: {exc})")


if __name__ == "__main__":
    main()
