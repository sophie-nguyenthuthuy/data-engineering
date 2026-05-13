"""Empirical DP validation via membership-inference attacks.

We pretend to be an adversary who:
  1. Generates two adjacent datasets D and D' (differing in one record)
  2. Runs the mechanism on each, many times
  3. Tries to distinguish "this output came from D" from "this output
     came from D'"

If the mechanism is ε-DP, the adversary's advantage is bounded by
(e^ε − 1) / (e^ε + 1). We measure the empirical advantage and verify it
stays below that bound.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from sdp.local.randomizers import LocalConfig, randomized_response


@dataclass(frozen=True, slots=True)
class MembershipInferenceResult:
    epsilon: float
    n_trials: int
    advantage: float  # |P(D | obs) − P(D' | obs)|
    theoretical_bound: float


def empirical_advantage_rr(
    eps0: float,
    domain_size: int,
    n_trials: int = 50_000,
    seed: int = 0,
) -> MembershipInferenceResult:
    """Run RR many times on values 0 and 1; measure the distinguishing advantage."""
    rng = np.random.default_rng(seed)
    cfg = LocalConfig(eps0=eps0, domain_size=domain_size)
    # P(rr(0) = 0) and P(rr(1) = 0) should differ by no more than e^ε factor
    n0_out_0 = 0
    n1_out_0 = 0
    for _ in range(n_trials):
        if randomized_response(0, cfg, rng) == 0:
            n0_out_0 += 1
        if randomized_response(1, cfg, rng) == 0:
            n1_out_0 += 1
    p0 = n0_out_0 / n_trials
    p1 = n1_out_0 / n_trials
    advantage = abs(p0 - p1)
    # Theoretical: total-variation distance ≤ (e^ε − 1) / (e^ε + 1)
    import math

    theoretical = (math.exp(eps0) - 1) / (math.exp(eps0) + 1)
    return MembershipInferenceResult(
        epsilon=eps0,
        n_trials=n_trials,
        advantage=advantage,
        theoretical_bound=theoretical,
    )


__all__ = ["MembershipInferenceResult", "empirical_advantage_rr"]
