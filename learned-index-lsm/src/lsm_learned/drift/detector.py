"""
Distribution-drift detection for learned index structures.

Two complementary detectors are provided:

ADWIN (Adaptive Windowing, Bifet & Gavalda, 2007)
    Maintains a variable-length window of observed values and triggers drift
    whenever the difference in means between two adjacent sub-windows exceeds a
    statistically significant threshold.  O(log n) amortized per element.

KSWindowDetector
    Sliding two-sample Kolmogorov-Smirnov test over a reference window and a
    recent window.  More sensitive to shape changes (not just mean shifts).

Both detectors emit a ``DriftSignal`` when drift is detected, after which the
caller should retrain the learned index or fall back to a classic structure.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
from scipy import stats as scipy_stats


@dataclass
class DriftSignal:
    """Emitted when a detector identifies a distribution change."""

    detector: str
    step: int
    statistic: float          # detector-specific test statistic
    threshold: float
    message: str = ""


class ADWINDetector:
    """
    ADWIN change-detection algorithm.

    Observes a stream of scalar values (e.g., RMI lookup errors).  Internally
    maintains a compressed sliding window of *buckets* where each bucket stores
    a count and a sum.  When the estimated means of the oldest and newest halves
    of the window differ by more than ``delta``-derived epsilon, drift is flagged
    and the stale portion of the window is discarded.

    Parameters
    ----------
    delta:
        Confidence parameter (lower = more sensitive).  Typical: 0.002.
    min_window:
        Minimum number of samples before drift can be reported.
    """

    def __init__(self, delta: float = 0.002, min_window: int = 30) -> None:
        self._delta = delta
        self._min_window = min_window
        self._buckets: deque[tuple[int, float]] = deque()  # (count, sum)
        self._total_count = 0
        self._total_sum = 0.0
        self._step = 0

    def add(self, value: float) -> Optional[DriftSignal]:
        """
        Add one observation.  Returns ``DriftSignal`` if drift is detected,
        otherwise ``None``.
        """
        self._step += 1
        self._total_count += 1
        self._total_sum += value
        self._buckets.append((1, value))
        self._compress()

        if self._total_count < self._min_window:
            return None

        return self._detect()

    # ------------------------------------------------------------------
    # ADWIN internals
    # ------------------------------------------------------------------

    def _compress(self) -> None:
        """Merge adjacent equal-sized buckets to bound memory to O(log n)."""
        # Buckets are stored oldest-first.  We scan and merge pairs of equal
        # count from the newest end.
        counts: dict[int, int] = {}
        new_buckets: deque[tuple[int, float]] = deque()
        for cnt, s in reversed(self._buckets):
            counts[cnt] = counts.get(cnt, 0) + 1
            new_buckets.appendleft((cnt, s))
            if counts[cnt] >= 2:
                # Find and merge the two oldest buckets with this count
                first = second = None
                tmp = deque()
                for item in new_buckets:
                    if item[0] == cnt and first is None:
                        first = item
                    elif item[0] == cnt and second is None:
                        second = item
                    else:
                        tmp.append(item)
                if first and second:
                    tmp.appendleft((cnt * 2, first[1] + second[1]))
                    new_buckets = tmp
                    break
        self._buckets = new_buckets

    def _detect(self) -> Optional[DriftSignal]:
        """
        Slide a cut-point through the window; return DriftSignal if any cut
        shows a statistically significant mean difference.
        """
        n = self._total_count
        mean = self._total_sum / n

        # Accumulate from oldest end
        n0 = 0
        s0 = 0.0
        for cnt, s in self._buckets:
            n0 += cnt
            s0 += s
            n1 = n - n0
            if n1 < self._min_window // 2:
                break
            m0 = s0 / n0
            m1 = (self._total_sum - s0) / n1
            epsilon = math.sqrt(
                (1.0 / (2 * n0) + 1.0 / (2 * n1))
                * math.log(4 * n / self._delta)
            )
            diff = abs(m0 - m1)
            if diff > epsilon:
                # Discard the older half — it represents the stale distribution
                self._trim_to(n1)
                return DriftSignal(
                    detector="ADWIN",
                    step=self._step,
                    statistic=diff,
                    threshold=epsilon,
                    message=f"|Δmean|={diff:.4f} > ε={epsilon:.4f} at n={n}",
                )
        return None

    def _trim_to(self, keep_n: int) -> None:
        """Discard oldest elements until window has `keep_n` items."""
        while self._total_count > keep_n and self._buckets:
            cnt, s = self._buckets.popleft()
            self._total_count -= cnt
            self._total_sum -= s

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def window_size(self) -> int:
        return self._total_count

    @property
    def window_mean(self) -> float:
        if self._total_count == 0:
            return 0.0
        return self._total_sum / self._total_count


class KSWindowDetector:
    """
    Two-sample Kolmogorov-Smirnov test over sliding reference and recent windows.

    Maintains a *reference* window from stable operating conditions and a
    *recent* window of the latest observations.  When the KS p-value drops
    below ``alpha``, the distribution has shifted.

    Parameters
    ----------
    ref_size:
        Number of samples in the reference window.
    recent_size:
        Number of samples in the recent window.
    alpha:
        Significance level for the KS test (typical: 0.01).
    """

    def __init__(
        self, ref_size: int = 500, recent_size: int = 200, alpha: float = 0.01
    ) -> None:
        self._ref_size = ref_size
        self._recent_size = recent_size
        self._alpha = alpha
        self._reference: deque[float] = deque(maxlen=ref_size)
        self._recent: deque[float] = deque(maxlen=recent_size)
        self._step = 0
        self._ref_frozen = False  # reference locked once full

    def add(self, value: float) -> Optional[DriftSignal]:
        self._step += 1
        if not self._ref_frozen:
            self._reference.append(value)
            if len(self._reference) >= self._ref_size:
                self._ref_frozen = True
            return None

        self._recent.append(value)
        if len(self._recent) < self._recent_size:
            return None

        return self._test()

    def _test(self) -> Optional[DriftSignal]:
        ref = np.array(self._reference)
        rec = np.array(self._recent)
        result = scipy_stats.ks_2samp(ref, rec)
        if result.pvalue < self._alpha:
            return DriftSignal(
                detector="KS",
                step=self._step,
                statistic=float(result.statistic),
                threshold=self._alpha,
                message=(
                    f"KS statistic={result.statistic:.4f}, "
                    f"p-value={result.pvalue:.4e} < α={self._alpha}"
                ),
            )
        return None

    def reset_reference(self) -> None:
        """Call after retraining the index to start a new reference window."""
        self._reference.clear()
        self._recent.clear()
        self._ref_frozen = False
