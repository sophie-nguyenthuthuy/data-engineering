"""Workload-drift detection.

Compares a streaming reference profile to a sliding "recent" profile and
reports a drift score in [0, 1] based on the **total-variation distance**
between the column-frequency vectors:

    TV(p, q) = ½ Σ_c |p(c) − q(c)|

TV is in [0, 1], symmetric, and bounded above by any f-divergence we
might prefer (KL, JS). Cheap, no numpy required.

A drift event is raised when TV exceeds a configurable threshold.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from llo.workload.profile import WorkloadProfile


@dataclass
class DriftDetector:
    """Stateful drift detector over per-column access frequencies."""

    threshold: float = 0.15
    _baseline: dict[str, float] | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if not 0.0 < self.threshold <= 1.0:
            raise ValueError("threshold must be in (0, 1]")

    def calibrate(self, profile: WorkloadProfile) -> None:
        """Snapshot the current profile as the drift baseline."""
        self._baseline = _freq_vector(profile)

    def score(self, profile: WorkloadProfile) -> float:
        """Total-variation distance between the baseline and the current profile."""
        if self._baseline is None:
            return 0.0
        current = _freq_vector(profile)
        keys = set(self._baseline) | set(current)
        return 0.5 * sum(abs(self._baseline.get(k, 0.0) - current.get(k, 0.0)) for k in keys)

    def has_drifted(self, profile: WorkloadProfile) -> bool:
        return self.score(profile) >= self.threshold


def _freq_vector(profile: WorkloadProfile) -> dict[str, float]:
    """Normalised per-column frequency vector summing to 1."""
    raw = {c: profile.freq(c) for c in profile.columns}
    total = sum(raw.values())
    if total <= 0:
        # Uniform when no observations yet.
        return dict.fromkeys(profile.columns, 1.0 / len(profile.columns))
    return {c: v / total for c, v in raw.items()}


__all__ = ["DriftDetector"]
