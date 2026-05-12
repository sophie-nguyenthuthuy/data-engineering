"""
Output divergence comparator.

Compares v1 and v2 outputs at multiple granularities and returns a normalised
divergence score in [0.0, 1.0] where 0.0 means identical and 1.0 means
completely different.
"""

import math
from typing import Any, Dict, List, Optional, Set, Tuple


# ---------------------------------------------------------------------------
# Low-level field comparisons
# ---------------------------------------------------------------------------

def _numeric_close(a: float, b: float, rel_tol: float = 1e-6, abs_tol: float = 1e-9) -> bool:
    return math.isclose(a, b, rel_tol=rel_tol, abs_tol=abs_tol)


def _field_divergence(v1_val: Any, v2_val: Any, numeric_tolerance: float = 1e-6) -> float:
    """
    Return a divergence score for a single field pair.

    Rules
    -----
    - Both None / missing  → 0.0
    - One None             → 1.0
    - Both numeric         → 0.0 if within tolerance, else 1.0
    - Both list/tuple      → element-wise mean divergence (length mismatch = 1.0)
    - Both dict            → recursive dict divergence
    - Otherwise            → 0.0 if equal, 1.0 if not
    """
    if v1_val is None and v2_val is None:
        return 0.0
    if v1_val is None or v2_val is None:
        return 1.0

    if isinstance(v1_val, bool) and isinstance(v2_val, bool):
        return 0.0 if v1_val == v2_val else 1.0

    if isinstance(v1_val, (int, float)) and isinstance(v2_val, (int, float)):
        return 0.0 if _numeric_close(float(v1_val), float(v2_val), rel_tol=numeric_tolerance) else 1.0

    if isinstance(v1_val, (list, tuple)) and isinstance(v2_val, (list, tuple)):
        if len(v1_val) != len(v2_val):
            return 1.0
        if len(v1_val) == 0:
            return 0.0
        scores = [_field_divergence(a, b, numeric_tolerance) for a, b in zip(v1_val, v2_val)]
        return sum(scores) / len(scores)

    if isinstance(v1_val, dict) and isinstance(v2_val, dict):
        return dict_divergence(v1_val, v2_val, numeric_tolerance=numeric_tolerance)

    return 0.0 if v1_val == v2_val else 1.0


# ---------------------------------------------------------------------------
# Dict-level divergence
# ---------------------------------------------------------------------------

def dict_divergence(
    v1_output: Dict[str, Any],
    v2_output: Dict[str, Any],
    ignore_keys: Optional[Set[str]] = None,
    numeric_tolerance: float = 1e-6,
) -> float:
    """
    Compute a normalised divergence score between two output dicts.

    The score is the mean per-field divergence across the union of keys.
    Keys present in only one output count as fully divergent (1.0).

    Args:
        v1_output:         Output produced by pipeline v1.
        v2_output:         Output produced by pipeline v2.
        ignore_keys:       Field names to exclude from comparison.
        numeric_tolerance: Relative tolerance for floating-point fields.

    Returns:
        Float in [0.0, 1.0].
    """
    ignore_keys = ignore_keys or set()
    all_keys = (set(v1_output) | set(v2_output)) - ignore_keys

    if not all_keys:
        return 0.0

    total = 0.0
    for key in all_keys:
        v1_val = v1_output.get(key)
        v2_val = v2_output.get(key)
        total += _field_divergence(v1_val, v2_val, numeric_tolerance)

    return total / len(all_keys)


# ---------------------------------------------------------------------------
# Rolling divergence tracker
# ---------------------------------------------------------------------------

class DivergenceTracker:
    """
    Maintains a rolling window of per-record divergence scores and exposes
    aggregate statistics used by the orchestrator.

    Args:
        window_size:       Maximum number of recent scores to retain.
        ignore_keys:       Field names excluded from every comparison.
        numeric_tolerance: Passed through to ``dict_divergence``.
    """

    def __init__(
        self,
        window_size: int = 1000,
        ignore_keys: Optional[Set[str]] = None,
        numeric_tolerance: float = 1e-6,
    ) -> None:
        self.window_size = window_size
        self.ignore_keys = ignore_keys or set()
        self.numeric_tolerance = numeric_tolerance

        self._scores: List[float] = []
        self._total_compared: int = 0
        self._total_divergent: int = 0  # score > 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record(
        self,
        v1_output: Dict[str, Any],
        v2_output: Dict[str, Any],
    ) -> float:
        """
        Compare one pair of outputs, update the rolling window, and return
        the per-record divergence score.
        """
        score = dict_divergence(
            v1_output,
            v2_output,
            ignore_keys=self.ignore_keys,
            numeric_tolerance=self.numeric_tolerance,
        )
        self._scores.append(score)
        if len(self._scores) > self.window_size:
            self._scores.pop(0)

        self._total_compared += 1
        if score > 0.0:
            self._total_divergent += 1

        return score

    @property
    def sample_count(self) -> int:
        """Number of records compared so far (unbounded)."""
        return self._total_compared

    @property
    def window_divergence_rate(self) -> float:
        """
        Fraction of records in the rolling window that had *any* divergence.
        Returns 0.0 when no records have been seen.
        """
        if not self._scores:
            return 0.0
        return sum(1 for s in self._scores if s > 0.0) / len(self._scores)

    @property
    def mean_divergence_score(self) -> float:
        """
        Mean of the raw divergence scores in the rolling window.
        Captures magnitude as well as frequency.
        """
        if not self._scores:
            return 0.0
        return sum(self._scores) / len(self._scores)

    def summary(self) -> Dict[str, Any]:
        """Return a dict snapshot of current tracker state."""
        return {
            "total_compared": self._total_compared,
            "total_divergent": self._total_divergent,
            "window_size": len(self._scores),
            "window_divergence_rate": round(self.window_divergence_rate, 6),
            "mean_divergence_score": round(self.mean_divergence_score, 6),
        }

    def reset(self) -> None:
        """Clear all recorded scores (e.g. after a traffic-shift step)."""
        self._scores.clear()
        self._total_compared = 0
        self._total_divergent = 0
