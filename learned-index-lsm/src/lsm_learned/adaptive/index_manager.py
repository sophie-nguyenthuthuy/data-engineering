"""
Adaptive index manager: uses an RMI in normal operation and falls back to a
B-tree when the ADWIN drift detector signals a distribution change.

When in fallback mode the manager continues monitoring errors.  Once the error
rate stabilises below ``retrain_threshold`` it attempts to retrain the RMI on
the current key distribution and switches back.
"""

from __future__ import annotations

import time
from enum import Enum, auto
from typing import Optional

import numpy as np

from ..drift.detector import ADWINDetector, DriftSignal
from ..indexes.btree import BTreeIndex
from ..indexes.bloom import BloomFilter
from ..indexes.rmi import RMI


class IndexMode(Enum):
    LEARNED = auto()
    FALLBACK = auto()


class AdaptiveIndexManager:
    """
    Wraps an RMI and a BTreeIndex.  Routes lookups to one or the other based
    on drift-detector state.

    Parameters
    ----------
    num_stage2:
        Stage-2 model count for the RMI.
    adwin_delta:
        ADWIN confidence parameter (lower = more sensitive to drift).
    retrain_threshold:
        Mean absolute error below which retraining is attempted.
    fallback_window:
        Minimum number of fallback queries before attempting retrain.
    """

    def __init__(
        self,
        num_stage2: int = 100,
        adwin_delta: float = 0.002,
        retrain_threshold: float = 50.0,
        fallback_window: int = 200,
    ) -> None:
        self._rmi = RMI(num_stage2=num_stage2)
        self._btree = BTreeIndex()
        self._bloom: Optional[BloomFilter] = None
        self._detector = ADWINDetector(delta=adwin_delta)
        self._mode = IndexMode.FALLBACK  # starts in fallback until trained
        self._retrain_threshold = retrain_threshold
        self._fallback_window = fallback_window
        self._fallback_query_count = 0
        self._drift_events: list[DriftSignal] = []
        self._total_queries = 0
        self._total_rmi_queries = 0
        self._error_history: list[float] = []

    # ------------------------------------------------------------------
    # Build / retrain
    # ------------------------------------------------------------------

    def build(self, sorted_keys: np.ndarray) -> None:
        """Initial build — trains the RMI and populates the B-tree."""
        self._rmi.train(sorted_keys)
        self._btree.build(sorted_keys.tolist())
        n = len(sorted_keys)
        if n > 0:
            self._bloom = BloomFilter(n)
            for k in sorted_keys:
                self._bloom.add(int(k))
        self._mode = IndexMode.LEARNED
        self._detector = ADWINDetector(delta=self._detector._delta)

    def _retrain(self, sorted_keys: np.ndarray) -> None:
        self._rmi.train(sorted_keys)
        self._mode = IndexMode.LEARNED
        self._fallback_query_count = 0
        # Reset detector with fresh window
        self._detector = ADWINDetector(delta=self._detector._delta)

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def lookup(self, key: int, sorted_keys: Optional[np.ndarray] = None) -> Optional[int]:
        """
        Return the array index of *key* (or None if absent).

        If ``sorted_keys`` is provided and the RMI is active, the actual
        position is used to measure prediction error and feed the drift detector.
        """
        self._total_queries += 1

        # Bloom filter short-circuit (only if built)
        if self._bloom and key not in self._bloom:
            return None

        if self._mode == IndexMode.LEARNED:
            return self._learned_lookup(key, sorted_keys)
        else:
            self._fallback_query_count += 1
            result = self._btree.lookup(key)
            if sorted_keys is not None and self._fallback_query_count >= self._fallback_window:
                self._maybe_retrain(sorted_keys)
            return result

    def _learned_lookup(
        self, key: int, sorted_keys: Optional[np.ndarray]
    ) -> Optional[int]:
        self._total_rmi_queries += 1
        idx = self._rmi.lookup(float(key))

        if sorted_keys is not None and idx is not None:
            lo, hi = self._rmi.search_range(float(key))
            pred_mid = (lo + hi) / 2.0
            error = abs(pred_mid - idx)
            self._error_history.append(error)

            signal = self._detector.add(error)
            if signal is not None:
                self._drift_events.append(signal)
                self._mode = IndexMode.FALLBACK
                self._fallback_query_count = 0

        return idx

    def _maybe_retrain(self, sorted_keys: np.ndarray) -> None:
        if not self._error_history:
            return
        recent = self._error_history[-self._fallback_window :]
        if np.mean(recent) < self._retrain_threshold:
            self._retrain(sorted_keys)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def mode(self) -> IndexMode:
        return self._mode

    @property
    def drift_events(self) -> list[DriftSignal]:
        return list(self._drift_events)

    @property
    def rmi_usage_rate(self) -> float:
        if self._total_queries == 0:
            return 0.0
        return self._total_rmi_queries / self._total_queries

    def summary(self) -> dict:
        return {
            "mode": self._mode.name,
            "total_queries": self._total_queries,
            "rmi_queries": self._total_rmi_queries,
            "fallback_queries": self._total_queries - self._total_rmi_queries,
            "drift_events": len(self._drift_events),
            "rmi_usage_rate": round(self.rmi_usage_rate, 4),
            "mean_error": round(float(np.mean(self._error_history)) if self._error_history else 0, 2),
        }
