"""Markov-chain page-access predictor.

Joseph & Grunwald (ISCA 1997) introduced Markov prefetchers for hardware
caches. We use the same model at the page level: P(next=B | last=A) is
learned online. Top-K candidates with `predict(k=K)`.

Includes:
  - order-1 chain (one predecessor)
  - bounded memory via LFU eviction of low-traffic predecessors
  - phase detection: if recent observations diverge from the model's
    in-sample distribution, reset (CUSUM-style detector)
"""

from __future__ import annotations

import threading
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from disagg.core.page import PageId


@dataclass
class PrefetchStats:
    observations: int = 0
    predictions_made: int = 0
    correct_predictions: int = 0
    resets: int = 0

    @property
    def accuracy(self) -> float:
        return self.correct_predictions / max(self.predictions_made, 1)


class MarkovPrefetcher:
    """Order-1 chain with bounded memory + phase detection."""

    def __init__(
        self,
        max_predecessors: int = 1024,
        phase_window: int = 200,
        phase_threshold: float = 0.7,
    ) -> None:
        # transitions[prev][next] = count
        self._transitions: dict[PageId, Counter[PageId]] = defaultdict(Counter)
        self._prev: PageId | None = None
        self.max_predecessors = max_predecessors
        self.phase_threshold = phase_threshold
        self.stats = PrefetchStats()
        self._recent: deque[tuple[PageId, PageId | None]] = deque(maxlen=phase_window)
        self._lock = threading.Lock()

    # ---- Training ---------------------------------------------------------

    def observe(self, page_id: PageId) -> None:
        with self._lock:
            self.stats.observations += 1
            if self._prev is not None:
                # Record (prev, page_id) transition + accuracy bookkeeping
                self._recent.append((self._prev, page_id))
                self._transitions[self._prev][page_id] += 1
                # Bounded memory: drop least-used predecessor
                if len(self._transitions) > self.max_predecessors:
                    victim = min(
                        self._transitions, key=lambda k: sum(self._transitions[k].values())
                    )
                    if victim != self._prev:
                        self._transitions.pop(victim, None)
            self._prev = page_id
            self._maybe_reset_phase()

    # ---- Prediction -------------------------------------------------------

    def predict(self, page_id: PageId | None = None, k: int = 1) -> list[PageId]:
        with self._lock:
            prev = page_id if page_id is not None else self._prev
            if prev is None:
                return []
            counter = self._transitions.get(prev)
            if not counter:
                return []
            return [p for p, _ in counter.most_common(k)]

    def record_prediction_outcome(self, predicted: PageId, actual: PageId) -> None:
        with self._lock:
            self.stats.predictions_made += 1
            if predicted == actual:
                self.stats.correct_predictions += 1

    # ---- Phase detection --------------------------------------------------

    def _maybe_reset_phase(self) -> None:
        """If recent observations diverge from learned distribution, reset.

        Simple heuristic: over the recent window, the fraction of observations
        whose `actual` matches the model's top-1 prediction. Below `threshold`
        means the workload has shifted; reset.
        """
        if len(self._recent) < self._recent.maxlen:  # type: ignore[operator]
            return
        n_correct = 0
        for prev, actual in self._recent:
            top = self._transitions[prev].most_common(1)
            if top and top[0][0] == actual:
                n_correct += 1
        observed_rate = n_correct / len(self._recent)
        if observed_rate < (1.0 - self.phase_threshold):
            # Workload phase shifted — clear chain to relearn
            self._transitions.clear()
            self._recent.clear()
            self.stats.resets += 1

    # ---- Introspection ----------------------------------------------------

    @property
    def n_predecessors(self) -> int:
        with self._lock:
            return len(self._transitions)

    def estimate_in_sample_top1_accuracy(self) -> float:
        """Top-1 accuracy on the data seen so far. Useful for tuning."""
        with self._lock:
            correct = total = 0
            for _prev, counter in self._transitions.items():
                if not counter:
                    continue
                top_count = counter.most_common(1)[0][1]
                total += sum(counter.values())
                correct += top_count
            return correct / max(total, 1)
