"""Markov-chain access predictor.

Trains on observed (prev_page → next_page) transitions; given the current
page, predicts top-K next pages. Bounded memory via LFU eviction of
transition counts.
"""
from __future__ import annotations

from collections import defaultdict, Counter
from dataclasses import dataclass, field


@dataclass
class MarkovPrefetcher:
    order: int = 1                  # 1st-order chain
    max_predecessors: int = 1024
    _transitions: dict = field(default_factory=lambda: defaultdict(Counter))
    _prev: int | None = None

    def observe(self, page_id: int) -> None:
        if self._prev is not None:
            self._transitions[self._prev][page_id] += 1
        self._prev = page_id
        # Memory cap
        if len(self._transitions) > self.max_predecessors:
            # Drop the least-used predecessor
            victim = min(self._transitions,
                         key=lambda k: sum(self._transitions[k].values()))
            self._transitions.pop(victim, None)

    def predict(self, page_id: int | None, k: int = 1) -> list[int]:
        if page_id is None or page_id not in self._transitions:
            return []
        return [p for p, _ in self._transitions[page_id].most_common(k)]

    def accuracy_estimate(self) -> float:
        """Top-1 accuracy on the training data (in-sample)."""
        correct = total = 0
        for prev, counter in self._transitions.items():
            if not counter:
                continue
            top = counter.most_common(1)[0][0]
            for nxt, cnt in counter.items():
                total += cnt
                if nxt == top:
                    correct += cnt
        return correct / max(total, 1)


__all__ = ["MarkovPrefetcher"]
