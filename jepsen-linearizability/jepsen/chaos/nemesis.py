"""Base nemesis interface and composite nemesis support."""

from __future__ import annotations

import abc
import random
import threading
import time
from typing import List


class Nemesis(abc.ABC):
    """A nemesis injects faults into the cluster during a test run."""

    @abc.abstractmethod
    def start(self) -> None:
        """Begin fault injection."""

    @abc.abstractmethod
    def stop(self) -> None:
        """Stop fault injection and heal all faults."""

    @abc.abstractmethod
    def describe(self) -> str:
        """Human-readable description of the fault type."""


class CompositeNemesis(Nemesis):
    """Runs multiple nemeses concurrently."""

    def __init__(self, nemeses: List[Nemesis]) -> None:
        self._nemeses = nemeses

    def start(self) -> None:
        for n in self._nemeses:
            n.start()

    def stop(self) -> None:
        for n in self._nemeses:
            n.stop()

    def describe(self) -> str:
        return " + ".join(n.describe() for n in self._nemeses)


class PeriodicNemesis(Nemesis):
    """Wraps a nemesis to run on a periodic schedule."""

    def __init__(
        self,
        nemesis: Nemesis,
        fault_duration: float = 2.0,
        heal_duration: float = 3.0,
    ) -> None:
        self._nemesis = nemesis
        self._fault_duration = fault_duration
        self._heal_duration = heal_duration
        self._thread: threading.Thread | None = None
        self._running = False

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        self._nemesis.stop()
        if self._thread:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        while self._running:
            jitter = random.uniform(0, self._heal_duration * 0.5)
            time.sleep(self._heal_duration + jitter)
            if not self._running:
                break
            self._nemesis.start()
            time.sleep(self._fault_duration + random.uniform(0, self._fault_duration * 0.5))
            self._nemesis.stop()

    def describe(self) -> str:
        return f"Periodic({self._nemesis.describe()}, every ~{self._heal_duration}s for ~{self._fault_duration}s)"
