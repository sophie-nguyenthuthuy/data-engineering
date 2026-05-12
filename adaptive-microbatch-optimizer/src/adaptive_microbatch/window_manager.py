"""
Adaptive window manager — integrates PID output, backpressure, and SLA
targets to decide the next micro-batch window size.
"""

import logging
import time
from dataclasses import dataclass
from typing import Optional

from .backpressure import BackpressureMonitor
from .metrics import MetricsCollector
from .pid_controller import PIDConfig, PIDController

logger = logging.getLogger(__name__)


@dataclass
class SLAConfig:
    target_latency_s: float = 0.2     # 200 ms p95 latency target
    min_throughput_eps: float = 100.0  # events/sec floor
    backpressure_weight: float = 0.5   # how much BP inflates the error signal


@dataclass
class WindowSnapshot:
    window_size_s: float
    pid_error: float
    backpressure_level: float
    p95_latency: Optional[float]
    throughput_eps: Optional[float]
    timestamp: float


class AdaptiveWindowManager:
    """
    Owns the current window size and updates it after each batch.

    Error signal composition:
        latency_error  = (p95 - target_latency) / target_latency   ∈ ℝ
        pressure_error = backpressure_level                         ∈ [0,1]
        combined_error = latency_error + sla.backpressure_weight * pressure_error
        (clamped to [-1, 1] before feeding the PID)
    """

    MIN_WINDOW = 0.05   # 50 ms
    MAX_WINDOW = 5.0    # 5 s

    def __init__(
        self,
        sla: Optional[SLAConfig] = None,
        pid_config: Optional[PIDConfig] = None,
        metrics: Optional[MetricsCollector] = None,
        backpressure: Optional[BackpressureMonitor] = None,
        initial_window: float = 0.5,
    ) -> None:
        self.sla = sla or SLAConfig()
        self.pid = PIDController(pid_config)
        self.metrics = metrics or MetricsCollector()
        self.backpressure = backpressure or BackpressureMonitor()

        self._window: float = max(self.MIN_WINDOW, min(self.MAX_WINDOW, initial_window))
        self._history: list[WindowSnapshot] = []

    @property
    def current_window(self) -> float:
        return self._window

    def after_batch(
        self,
        batch_size: int,
        processing_time_s: float,
    ) -> float:
        """
        Called after every batch completes.  Records metrics, computes the
        error signal, drives the PID, and returns the next window size.

        Args:
            batch_size:        Number of events in the completed batch.
            processing_time_s: Wall time (seconds) to process the batch.

        Returns:
            New window size in seconds.
        """
        self.metrics.record(batch_size, processing_time_s, self._window)

        lat = self.metrics.latency_snapshot()
        tput = self.metrics.throughput_snapshot()

        p95 = lat.p95 if lat else processing_time_s
        eps = tput.events_per_second if tput else 0.0
        bp = self.backpressure.current_level()

        latency_error = (p95 - self.sla.target_latency_s) / max(
            self.sla.target_latency_s, 1e-9
        )
        combined_error = latency_error + self.sla.backpressure_weight * bp
        combined_error = max(-1.0, min(1.0, combined_error))

        new_window = self.pid.apply(self._window, combined_error)
        self._window = new_window

        snap = WindowSnapshot(
            window_size_s=new_window,
            pid_error=combined_error,
            backpressure_level=bp,
            p95_latency=p95,
            throughput_eps=eps,
            timestamp=time.monotonic(),
        )
        self._history.append(snap)
        if len(self._history) > 1000:
            self._history = self._history[-1000:]

        logger.debug(
            "window=%.3fs  error=%.3f  bp=%.2f  p95=%.3fs  tput=%.1f eps",
            new_window,
            combined_error,
            bp,
            p95,
            eps,
        )
        return new_window

    def history(self) -> list[WindowSnapshot]:
        return list(self._history)

    def reset(self) -> None:
        self.pid.reset()
        self._window = 0.5
        self._history.clear()
