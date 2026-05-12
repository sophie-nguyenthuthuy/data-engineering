"""
PID controller that drives adaptive window-size decisions.

The controlled variable is window_size (seconds).
The error signal is a composite of latency overshoot and backpressure.
"""

import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PIDConfig:
    kp: float = 0.4          # proportional gain
    ki: float = 0.05         # integral gain
    kd: float = 0.15         # derivative gain
    min_output: float = 0.05  # 50 ms floor
    max_output: float = 5.0   # 5 s ceiling
    integral_clamp: float = 2.0  # anti-windup clamp


class PIDController:
    """
    Standard PID controller with anti-windup and output clamping.

    Error convention: positive error → window too large (latency high or
    backpressure present), so the correction shrinks the window.
    """

    def __init__(self, config: Optional[PIDConfig] = None) -> None:
        self.cfg = config or PIDConfig()
        self._integral: float = 0.0
        self._prev_error: float = 0.0
        self._prev_time: float = time.monotonic()
        self.last_output: float = 0.5  # start at 500 ms

    def reset(self) -> None:
        self._integral = 0.0
        self._prev_error = 0.0
        self._prev_time = time.monotonic()

    def update(self, error: float) -> float:
        """
        Compute new window-size adjustment.

        Args:
            error: Normalised error in [-1, 1].
                   Positive → shrink window, negative → grow window.

        Returns:
            Signed delta to apply to the current window size (seconds).
        """
        now = time.monotonic()
        dt = max(now - self._prev_time, 1e-6)

        self._integral += error * dt
        # Anti-windup
        self._integral = max(
            -self.cfg.integral_clamp,
            min(self.cfg.integral_clamp, self._integral),
        )

        derivative = (error - self._prev_error) / dt

        raw = (
            self.cfg.kp * error
            + self.cfg.ki * self._integral
            + self.cfg.kd * derivative
        )

        # Negate: positive error → negative delta (shrink)
        delta = -raw

        self._prev_error = error
        self._prev_time = now
        return delta

    def apply(self, current_window: float, error: float) -> float:
        """
        Return the new clamped window size.

        Args:
            current_window: Current window size in seconds.
            error: Normalised error signal.

        Returns:
            New window size in seconds, clamped to [min_output, max_output].
        """
        delta = self.update(error)
        new_window = current_window + delta
        clamped = max(self.cfg.min_output, min(self.cfg.max_output, new_window))
        self.last_output = clamped
        return clamped
