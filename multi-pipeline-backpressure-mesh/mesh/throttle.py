"""Token-bucket rate limiter applied externally at a job's source reader."""
from __future__ import annotations

import asyncio
import logging
import time

logger = logging.getLogger(__name__)


class TokenBucketThrottle:
    """
    Token-bucket throttle that can be injected at the source-read boundary of a
    streaming job without touching the job's internal processing logic.

    Usage inside a source adapter:

        throttle = TokenBucketThrottle(rate=1000.0)  # records/sec baseline
        ...
        async def read_record():
            await throttle.acquire()          # blocks when rate exceeded
            return source.next()
    """

    def __init__(self, rate: float, burst_multiplier: float = 2.0) -> None:
        self._baseline_rate = rate
        self._rate = rate
        self._burst = rate * burst_multiplier
        self._tokens: float = self._burst
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()
        self._factor: float = 1.0  # [0, 1] — set by coordinator commands

    @property
    def effective_rate(self) -> float:
        return self._rate  # _rate is already baseline * factor

    def set_throttle_factor(self, factor: float) -> None:
        """Apply a throttle factor in [0, 1]. 1.0 = full rate, 0.0 = paused."""
        factor = max(0.0, min(1.0, factor))
        old = self._factor
        self._factor = factor
        new_rate = self._baseline_rate * factor
        if new_rate != self._rate:
            logger.info(
                "Throttle adjusted %.0f → %.0f recs/s (factor %.2f)",
                self._rate,
                new_rate,
                factor,
            )
            self._rate = new_rate
            self._burst = new_rate * 2.0 if new_rate > 0 else 0.0

    async def acquire(self, n: int = 1) -> None:
        """Block until n tokens are available."""
        if self._factor == 0.0:
            await asyncio.sleep(0.1)
            return

        async with self._lock:
            self._refill()
            while self._tokens < n:
                deficit = n - self._tokens
                wait = deficit / max(self._rate, 1e-9)
                await asyncio.sleep(wait)
                self._refill()
            self._tokens -= n

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
        self._last_refill = now

    def reset(self) -> None:
        self._factor = 1.0
        self._rate = self._baseline_rate
        self._burst = self._baseline_rate * 2.0
        self._tokens = self._burst
        self._last_refill = time.monotonic()
