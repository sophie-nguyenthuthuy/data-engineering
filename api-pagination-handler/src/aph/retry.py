"""Exponential-backoff retry policy.

The classic recipe — wait ``base · multiplier^k`` seconds after the
``k``-th failure, capped at ``cap``, optionally with full-random
jitter (Amazon Builders' Library) so retries from many clients do not
synchronise into thundering herds.

``RetryPolicy.run(callable_)`` invokes the callable and returns its
result, retrying transient failures up to ``max_attempts`` times.
Failures are detected via either an exception filter or a "this
:class:`Response` is retryable" predicate (default:
``Response.is_retryable()``).
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable

    from aph.transport import Response

T = TypeVar("T")


class RetryError(RuntimeError):
    """Raised when the retry policy exhausts its budget."""


@dataclass
class RetryPolicy:
    """Exponential backoff with optional jitter."""

    max_attempts: int = 5
    base: float = 0.1
    multiplier: float = 2.0
    cap: float = 30.0
    jitter: bool = True
    retry_on: tuple[type[BaseException], ...] = (Exception,)
    sleep: Callable[[float], None] = field(default=lambda _s: None)
    rng: random.Random | None = None

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be ≥ 1")
        if self.base <= 0:
            raise ValueError("base must be > 0")
        if self.multiplier < 1:
            raise ValueError("multiplier must be ≥ 1")
        if self.cap < self.base:
            raise ValueError("cap must be ≥ base")
        if self.rng is None:
            self.rng = random.Random(0)

    # ---------------------------------------------------------------- API

    def delay(self, attempt: int) -> float:
        """Seconds to wait before retrying after the ``attempt``-th failure (1-indexed)."""
        if attempt < 1:
            raise ValueError("attempt must be ≥ 1")
        exp = self.base * (self.multiplier ** (attempt - 1))
        capped = min(exp, self.cap)
        if not self.jitter:
            return capped
        # Full jitter: uniform(0, capped). Amazon Builders' Library recommends
        # this over half-jitter for high-concurrency clients.
        return self._rng.uniform(0.0, capped)

    def run(self, fn: Callable[[], T], is_failure: Callable[[T], bool] | None = None) -> T:
        """Invoke ``fn`` with retries on exceptions or failure predicate."""
        last_exc: BaseException | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                result = fn()
            except self.retry_on as exc:
                last_exc = exc
            else:
                if is_failure is None or not is_failure(result):
                    return result
                last_exc = None
            if attempt < self.max_attempts:
                self.sleep(self.delay(attempt))
        if last_exc is not None:
            raise RetryError(
                f"exhausted {self.max_attempts} attempts; last error: {last_exc!r}"
            ) from last_exc
        raise RetryError(f"exhausted {self.max_attempts} attempts (final result reported failure)")

    def run_response(self, fn: Callable[[], Response]) -> Response:
        """Convenience wrapper for HTTP calls using ``Response.is_retryable``."""

        def _failure(r: Response) -> bool:
            return r.is_retryable()

        return self.run(fn, is_failure=_failure)

    # ------------------------------------------------------------- guts

    @property
    def _rng(self) -> random.Random:
        assert self.rng is not None
        return self.rng


__all__ = ["RetryError", "RetryPolicy"]
