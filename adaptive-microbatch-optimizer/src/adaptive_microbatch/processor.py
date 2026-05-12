"""
MicroBatchProcessor — the main streaming entry point.

Usage
-----
    async def my_handler(batch: list[Event]) -> None:
        ...

    proc = MicroBatchProcessor(handler=my_handler, sla=SLAConfig(target_latency_s=0.1))
    await proc.start()
    await proc.ingest(event)   # thread/coroutine-safe
    await proc.stop()
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Generic, List, Optional, TypeVar

from .backpressure import BackpressureMonitor
from .metrics import MetricsCollector
from .window_manager import AdaptiveWindowManager, SLAConfig

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class ProcessorStats:
    total_events: int = 0
    total_batches: int = 0
    dropped_events: int = 0
    current_window_s: float = 0.5
    uptime_s: float = 0.0


class MicroBatchProcessor(Generic[T]):
    """
    Collects events into micro-batches whose window size is adaptively
    controlled by a PID controller reacting to latency, throughput, and
    downstream backpressure.

    Args:
        handler:         Async callable that processes a complete batch.
        sla:             Latency / throughput SLA configuration.
        max_queue_size:  Hard cap on the internal event queue.  Events
                         ingested beyond this limit are dropped and counted.
        initial_window:  Starting window size in seconds.
    """

    def __init__(
        self,
        handler: Callable[[List[T]], Awaitable[None]],
        sla: Optional[SLAConfig] = None,
        max_queue_size: int = 10_000,
        initial_window: float = 0.5,
    ) -> None:
        self._handler = handler
        self._queue: asyncio.Queue[T] = asyncio.Queue(maxsize=max_queue_size)
        self.backpressure = BackpressureMonitor()
        self.metrics = MetricsCollector()
        self.window_mgr = AdaptiveWindowManager(
            sla=sla,
            metrics=self.metrics,
            backpressure=self.backpressure,
            initial_window=initial_window,
        )

        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._start_time: float = 0.0
        self._stats = ProcessorStats()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the background batch-dispatch loop."""
        if self._running:
            return
        self._running = True
        self._start_time = time.monotonic()
        self._task = asyncio.create_task(self._dispatch_loop(), name="microbatch-dispatch")
        logger.info("MicroBatchProcessor started (initial window=%.3fs)", self.window_mgr.current_window)

    async def stop(self, drain: bool = True) -> None:
        """
        Stop the processor.

        Args:
            drain: If True, flush remaining queued events before stopping.
        """
        self._running = False
        if drain:
            await self._flush()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("MicroBatchProcessor stopped. %s", self.stats())

    async def ingest(self, event: T) -> bool:
        """
        Enqueue a single event for batching.

        Returns True if the event was accepted, False if the queue was full
        (the event is dropped and counted).
        """
        try:
            self._queue.put_nowait(event)
            return True
        except asyncio.QueueFull:
            self._stats.dropped_events += 1
            logger.warning("Queue full — event dropped (total dropped=%d)", self._stats.dropped_events)
            return False

    async def ingest_many(self, events: List[T]) -> int:
        """Enqueue multiple events.  Returns the number accepted."""
        accepted = 0
        for e in events:
            if await self.ingest(e):
                accepted += 1
        return accepted

    def report_backpressure(self, source: str, level: float) -> None:
        """
        Downstream worker calls this to signal load pressure.

        Args:
            source: Logical name of the downstream sink (e.g. "db-pool").
            level:  Load factor in [0.0, 1.0].
        """
        self.backpressure.push(source, level)

    def stats(self) -> ProcessorStats:
        self._stats.current_window_s = self.window_mgr.current_window
        self._stats.uptime_s = time.monotonic() - self._start_time
        return self._stats

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------

    async def _dispatch_loop(self) -> None:
        while self._running:
            window = self.window_mgr.current_window
            await asyncio.sleep(window)
            await self._flush()

    async def _flush(self) -> None:
        batch: List[T] = []
        while not self._queue.empty():
            try:
                batch.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        if not batch:
            return

        t0 = time.monotonic()
        try:
            await self._handler(batch)
        except Exception:
            logger.exception("Batch handler raised an exception (batch_size=%d)", len(batch))
        elapsed = time.monotonic() - t0

        self._stats.total_events += len(batch)
        self._stats.total_batches += 1

        self.window_mgr.after_batch(len(batch), elapsed)

        logger.debug(
            "Flushed batch size=%d in %.4fs  new_window=%.3fs",
            len(batch),
            elapsed,
            self.window_mgr.current_window,
        )
