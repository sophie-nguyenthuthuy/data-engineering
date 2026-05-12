"""Backpressure coordinator.

Listens to all backpressure signals on the bus, maintains a per-job pressure
registry, and emits throttle commands to upstream ancestors using an
exponential-decay propagation model.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from .bus import BackpressureBus
from .metrics import BackpressureLevel, BackpressureSignal, ThrottleCommand
from .topology import PipelineTopology

logger = logging.getLogger(__name__)

_DECAY_WINDOW_SECS = 10.0  # time before pressure score decays to zero
_HOP_ATTENUATION = 0.7     # throttle factor is multiplied by this per hop


@dataclass
class _PressureRecord:
    signal: BackpressureSignal
    expires_at: float = field(init=False)

    def __post_init__(self):
        self.expires_at = time.monotonic() + _DECAY_WINDOW_SECS

    @property
    def is_expired(self) -> bool:
        return time.monotonic() > self.expires_at


class BackpressureCoordinator:
    """
    Central coordinator that:
      1. Subscribes to all backpressure signals on the bus.
      2. Maintains a sliding-window pressure registry per job.
      3. Propagates throttle commands to upstream ancestors.
      4. Runs a periodic reconcile loop that clears stale pressure and
         issues release commands when pressure subsides.
    """

    def __init__(
        self,
        bus: BackpressureBus,
        topology: PipelineTopology,
        reconcile_interval: float = 2.0,
    ) -> None:
        self._bus = bus
        self._topo = topology
        self._reconcile_interval = reconcile_interval
        self._pressure: dict[str, _PressureRecord] = {}
        self._current_throttles: dict[str, float] = {}  # job_id → last factor sent
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        await self._bus.subscribe_signals(self._on_signal)
        self._task = asyncio.create_task(self._reconcile_loop(), name="bp-coordinator")
        logger.info("BackpressureCoordinator started")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("BackpressureCoordinator stopped")

    async def _on_signal(self, signal: BackpressureSignal) -> None:
        if signal.source_job_id not in self._topo:
            logger.warning("Signal from unknown job %s — ignored", signal.source_job_id)
            return

        async with self._lock:
            self._pressure[signal.source_job_id] = _PressureRecord(signal)

        logger.info(
            "Signal received: %s level=%s score=%.2f",
            signal.source_job_id,
            signal.level.name,
            signal.score,
        )
        await self._propagate(signal)

    async def _propagate(self, signal: BackpressureSignal) -> None:
        """Walk upstream ancestors and issue throttle commands."""
        ancestors = self._topo.upstream_ancestors(signal.source_job_id)
        if not ancestors:
            return

        commands = []
        for job_id, hop in ancestors.items():
            attenuation = _HOP_ATTENUATION ** hop
            throttle_factor = max(0.0, 1.0 - signal.score * attenuation)
            node = self._topo.get_node(job_id)
            throttle_factor = 1.0 - (1.0 - throttle_factor) * node.propagation_weight
            commands.append(
                ThrottleCommand(
                    target_job_id=job_id,
                    throttle_factor=round(throttle_factor, 3),
                    reason=f"backpressure from {signal.source_job_id} (hop={hop})",
                    originating_signal=signal,
                )
            )

        async with self._lock:
            for cmd in commands:
                self._current_throttles[cmd.target_job_id] = cmd.throttle_factor

        await asyncio.gather(*(self._bus.publish_throttle(c) for c in commands))

    async def _reconcile_loop(self) -> None:
        while True:
            await asyncio.sleep(self._reconcile_interval)
            await self._reconcile()

    async def _reconcile(self) -> None:
        """Expire stale pressure records and release throttles no longer needed."""
        async with self._lock:
            stale = [jid for jid, rec in self._pressure.items() if rec.is_expired]
            for jid in stale:
                del self._pressure[jid]
                logger.info("Pressure expired for %s — releasing throttles", jid)

            if not stale:
                return

            # Recompute required throttle per ancestor across all active pressures
            required: dict[str, float] = {}  # job_id → minimum factor (most restrictive)
            for sig_job_id, rec in self._pressure.items():
                for job_id, hop in self._topo.upstream_ancestors(sig_job_id).items():
                    attenuation = _HOP_ATTENUATION ** hop
                    factor = max(0.0, 1.0 - rec.signal.score * attenuation)
                    required[job_id] = min(required.get(job_id, 1.0), factor)

        release_cmds = []
        async with self._lock:
            for job_id, current_factor in list(self._current_throttles.items()):
                desired = required.get(job_id, 1.0)
                if abs(desired - current_factor) > 0.01:
                    self._current_throttles[job_id] = desired
                    release_cmds.append(
                        ThrottleCommand(
                            target_job_id=job_id,
                            throttle_factor=desired,
                            reason="reconcile: pressure subsided",
                        )
                    )
            # clean up entries where we restored full rate
            for cmd in release_cmds:
                if cmd.throttle_factor >= 1.0:
                    self._current_throttles.pop(cmd.target_job_id, None)

        if release_cmds:
            await asyncio.gather(*(self._bus.publish_throttle(c) for c in release_cmds))
            for cmd in release_cmds:
                logger.info(
                    "Throttle updated %s → factor=%.2f (%s)",
                    cmd.target_job_id,
                    cmd.throttle_factor,
                    cmd.reason,
                )

    @property
    def active_pressure(self) -> dict[str, float]:
        return {jid: rec.signal.score for jid, rec in self._pressure.items() if not rec.is_expired}

    @property
    def active_throttles(self) -> dict[str, float]:
        return dict(self._current_throttles)
