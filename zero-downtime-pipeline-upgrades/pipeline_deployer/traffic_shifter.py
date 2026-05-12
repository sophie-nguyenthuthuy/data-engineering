"""
Traffic shifter — gradually moves traffic from v1 to v2 in steps,
gated on the divergence tracker staying below the configured threshold.
"""

import logging
import threading
import time
from enum import Enum, auto
from typing import Callable, Dict, Any, Optional

from .comparator import DivergenceTracker
from .config import DeploymentConfig
from .shadow_runner import ShadowRunner

logger = logging.getLogger(__name__)


class ShiftState(Enum):
    IDLE = auto()          # Not yet started
    SHADOW_ONLY = auto()   # v2 running in shadow, 0 % live traffic
    SHIFTING = auto()      # Actively stepping traffic toward v2
    PROMOTED = auto()      # v2 at 100 %, v1 still in shadow for safety
    ROLLED_BACK = auto()   # Returned to v1 due to divergence spike
    PAUSED = auto()        # Operator paused automatic shifting
    COMPLETE = auto()      # v1 shadow torn down — upgrade fully complete


class TrafficShifter:
    """
    Drives the gradual traffic migration from v1 to v2.

    The shifter runs in its own background thread and ticks every
    ``config.traffic_shift_interval_sec`` seconds.  On each tick it:

    1. Checks the rolling divergence rate from the tracker.
    2. If below ``config.divergence_threshold`` and enough samples have
       been collected — advances the split by ``config.traffic_shift_step``.
    3. If above ``config.rollback_threshold`` — triggers a rollback.
    4. Emits a structured log line with current stats.

    External code (e.g. the orchestrator) can also call
    ``force_shift()`` / ``force_rollback()`` directly.

    Args:
        runner:         The active ShadowRunner to mutate.
        tracker:        Shared DivergenceTracker.
        config:         Deployment configuration.
        on_promoted:    Optional callback fired when v2 reaches 100 %.
        on_rolled_back: Optional callback fired on rollback.
    """

    def __init__(
        self,
        runner: ShadowRunner,
        tracker: DivergenceTracker,
        config: DeploymentConfig,
        on_promoted: Optional[Callable[[], None]] = None,
        on_rolled_back: Optional[Callable[[], None]] = None,
    ) -> None:
        self.runner = runner
        self.tracker = tracker
        self.config = config
        self.on_promoted = on_promoted
        self.on_rolled_back = on_rolled_back

        self._state = ShiftState.IDLE
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._state_lock = threading.Lock()
        self._shift_history: list[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    @property
    def state(self) -> ShiftState:
        return self._state

    def _set_state(self, new_state: ShiftState) -> None:
        with self._state_lock:
            old = self._state
            self._state = new_state
        logger.info("Shifter state: %s → %s", old.name, new_state.name)

    # ------------------------------------------------------------------
    # Public control API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background shifting thread."""
        if self._thread and self._thread.is_alive():
            logger.warning("TrafficShifter already running")
            return
        self._stop_event.clear()
        self._set_state(ShiftState.SHADOW_ONLY)
        self._thread = threading.Thread(target=self._loop, daemon=True, name="traffic-shifter")
        self._thread.start()
        logger.info(
            "TrafficShifter started — step=%.0f%% interval=%.0fs threshold=%.1f%%",
            self.config.traffic_shift_step * 100,
            self.config.traffic_shift_interval_sec,
            self.config.divergence_threshold * 100,
        )

    def stop(self) -> None:
        """Stop the background thread gracefully."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)

    def pause(self) -> None:
        """Pause automatic shifting (manual override)."""
        self._set_state(ShiftState.PAUSED)

    def resume(self) -> None:
        """Resume automatic shifting after a pause."""
        if self._state == ShiftState.PAUSED:
            self._set_state(ShiftState.SHIFTING)

    def force_shift(self, target_percentage: float) -> None:
        """Immediately set v2 traffic to ``target_percentage`` (0.0–1.0)."""
        self.runner.v2_percentage = target_percentage
        logger.info("Force-shifted v2 traffic to %.0f%%", target_percentage * 100)
        self._record_shift_event("force_shift", target_percentage)

    def force_rollback(self) -> None:
        """Immediately roll back to v1 regardless of current state."""
        self._do_rollback(reason="operator-forced")

    # ------------------------------------------------------------------
    # Background loop
    # ------------------------------------------------------------------

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self.config.traffic_shift_interval_sec)
            if self._stop_event.is_set():
                break
            self._tick()

    def _tick(self) -> None:
        state = self._state
        if state in (ShiftState.IDLE, ShiftState.PAUSED,
                     ShiftState.ROLLED_BACK, ShiftState.COMPLETE):
            return

        divergence = self.tracker.window_divergence_rate
        samples = self.tracker.sample_count
        current_pct = self.runner.v2_percentage

        logger.info(
            "Tick | state=%s v2=%.0f%% divergence=%.2f%% samples=%d",
            state.name, current_pct * 100, divergence * 100, samples,
        )

        # Rollback check (runs even when PROMOTED — v2 may still regress)
        if (
            self.config.enable_auto_rollback
            and current_pct > 0.0
            and divergence > self.config.rollback_threshold
        ):
            self._do_rollback(reason=f"divergence={divergence:.2%} > rollback_threshold={self.config.rollback_threshold:.2%}")
            return

        # Nothing more to do once fully promoted
        if state == ShiftState.PROMOTED:
            return

        # Gate: need enough samples before advancing
        if samples < self.config.min_samples_for_promotion:
            logger.info("Waiting for more samples (%d/%d)", samples, self.config.min_samples_for_promotion)
            return

        # Gate: divergence must be below threshold
        if divergence > self.config.divergence_threshold:
            logger.info(
                "Divergence too high (%.2f%% > %.2f%%) — holding at v2=%.0f%%",
                divergence * 100, self.config.divergence_threshold * 100, current_pct * 100,
            )
            return

        # Advance traffic
        if not self.config.enable_auto_promotion:
            return

        new_pct = min(1.0, current_pct + self.config.traffic_shift_step)
        self.runner.v2_percentage = new_pct
        self._set_state(ShiftState.SHIFTING)
        self._record_shift_event("auto_shift", new_pct, divergence=divergence, samples=samples)

        if new_pct >= 1.0:
            self._set_state(ShiftState.PROMOTED)
            logger.info("v2 fully promoted — running v1 as safety shadow")
            if self.on_promoted:
                self.on_promoted()

    # ------------------------------------------------------------------
    # Rollback
    # ------------------------------------------------------------------

    def _do_rollback(self, reason: str) -> None:
        logger.warning("ROLLBACK triggered: %s", reason)
        self.runner.v2_percentage = 0.0
        self._set_state(ShiftState.ROLLED_BACK)
        self._record_shift_event("rollback", 0.0, reason=reason)
        if self.on_rolled_back:
            self.on_rolled_back()

    # ------------------------------------------------------------------
    # History
    # ------------------------------------------------------------------

    def _record_shift_event(self, event: str, v2_pct: float, **extra) -> None:
        self._shift_history.append({
            "ts": time.time(),
            "event": event,
            "v2_percentage": round(v2_pct, 4),
            **extra,
        })

    def history(self) -> list:
        return list(self._shift_history)
