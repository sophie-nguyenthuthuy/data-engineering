"""
Deployment orchestrator — the single entry-point for running a zero-downtime
pipeline version upgrade end-to-end.
"""

import logging
import time
from typing import Any, Dict, Iterable, Optional

from .comparator import DivergenceTracker
from .config import DeploymentConfig
from .pipeline import BasePipeline
from .shadow_runner import ShadowRunner
from .traffic_shifter import ShiftState, TrafficShifter

logger = logging.getLogger(__name__)


class DeploymentOrchestrator:
    """
    Coordinates the full lifecycle of a zero-downtime pipeline upgrade:

    1. **Setup** — initialise both pipelines.
    2. **Shadow phase** — v2 runs alongside v1; outputs are compared but v1
       serves all traffic.
    3. **Shift phase** — traffic migrates from v1 to v2 in configurable steps
       whenever divergence stays below the threshold.
    4. **Promotion** — v2 owns 100 % of traffic; v1 continues as a safety
       shadow until the caller calls ``complete()``.
    5. **Teardown** — both pipelines are shut down cleanly.

    At any point the orchestrator (or the TrafficShifter) may trigger a
    **rollback** back to v1 if divergence exceeds ``rollback_threshold``.

    Usage
    -----
    ::

        config = DeploymentConfig(divergence_threshold=0.02, traffic_shift_step=0.1)
        orch = DeploymentOrchestrator(v1_pipeline, v2_pipeline, config)
        orch.start()

        for record in stream:
            output = orch.process(record)
            # use output ...

        orch.complete()   # tear everything down after stream ends
    """

    def __init__(
        self,
        v1: BasePipeline,
        v2: BasePipeline,
        config: Optional[DeploymentConfig] = None,
    ) -> None:
        self.v1 = v1
        self.v2 = v2
        self.config = config or DeploymentConfig()
        self.config.validate()

        self.tracker = DivergenceTracker(
            window_size=self.config.comparison_window_size,
        )

        self.runner = ShadowRunner(
            v1=v1,
            v2=v2,
            config=self.config,
            tracker=self.tracker,
        )

        self.shifter = TrafficShifter(
            runner=self.runner,
            tracker=self.tracker,
            config=self.config,
            on_promoted=self._on_promoted,
            on_rolled_back=self._on_rolled_back,
        )

        self._started = False
        self._completed = False
        self._promoted = False
        self._rolled_back = False
        self._start_time: Optional[float] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> "DeploymentOrchestrator":
        """Set up pipelines and start the traffic-shifting background thread."""
        if self._started:
            raise RuntimeError("Orchestrator already started")

        logger.info(
            "Starting upgrade: %s → %s | threshold=%.1f%% rollback=%.1f%%",
            self.v1.version,
            self.v2.version,
            self.config.divergence_threshold * 100,
            self.config.rollback_threshold * 100,
        )

        self.runner.setup()
        self.shifter.start()

        self._started = True
        self._start_time = time.time()
        return self

    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single record through the active routing policy.

        Delegates to ``ShadowRunner.process``, which handles primary /
        shadow dispatch and divergence recording automatically.
        """
        if not self._started:
            raise RuntimeError("Call start() before process()")
        if self._completed:
            raise RuntimeError("Orchestrator has been completed/torn down")

        return self.runner.process(record)

    def process_stream(
        self,
        stream: Iterable[Dict[str, Any]],
        *,
        progress_every: int = 500,
    ) -> Iterable[Dict[str, Any]]:
        """
        Convenience generator: process an iterable of records, yielding each
        output and logging periodic progress.

        Args:
            stream:         Iterable of input records.
            progress_every: Log a summary every N records.
        """
        for i, record in enumerate(stream, start=1):
            yield self.process(record)
            if i % progress_every == 0:
                self._log_progress(i)

    def complete(self) -> Dict[str, Any]:
        """
        Cleanly shut down both pipelines and stop the shifter.

        Returns a final summary dict.
        """
        if self._completed:
            return {}

        logger.info("Completing deployment — tearing down pipelines")
        self.shifter.stop()
        self.runner.teardown()
        self._completed = True

        summary = self.status()
        elapsed = time.time() - (self._start_time or time.time())
        summary["elapsed_seconds"] = round(elapsed, 1)

        logger.info("Deployment complete: %s", summary)
        return summary

    # ------------------------------------------------------------------
    # Manual overrides
    # ------------------------------------------------------------------

    def pause(self) -> None:
        """Pause automatic traffic shifting."""
        self.shifter.pause()

    def resume(self) -> None:
        """Resume automatic traffic shifting."""
        self.shifter.resume()

    def force_shift(self, percentage: float) -> None:
        """Immediately set v2 traffic to *percentage* (0.0–1.0)."""
        self.shifter.force_shift(percentage)

    def rollback(self) -> None:
        """Immediately return all traffic to v1."""
        self.shifter.force_rollback()

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    def status(self) -> Dict[str, Any]:
        """Return a point-in-time snapshot of the deployment status."""
        return {
            "v1_version": self.v1.version,
            "v2_version": self.v2.version,
            "state": self.shifter.state.name,
            "promoted": self._promoted,
            "rolled_back": self._rolled_back,
            "runner_stats": self.runner.stats(),
            "shift_history": self.shifter.history(),
        }

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------

    def _on_promoted(self) -> None:
        self._promoted = True
        logger.info(
            "PROMOTED: %s is now primary. v1 (%s) running as safety shadow.",
            self.v2.version,
            self.v1.version,
        )

    def _on_rolled_back(self) -> None:
        self._rolled_back = True
        logger.warning(
            "ROLLED BACK: %s restored as primary. %s removed from traffic.",
            self.v1.version,
            self.v2.version,
        )

    def _log_progress(self, count: int) -> None:
        s = self.runner.stats()
        logger.info(
            "Progress | records=%d v2=%.0f%% divergence=%.2f%% state=%s",
            count,
            s["v2_percentage"] * 100,
            s["window_divergence_rate"] * 100,
            self.shifter.state.name,
        )
