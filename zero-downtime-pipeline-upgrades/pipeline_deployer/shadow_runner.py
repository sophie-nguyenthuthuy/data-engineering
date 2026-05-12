"""
Shadow runner — executes every record through both v1 (primary) and v2 (shadow),
compares outputs, and records divergence without affecting live responses.
"""

import hashlib
import json
import logging
import threading
import time
from typing import Any, Callable, Dict, Optional

from .comparator import DivergenceTracker
from .config import DeploymentConfig
from .pipeline import BasePipeline

logger = logging.getLogger(__name__)


def _stable_hash(record: Dict[str, Any]) -> int:
    """Deterministic integer hash of a record used for traffic routing."""
    raw = json.dumps(record, sort_keys=True, default=str).encode()
    return int(hashlib.md5(raw).hexdigest(), 16)


class ShadowRunner:
    """
    Routes traffic between v1 and v2 with a configurable split.

    Modes
    -----
    * **Pure shadow** (``v2_percentage == 0``): every record goes to v1;
      v2 runs in parallel but its output is only compared, never returned.
    * **Canary** (``0 < v2_percentage < 1``): a deterministic hash of each
      record decides which pipeline *owns* the response.  The other pipeline
      still runs in the background for comparison.
    * **Full cutover** (``v2_percentage == 1``): v2 owns all responses; v1
      still runs in shadow for rollback safety.

    Args:
        v1:       Live (primary) pipeline instance.
        v2:       Candidate (shadow) pipeline instance.
        config:   Deployment configuration.
        tracker:  Shared divergence tracker (injected by orchestrator).
    """

    def __init__(
        self,
        v1: BasePipeline,
        v2: BasePipeline,
        config: DeploymentConfig,
        tracker: DivergenceTracker,
    ) -> None:
        self.v1 = v1
        self.v2 = v2
        self.config = config
        self.tracker = tracker

        self._v2_percentage: float = config.initial_v2_percentage
        self._lock = threading.Lock()
        self._shadow_log_file = None

        if config.shadow_log_path:
            self._shadow_log_file = open(config.shadow_log_path, "a")

        # Per-run stats
        self._records_processed: int = 0
        self._v1_errors: int = 0
        self._v2_errors: int = 0
        self._last_divergence_score: float = 0.0

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def v2_percentage(self) -> float:
        return self._v2_percentage

    @v2_percentage.setter
    def v2_percentage(self, value: float) -> None:
        value = max(0.0, min(1.0, value))
        with self._lock:
            old = self._v2_percentage
            self._v2_percentage = value
        logger.info("Traffic split updated: v1=%.0f%% v2=%.0f%%", (1 - value) * 100, value * 100)

    # ------------------------------------------------------------------
    # Core processing
    # ------------------------------------------------------------------

    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process one record and return the authoritative response.

        The non-authoritative pipeline always runs asynchronously so it
        never adds latency to the caller.
        """
        with self._lock:
            pct = self._v2_percentage

        record_hash = _stable_hash(record)
        v2_is_primary = (record_hash % 10_000) < int(pct * 10_000)

        primary = self.v2 if v2_is_primary else self.v1
        shadow = self.v1 if v2_is_primary else self.v2

        # Run primary synchronously
        primary_output, primary_err = self._safe_process(primary, record)

        # Run shadow asynchronously
        shadow_thread = threading.Thread(
            target=self._shadow_compare,
            args=(shadow, record, primary_output, v2_is_primary),
            daemon=True,
        )
        shadow_thread.start()

        self._records_processed += 1

        if primary_err:
            raise primary_err

        return primary_output

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _safe_process(
        self, pipeline: BasePipeline, record: Dict[str, Any]
    ) -> tuple:
        """Run pipeline.process and capture exceptions without raising."""
        try:
            result = pipeline.process(record)
            return result, None
        except Exception as exc:  # noqa: BLE001
            if pipeline is self.v1:
                self._v1_errors += 1
            else:
                self._v2_errors += 1
            logger.error("Pipeline %s raised: %s", pipeline.version, exc)
            return {}, exc

    def _shadow_compare(
        self,
        shadow: BasePipeline,
        record: Dict[str, Any],
        primary_output: Dict[str, Any],
        v2_is_primary: bool,
    ) -> None:
        shadow_output, shadow_err = self._safe_process(shadow, record)
        if shadow_err:
            return  # error already logged

        v1_out = primary_output if not v2_is_primary else shadow_output
        v2_out = shadow_output if not v2_is_primary else primary_output

        score = self.tracker.record(v1_out, v2_out)
        self._last_divergence_score = score

        if self._shadow_log_file and score > 0.0:
            entry = {
                "ts": time.time(),
                "divergence_score": round(score, 6),
                "v1_output": v1_out,
                "v2_output": v2_out,
            }
            self._shadow_log_file.write(json.dumps(entry) + "\n")
            self._shadow_log_file.flush()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def setup(self) -> None:
        logger.info("Setting up pipeline %s", self.v1.version)
        self.v1.setup()
        logger.info("Setting up pipeline %s", self.v2.version)
        self.v2.setup()

    def teardown(self) -> None:
        self.v1.teardown()
        self.v2.teardown()
        if self._shadow_log_file:
            self._shadow_log_file.close()

    def stats(self) -> Dict[str, Any]:
        return {
            "records_processed": self._records_processed,
            "v2_percentage": round(self._v2_percentage, 4),
            "v1_errors": self._v1_errors,
            "v2_errors": self._v2_errors,
            "last_divergence_score": round(self._last_divergence_score, 6),
            **self.tracker.summary(),
        }
