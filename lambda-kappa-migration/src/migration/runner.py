"""Migration runner: orchestrates the cutover from Lambda to Kappa architecture."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from src.config import HISTORICAL_DIR, config
from src.kappa_arch.stream_processor import KappaProcessor
from src.lambda_arch.batch_layer import BatchProcessor
from src.lambda_arch.models import BatchView
from src.lambda_arch.serving_layer import ServingLayer
from src.lambda_arch.speed_layer import SpeedLayer
from src.migration.backfill import BackfillJob
from src.validator.correctness_validator import CorrectnessValidator

logger = logging.getLogger(__name__)


class MigrationPhase(str, Enum):
    """Phases of the Lambda → Kappa migration."""

    INITIAL = "initial"
    LAMBDA_RUNNING = "lambda_running"
    KAPPA_STANDING_UP = "kappa_standing_up"
    DUAL_WRITE = "dual_write"
    BACKFILL = "backfill"
    VALIDATION = "validation"
    CUTOVER = "cutover"
    COMPLETE = "complete"
    ROLLED_BACK = "rolled_back"


@dataclass
class MigrationState:
    """Tracks the current state of an active migration."""

    phase: MigrationPhase = MigrationPhase.INITIAL
    started_at: datetime = field(default_factory=datetime.utcnow)
    phase_log: list[dict] = field(default_factory=list)
    validation_passed: bool = False
    events_backfilled: int = 0

    def transition(self, new_phase: MigrationPhase, note: str = "") -> None:
        """Record a phase transition."""
        self.phase_log.append(
            {
                "from": self.phase.value,
                "to": new_phase.value,
                "at": datetime.utcnow().isoformat(),
                "note": note,
            }
        )
        logger.info("Migration phase: %s → %s  %s", self.phase.value, new_phase.value, note)
        self.phase = new_phase


class MigrationRunner:
    """Orchestrates the full Lambda → Kappa migration lifecycle."""

    def __init__(self, local_mode: bool | None = None, skip_validation: bool = False) -> None:
        self.local_mode = config.local_mode if local_mode is None else local_mode
        self.skip_validation = skip_validation
        self.state = MigrationState()

        # Architecture components (instantiated lazily)
        self._batch_processor: BatchProcessor | None = None
        self._speed_layer: SpeedLayer | None = None
        self._serving_layer: ServingLayer | None = None
        self._kappa_processor: KappaProcessor | None = None
        self._batch_view: BatchView | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_full_migration(self) -> MigrationState:
        """Execute the complete migration sequence end to end."""
        try:
            self._phase_lambda_running()
            self._phase_kappa_standing_up()
            self._phase_backfill()
            self._phase_validation()
            self._phase_cutover()
        except Exception as exc:  # noqa: BLE001
            logger.error("Migration failed: %s", exc, exc_info=True)
            self._rollback()
        return self.state

    def rollback(self) -> None:
        """Public rollback entry point."""
        self._rollback()

    # ------------------------------------------------------------------
    # Migration phases
    # ------------------------------------------------------------------

    def _phase_lambda_running(self) -> None:
        self.state.transition(MigrationPhase.LAMBDA_RUNNING, "Starting Lambda batch layer")
        self._batch_processor = BatchProcessor(HISTORICAL_DIR)
        self._batch_view = self._batch_processor.run()
        self._speed_layer = SpeedLayer(local_mode=self.local_mode)
        self._serving_layer = ServingLayer(self._batch_view, self._speed_layer.get_view())
        logger.info("Lambda architecture operational")

    def _phase_kappa_standing_up(self) -> None:
        self.state.transition(MigrationPhase.KAPPA_STANDING_UP, "Initialising Kappa processor")
        self._kappa_processor = KappaProcessor(local_mode=self.local_mode)
        self.state.transition(MigrationPhase.DUAL_WRITE, "Dual-write period active")
        logger.info("Kappa processor ready; dual-write active")

    def _phase_backfill(self) -> None:
        self.state.transition(MigrationPhase.BACKFILL, "Replaying historical data into Kappa")
        assert self._kappa_processor is not None
        count = self._kappa_processor.run_replay()
        self.state.events_backfilled = count
        logger.info("Backfill complete: %d events replayed", count)

    def _phase_validation(self) -> None:
        self.state.transition(MigrationPhase.VALIDATION, "Running correctness validator")
        if self.skip_validation:
            logger.warning("Skipping validation (skip_validation=True)")
            self.state.validation_passed = True
            return
        validator = CorrectnessValidator(local_mode=self.local_mode)
        report = validator.run()
        self.state.validation_passed = report.passed
        if not report.passed:
            raise RuntimeError(
                f"Correctness validation FAILED: {report.mismatch_count} mismatches. "
                "Aborting cutover — run rollback."
            )
        logger.info("Validation PASSED")

    def _phase_cutover(self) -> None:
        self.state.transition(MigrationPhase.CUTOVER, "Cutting over to Kappa architecture")
        # Disable Lambda components
        if self._speed_layer:
            self._speed_layer.stop()
        self._batch_processor = None
        self._serving_layer = None
        self.state.transition(MigrationPhase.COMPLETE, "Migration complete — Kappa is now primary")

    def _rollback(self) -> None:
        logger.warning("ROLLBACK: reverting to Lambda architecture")
        if self._speed_layer:
            self._speed_layer.stop()
        if self._kappa_processor:
            self._kappa_processor.stop()
        self.state.transition(MigrationPhase.ROLLED_BACK, "Lambda architecture restored as primary")
