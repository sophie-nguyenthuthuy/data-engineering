from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Callable, Optional

from .models import AlertEvent, DQDimension, PosteriorState

logger = logging.getLogger(__name__)


class AlertManager:
    """
    Fires AlertEvents when P(healthy) drops below per-dimension thresholds.

    Parameters
    ----------
    thresholds:
        Dict mapping DQDimension -> minimum acceptable P(healthy).
        Defaults to 0.20 for all dimensions if not specified.
    cooldown_batches:
        Suppress repeat alerts for the same dimension until this many
        batches have passed (prevents alert storms during degraded periods).
    handlers:
        Optional list of callables(AlertEvent) -> None for custom sinks
        (e.g. Slack, PagerDuty, email).
    """

    DEFAULT_THRESHOLD = 0.20

    def __init__(
        self,
        thresholds: Optional[dict[DQDimension, float]] = None,
        cooldown_batches: int = 3,
        handlers: Optional[list[Callable[[AlertEvent], None]]] = None,
    ):
        self.thresholds: dict[DQDimension, float] = thresholds or {}
        self.cooldown_batches = cooldown_batches
        self.handlers: list[Callable[[AlertEvent], None]] = handlers or []
        self._history: list[AlertEvent] = []
        self._last_alert_batch: dict[DQDimension, int] = {}
        self._global_batch_counter = 0

    def threshold_for(self, dimension: DQDimension) -> float:
        return self.thresholds.get(dimension, self.DEFAULT_THRESHOLD)

    def start_batch(self) -> None:
        """Call once per batch before evaluating all dimensions."""
        self._global_batch_counter += 1

    def evaluate(
        self,
        dimension: DQDimension,
        p_healthy: float,
        posterior: PosteriorState,
        batch_id: str,
    ) -> Optional[AlertEvent]:
        """
        Check p_healthy against threshold.  Returns an AlertEvent if the
        alert fires, otherwise None.  Call start_batch() once per batch
        before evaluating dimensions so the cooldown counts batches, not
        individual evaluate() calls.
        """
        threshold = self.threshold_for(dimension)

        if p_healthy >= threshold:
            return None

        last_alert = self._last_alert_batch.get(dimension, -999)
        if self._global_batch_counter - last_alert < self.cooldown_batches:
            logger.debug(
                "Alert suppressed (cooldown) for %s in batch %s", dimension.value, batch_id
            )
            return None

        event = AlertEvent(
            dimension=dimension,
            batch_id=batch_id,
            timestamp=datetime.now(timezone.utc),
            p_healthy=p_healthy,
            threshold=threshold,
            posterior_mean=posterior.mean,
            posterior_std=posterior.std,
            message=(
                f"[{dimension.value.upper()}] P(healthy)={p_healthy:.3f} < "
                f"threshold={threshold:.3f}. "
                f"Posterior mean={posterior.mean:.3f} ± {posterior.std:.3f}"
            ),
        )

        self._history.append(event)
        self._last_alert_batch[dimension] = self._global_batch_counter
        self._dispatch(event)
        return event

    def _dispatch(self, event: AlertEvent) -> None:
        logger.warning(event.message)
        for handler in self.handlers:
            try:
                handler(event)
            except Exception as exc:  # noqa: BLE001
                logger.error("Alert handler %s raised: %s", handler, exc)

    @property
    def history(self) -> list[AlertEvent]:
        return list(self._history)

    def add_handler(self, fn: Callable[[AlertEvent], None]) -> None:
        self.handlers.append(fn)

    def clear_history(self) -> None:
        self._history.clear()
