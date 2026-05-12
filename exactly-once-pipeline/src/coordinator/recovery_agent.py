"""
Recovery Agent — periodically scans for stuck sagas and either retries
them (by resetting the outbox entry) or compensates them (if retry limit
is exceeded).

Run this as a sidecar to the main pipeline components.
"""
from __future__ import annotations

import time

import structlog

from src.coordinator.transaction_coordinator import TransactionCoordinator
from src.db import transaction

log = structlog.get_logger(__name__)

coordinator = TransactionCoordinator()
MAX_RETRIES = 10
SCAN_INTERVAL_SECONDS = 30


class RecoveryAgent:
    def start(self) -> None:
        log.info("recovery_agent.started", scan_interval=SCAN_INTERVAL_SECONDS)
        while True:
            try:
                self._scan()
            except Exception as exc:
                log.error("recovery_agent.scan_error", error=str(exc))
            time.sleep(SCAN_INTERVAL_SECONDS)

    def _scan(self) -> None:
        incomplete = coordinator.recover_incomplete(max_age_minutes=5)
        for saga in incomplete:
            key = saga["idempotency_key"]
            retries = saga["retry_count"]
            step = saga["current_step"]

            if retries >= MAX_RETRIES:
                log.error("recovery_agent.compensating",
                          idempotency_key=key, retries=retries, step=step)
                coordinator.compensate(
                    key,
                    f"Exceeded max retries ({MAX_RETRIES}) at step {step}",
                )
            else:
                log.warning("recovery_agent.requeuing",
                            idempotency_key=key, retries=retries, step=step)
                self._reset_outbox(key)

    def _reset_outbox(self, idempotency_key: str) -> None:
        """Re-arm the outbox entry so the poller will republish it."""
        with transaction() as cur:
            cur.execute(
                """
                UPDATE outbox
                SET    published_at = NULL,
                       retry_count  = retry_count + 1
                WHERE  idempotency_key = %s
                  AND  published_at IS NOT NULL
                """,
                (idempotency_key,),
            )
            affected = cur.rowcount
        if affected:
            log.info("recovery_agent.outbox_reset", idempotency_key=idempotency_key)
