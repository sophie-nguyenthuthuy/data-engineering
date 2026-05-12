"""
Retraining trigger: decides whether to kick off a retraining job
based on the drift report, then executes the trigger action.

Supported trigger backends:
  - webhook  (POST to RETRAINING_WEBHOOK_URL)
  - log      (just logs; useful for testing)

Set RETRAINING_BACKEND=webhook|log (default: log).
"""
from __future__ import annotations

import json
import logging
import os
import time

import httpx

from feature_store.drift_detector import DriftReport

logger = logging.getLogger(__name__)


RETRAINING_BACKEND = os.getenv("RETRAINING_BACKEND", "log")
RETRAINING_WEBHOOK_URL = os.getenv("RETRAINING_WEBHOOK_URL", "")
DRIFT_FRACTION_THRESHOLD = float(os.getenv("DRIFT_FRACTION_THRESHOLD", "0.25"))


class RetrainingTrigger:
    """
    Fires when the fraction of drifted features exceeds
    DRIFT_FRACTION_THRESHOLD (default 25%).
    """

    def should_trigger(self, report: DriftReport) -> bool:
        return report.overall_drift_score >= DRIFT_FRACTION_THRESHOLD

    def trigger(self, report: DriftReport) -> bool:
        """
        Execute the trigger. Returns True if the trigger fired successfully.
        """
        if not self.should_trigger(report):
            logger.info(
                "Drift score %.2f below threshold %.2f — no retraining needed.",
                report.overall_drift_score,
                DRIFT_FRACTION_THRESHOLD,
            )
            return False

        logger.warning(
            "RETRAINING TRIGGERED — drift score %.2f (threshold %.2f), "
            "drifted features: %s",
            report.overall_drift_score,
            DRIFT_FRACTION_THRESHOLD,
            report.drifted_features,
        )

        backend = RETRAINING_BACKEND.lower()
        if backend == "webhook":
            return self._trigger_webhook(report)
        else:
            return self._trigger_log(report)

    def _trigger_log(self, report: DriftReport) -> bool:
        logger.warning(
            "[RETRAINING] Would submit job with payload: %s",
            json.dumps({
                "triggered_at": report.generated_at,
                "drifted_features": report.drifted_features,
                "overall_drift_score": report.overall_drift_score,
            }),
        )
        return True

    def _trigger_webhook(self, report: DriftReport) -> bool:
        if not RETRAINING_WEBHOOK_URL:
            logger.error("RETRAINING_WEBHOOK_URL is not set; falling back to log.")
            return self._trigger_log(report)

        payload = {
            "triggered_at": report.generated_at,
            "drifted_features": report.drifted_features,
            "overall_drift_score": report.overall_drift_score,
            "feature_results": [
                {
                    "name": r.feature_name,
                    "drifted": r.drifted,
                    "details": r.details,
                }
                for r in report.feature_results
            ],
        }
        try:
            resp = httpx.post(
                RETRAINING_WEBHOOK_URL,
                json=payload,
                timeout=10.0,
            )
            resp.raise_for_status()
            logger.info("Retraining webhook responded %s", resp.status_code)
            return True
        except httpx.HTTPError as exc:
            logger.error("Retraining webhook failed: %s", exc)
            return False
