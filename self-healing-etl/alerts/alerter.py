from __future__ import annotations

import json
import logging
import textwrap
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import IntEnum
from typing import Any

logger = logging.getLogger(__name__)


class Severity(IntEnum):
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50

    @classmethod
    def from_str(cls, s: str) -> "Severity":
        return cls[s.upper()]


@dataclass
class Alert:
    title: str
    severity: Severity
    pipeline_name: str
    source_name: str
    run_id: str
    summary: str
    root_cause_hints: list[str] = field(default_factory=list)
    suggested_actions: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        return {
            "alert": {
                "title": self.title,
                "severity": self.severity.name,
                "timestamp": self.timestamp.isoformat(),
                "pipeline": self.pipeline_name,
                "source": self.source_name,
                "run_id": self.run_id,
            },
            "summary": self.summary,
            "root_cause_hints": self.root_cause_hints,
            "suggested_actions": self.suggested_actions,
            "metrics": self.metrics,
        }

    def to_slack_payload(self) -> dict[str, Any]:
        color = {
            Severity.DEBUG: "#95a5a6",
            Severity.INFO: "#3498db",
            Severity.WARNING: "#f39c12",
            Severity.ERROR: "#e74c3c",
            Severity.CRITICAL: "#8e44ad",
        }.get(self.severity, "#95a5a6")

        hints_text = "\n".join(f"• {h}" for h in self.root_cause_hints) or "_none_"
        actions_text = "\n".join(f"• {a}" for a in self.suggested_actions) or "_none_"

        return {
            "attachments": [
                {
                    "color": color,
                    "title": f"[{self.severity.name}] {self.title}",
                    "text": self.summary,
                    "fields": [
                        {"title": "Pipeline", "value": self.pipeline_name, "short": True},
                        {"title": "Source", "value": self.source_name, "short": True},
                        {"title": "Run ID", "value": self.run_id, "short": True},
                        {"title": "Time", "value": self.timestamp.isoformat(), "short": True},
                        {"title": "Root-cause hints", "value": hints_text, "short": False},
                        {"title": "Suggested actions", "value": actions_text, "short": False},
                    ],
                    "footer": "Self-Healing ETL",
                }
            ]
        }

    def to_human_text(self) -> str:
        lines = [
            f"{'='*60}",
            f"[{self.severity.name}] {self.title}",
            f"Pipeline : {self.pipeline_name}",
            f"Source   : {self.source_name}",
            f"Run ID   : {self.run_id}",
            f"Time     : {self.timestamp.isoformat()}",
            f"",
            f"Summary",
            f"-------",
            textwrap.fill(self.summary, 70),
        ]
        if self.metrics:
            lines += ["", "Metrics", "-------"]
            lines += [f"  {k}: {v}" for k, v in self.metrics.items()]
        if self.root_cause_hints:
            lines += ["", "Root-cause hints", "----------------"]
            lines += [f"  • {h}" for h in self.root_cause_hints]
        if self.suggested_actions:
            lines += ["", "Suggested actions", "-----------------"]
            lines += [f"  • {a}" for a in self.suggested_actions]
        lines.append("=" * 60)
        return "\n".join(lines)


class Alerter:
    """
    Multi-channel alert dispatcher.

    Channels enabled by config:
      - console  (always active)
      - slack    (if slack_webhook_url is set)
    """

    def __init__(
        self,
        slack_webhook_url: str | None = None,
        min_severity: str = "WARNING",
    ):
        self.slack_webhook_url = slack_webhook_url
        self.min_severity = Severity.from_str(min_severity)

    def send(self, alert: Alert) -> None:
        if alert.severity < self.min_severity:
            return
        self._log(alert)
        if self.slack_webhook_url:
            self._slack(alert)

    # ------------------------------------------------------------------
    # Convenience factory methods
    # ------------------------------------------------------------------

    def schema_drift_alert(
        self,
        pipeline_name: str,
        source_name: str,
        run_id: str,
        drift_summary: str,
        root_cause_hints: list[str],
        healed: bool,
        metrics: dict[str, Any],
    ) -> None:
        severity = Severity.WARNING if healed else Severity.ERROR
        actions = _drift_suggested_actions(healed)
        self.send(Alert(
            title="Schema Drift Detected",
            severity=severity,
            pipeline_name=pipeline_name,
            source_name=source_name,
            run_id=run_id,
            summary=(
                f"Schema drift detected in source '{source_name}'. "
                f"{'Auto-healed.' if healed else 'Could NOT auto-heal — manual review required.'} "
                f"Details: {drift_summary}"
            ),
            root_cause_hints=root_cause_hints,
            suggested_actions=actions,
            metrics=metrics,
        ))

    def quarantine_alert(
        self,
        pipeline_name: str,
        source_name: str,
        run_id: str,
        n_quarantined: int,
        error_type: str,
        root_cause_hints: list[str],
        metrics: dict[str, Any],
    ) -> None:
        self.send(Alert(
            title="Records Quarantined",
            severity=Severity.ERROR,
            pipeline_name=pipeline_name,
            source_name=source_name,
            run_id=run_id,
            summary=(
                f"{n_quarantined} record(s) quarantined from source '{source_name}' "
                f"due to [{error_type}]."
            ),
            root_cause_hints=root_cause_hints,
            suggested_actions=[
                "Review quarantine_records table for affected rows.",
                "Identify whether issue is systemic or isolated batch.",
                "Re-run with healing enabled after fixing upstream.",
            ],
            metrics=metrics,
        ))

    def pipeline_failure_alert(
        self,
        pipeline_name: str,
        source_name: str,
        run_id: str,
        error_message: str,
        root_cause_hints: list[str],
    ) -> None:
        self.send(Alert(
            title="Pipeline Run Failed",
            severity=Severity.CRITICAL,
            pipeline_name=pipeline_name,
            source_name=source_name,
            run_id=run_id,
            summary=f"Pipeline '{pipeline_name}' failed: {error_message}",
            root_cause_hints=root_cause_hints,
            suggested_actions=[
                "Check pipeline logs for full traceback.",
                "Verify source connectivity and credentials.",
                "Inspect quarantine store for partial loads.",
            ],
        ))

    # ------------------------------------------------------------------
    # Channel implementations
    # ------------------------------------------------------------------

    def _log(self, alert: Alert) -> None:
        text = alert.to_human_text()
        log_level = {
            Severity.DEBUG: logging.DEBUG,
            Severity.INFO: logging.INFO,
            Severity.WARNING: logging.WARNING,
            Severity.ERROR: logging.ERROR,
            Severity.CRITICAL: logging.CRITICAL,
        }.get(alert.severity, logging.WARNING)
        logger.log(log_level, "\n%s", text)

    def _slack(self, alert: Alert) -> None:
        try:
            import httpx
            payload = alert.to_slack_payload()
            resp = httpx.post(
                self.slack_webhook_url,
                json=payload,
                timeout=10,
            )
            if resp.status_code != 200:
                logger.warning("Slack alert failed (%s): %s", resp.status_code, resp.text)
        except Exception as exc:
            logger.warning("Slack alert dispatch error: %s", exc)


def _drift_suggested_actions(healed: bool) -> list[str]:
    base = [
        "Review drift_events table for full change details.",
        "Notify the upstream data producer of the schema change.",
    ]
    if healed:
        return base + [
            "Validate the healed output sample before promoting to production.",
            "Update downstream consumers if new columns were added.",
        ]
    return base + [
        "Manual intervention required — check quarantine_records.",
        "Fix schema or healing config, then re-run the pipeline.",
    ]
