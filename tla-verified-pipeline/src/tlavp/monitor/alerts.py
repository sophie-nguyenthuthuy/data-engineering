"""Alert sinks.

Production deployment would route to PagerDuty / Slack / OpsGenie.
For tests we expose:
  - ListAlertSink: collects alerts in memory
  - ConsoleAlertSink: prints to stdout
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


class AlertSink(ABC):
    @abstractmethod
    def emit(self, incident: dict[str, Any]) -> None: ...


@dataclass
class ListAlertSink(AlertSink):
    incidents: list[dict[str, Any]] = field(default_factory=list)

    def emit(self, incident: dict[str, Any]) -> None:
        self.incidents.append(incident)


class ConsoleAlertSink(AlertSink):
    def emit(self, incident: dict[str, Any]) -> None:
        violations = incident.get("violations", [])
        step = incident.get("step", "?")
        print(f"[INCIDENT step={step}] {', '.join(violations)}")


__all__ = ["AlertSink", "ConsoleAlertSink", "ListAlertSink"]
