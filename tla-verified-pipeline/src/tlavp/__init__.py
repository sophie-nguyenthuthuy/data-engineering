"""TLA+ verified pipeline + runtime monitor."""

from __future__ import annotations

__version__ = "0.1.0"

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from tlavp.invariants.safety import SafetyResult
    from tlavp.monitor.replay import Incident, Monitor
    from tlavp.state.machine import StateMachine

_LAZY: dict[str, str] = {
    "StateMachine": "tlavp.state.machine",
    "Monitor": "tlavp.monitor.replay",
    "Incident": "tlavp.monitor.replay",
    "SafetyResult": "tlavp.invariants.safety",
}


def __getattr__(name: str) -> Any:
    mod = _LAZY.get(name)
    if mod is None:
        raise AttributeError(f"module 'tlavp' has no attribute {name!r}")
    import importlib

    return getattr(importlib.import_module(mod), name)


__all__ = ["Incident", "Monitor", "SafetyResult", "StateMachine"]
