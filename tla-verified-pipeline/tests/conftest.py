"""Shared fixtures."""

from __future__ import annotations

import pytest

from tlavp.monitor.replay import Monitor
from tlavp.state.machine import StateMachine


@pytest.fixture
def sm() -> StateMachine:
    return StateMachine()


@pytest.fixture
def monitor() -> Monitor:
    return Monitor(max_lag=10, max_steps_to_delivery=200)
