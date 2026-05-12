"""Synthetic workload generators."""

from __future__ import annotations

from pwm.workload.synthetic import (
    bimodal_workload,
    exponential_delay_workload,
    lognormal_delay_workload,
    pareto_delay_workload,
)

__all__ = [
    "bimodal_workload",
    "exponential_delay_workload",
    "lognormal_delay_workload",
    "pareto_delay_workload",
]
