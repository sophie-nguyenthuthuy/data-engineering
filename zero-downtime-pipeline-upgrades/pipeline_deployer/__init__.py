"""
zero-downtime-pipeline-upgrades
================================
A deployment system for stateful stream-processing pipelines that lets v2
run in shadow mode alongside v1, compares outputs for divergence, and
gradually shifts traffic to v2 only when the diff is below a configurable
threshold.
"""

from .config import DeploymentConfig
from .pipeline import BasePipeline
from .comparator import DivergenceTracker, dict_divergence
from .shadow_runner import ShadowRunner
from .traffic_shifter import TrafficShifter, ShiftState
from .orchestrator import DeploymentOrchestrator

__all__ = [
    "DeploymentConfig",
    "BasePipeline",
    "DivergenceTracker",
    "dict_divergence",
    "ShadowRunner",
    "TrafficShifter",
    "ShiftState",
    "DeploymentOrchestrator",
]
