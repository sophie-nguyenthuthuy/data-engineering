"""forecasting-pipeline-scheduler — DAG scheduler with forecasts + B&B."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

if TYPE_CHECKING:
    from fps.bench import random_layered_dag
    from fps.dag import DAG, CycleError, Task
    from fps.forecast.cusum import CUSUMDetector
    from fps.forecast.lognormal import LognormalForecaster, TaskStats
    from fps.scheduler.baseline import baseline_fcfs_schedule
    from fps.scheduler.branch_bound import branch_and_bound
    from fps.scheduler.common import (
        Schedule,
        ScheduledTask,
        assert_valid_schedule,
        makespan,
    )
    from fps.scheduler.list_sched import list_schedule
    from fps.shadow import RegretReport, regret, regret_over_dags

_LAZY: dict[str, tuple[str, str]] = {
    "Task": ("fps.dag", "Task"),
    "DAG": ("fps.dag", "DAG"),
    "CycleError": ("fps.dag", "CycleError"),
    "TaskStats": ("fps.forecast.lognormal", "TaskStats"),
    "LognormalForecaster": ("fps.forecast.lognormal", "LognormalForecaster"),
    "CUSUMDetector": ("fps.forecast.cusum", "CUSUMDetector"),
    "ScheduledTask": ("fps.scheduler.common", "ScheduledTask"),
    "Schedule": ("fps.scheduler.common", "Schedule"),
    "makespan": ("fps.scheduler.common", "makespan"),
    "assert_valid_schedule": ("fps.scheduler.common", "assert_valid_schedule"),
    "list_schedule": ("fps.scheduler.list_sched", "list_schedule"),
    "baseline_fcfs_schedule": ("fps.scheduler.baseline", "baseline_fcfs_schedule"),
    "branch_and_bound": ("fps.scheduler.branch_bound", "branch_and_bound"),
    "RegretReport": ("fps.shadow", "RegretReport"),
    "regret": ("fps.shadow", "regret"),
    "regret_over_dags": ("fps.shadow", "regret_over_dags"),
    "random_layered_dag": ("fps.bench", "random_layered_dag"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        from importlib import import_module

        module, attr = _LAZY[name]
        return getattr(import_module(module), attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "DAG",
    "CUSUMDetector",
    "CycleError",
    "LognormalForecaster",
    "RegretReport",
    "Schedule",
    "ScheduledTask",
    "Task",
    "TaskStats",
    "__version__",
    "assert_valid_schedule",
    "baseline_fcfs_schedule",
    "branch_and_bound",
    "list_schedule",
    "makespan",
    "random_layered_dag",
    "regret",
    "regret_over_dags",
]
