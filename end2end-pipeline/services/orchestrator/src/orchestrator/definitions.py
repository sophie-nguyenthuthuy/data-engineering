"""Dagster `Definitions` entry point. Loaded via `workspace.yaml`."""

from dagster import Definitions, load_assets_from_modules

from orchestrator.assets import analysis, raw_events, report
from orchestrator.jobs import dlq_replay_job
from orchestrator.resources import build_resources
from orchestrator.schedules import hourly_analysis_job, hourly_analysis_schedule

_assets = load_assets_from_modules([raw_events, analysis, report])

defs = Definitions(
    assets=_assets,
    resources=build_resources(),
    jobs=[hourly_analysis_job, dlq_replay_job],
    schedules=[hourly_analysis_schedule],
)
