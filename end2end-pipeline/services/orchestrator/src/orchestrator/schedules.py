"""Schedules + jobs for the hourly analysis pipeline."""

from __future__ import annotations

from dagster import AssetSelection, build_schedule_from_partitioned_job, define_asset_job

hourly_analysis_job = define_asset_job(
    name="hourly_analysis_job",
    selection=AssetSelection.assets(
        "raw_events_parquet", "event_analysis", "analysis_report"
    ),
    description="Ingest → Spark analysis → ClickHouse report for one hourly window.",
)

hourly_analysis_schedule = build_schedule_from_partitioned_job(
    hourly_analysis_job,
    # Run at :05 to give the sink a few seconds past the hour boundary to land
    # any late events before we read the slice.
    minute_of_hour=5,
)
