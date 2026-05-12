"""Dagster entrypoint: `dagster dev -m re_pipeline.definitions`."""
from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)

from .assets import (
    bronze_listings,
    gold_city_stats,
    gold_price_bands,
    silver_listings,
    silver_price_history,
)

all_assets = [
    bronze_listings,
    silver_listings,
    silver_price_history,
    gold_city_stats,
    gold_price_bands,
]

daily_job = define_asset_job("daily_refresh", selection=AssetSelection.all())

daily_schedule = ScheduleDefinition(
    job=daily_job,
    cron_schedule="0 6 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
)

defs = Definitions(
    assets=all_assets,
    jobs=[daily_job],
    schedules=[daily_schedule],
)
