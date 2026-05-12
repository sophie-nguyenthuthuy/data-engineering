#!/usr/bin/env python3
"""End-to-end Lambda architecture demo."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import os
os.environ.setdefault("LOCAL_MODE", "true")

from src.config import HISTORICAL_DIR, config
from src.lambda_arch.batch_layer import BatchProcessor
from src.lambda_arch.serving_layer import ServingLayer
from src.lambda_arch.speed_layer import SpeedLayer

logging.basicConfig(
    level=getattr(logging, config.log_level),
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("lambda-demo")


def main() -> None:
    try:
        from rich.console import Console
        from rich import print as rprint
        console = Console()
        has_rich = True
    except ImportError:
        has_rich = False
        console = None

    def hr(title: str) -> None:
        if has_rich:
            console.rule(f"[bold cyan]{title}[/bold cyan]")
        else:
            print(f"\n{'='*60}\n  {title}\n{'='*60}")

    hr("Lambda Architecture Demo")

    # --- Ensure seed data exists ---
    json_files = list(HISTORICAL_DIR.glob("*.json"))
    if not json_files:
        logger.info("No historical data found — running seed_data.py first...")
        from scripts.seed_data import main as seed_main
        seed_main()

    # --- Batch Layer ---
    hr("Step 1: Batch Layer")
    processor = BatchProcessor(historical_dir=HISTORICAL_DIR)
    batch_view = processor.run()
    print(f"  Batch view computed. Event types: {list(batch_view.event_type_summary.data.keys())}")

    # --- Speed Layer (local, no Kafka needed) ---
    hr("Step 2: Speed Layer (local mode)")
    speed_layer = SpeedLayer(local_mode=True)
    realtime_view = speed_layer.get_view()
    print("  Speed layer initialised (no live events in local mode demo)")

    # --- Serving Layer ---
    hr("Step 3: Serving Layer — query results")
    serving = ServingLayer(batch_view=batch_view, realtime_view=realtime_view)

    user_totals = serving.get_user_totals()
    et_summary = serving.get_event_type_summary()
    hourly = serving.get_hourly_event_counts()

    print(f"\n  Users tracked: {len(user_totals)}")
    print(f"  Hour buckets: {len(hourly)}")
    print("\n  Event-type summary:")
    for et, vals in sorted(et_summary.items()):
        print(f"    {et:12s}  count={vals['count']:6d}  total_amount=${vals['total_amount']:10.2f}  avg=${vals['avg_amount']:.2f}")

    print("\n  Top 5 users by total spend:")
    top_users = sorted(user_totals.items(), key=lambda x: x[1]["total_amount"], reverse=True)[:5]
    for uid, vals in top_users:
        print(f"    {uid}  spend=${vals['total_amount']:.2f}  events={vals['event_count']}")

    hr("Lambda Demo Complete")


if __name__ == "__main__":
    main()
