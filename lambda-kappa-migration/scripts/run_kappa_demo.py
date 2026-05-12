#!/usr/bin/env python3
"""End-to-end Kappa architecture demo."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import os
os.environ.setdefault("LOCAL_MODE", "true")

from src.config import HISTORICAL_DIR, config
from src.kappa_arch.stream_processor import KappaProcessor

logging.basicConfig(
    level=getattr(logging, config.log_level),
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("kappa-demo")


def main() -> None:
    try:
        from rich.console import Console
        console = Console()
        has_rich = True
    except ImportError:
        has_rich = False
        console = None

    def hr(title: str) -> None:
        if has_rich:
            console.rule(f"[bold green]{title}[/bold green]")
        else:
            print(f"\n{'='*60}\n  {title}\n{'='*60}")

    hr("Kappa Architecture Demo")

    # --- Ensure seed data exists ---
    json_files = list(HISTORICAL_DIR.glob("*.json"))
    if not json_files:
        logger.info("No historical data found — running seed_data.py first...")
        from scripts.seed_data import main as seed_main
        seed_main()

    # --- Replay ---
    hr("Step 1: Replay historical events through Kappa stream processor")
    processor = KappaProcessor(local_mode=True)
    count = processor.run_replay(historical_dir=HISTORICAL_DIR)
    print(f"  Replayed {count:,} events")

    # --- Query results ---
    hr("Step 2: Query results from Kappa state store")
    results = processor.get_results()

    user_totals = results["user_totals"]
    et_summary = results["event_type_summary"]
    hourly = results["hourly_event_counts"]

    print(f"\n  Users tracked: {len(user_totals)}")
    print(f"  Hour buckets: {len(hourly)}")
    print("\n  Event-type summary:")
    for et, vals in sorted(et_summary.items()):
        print(f"    {et:12s}  count={int(vals['count']):6d}  total_amount=${float(vals['total_amount']):10.2f}  avg=${float(vals['avg_amount']):.2f}")

    print("\n  Top 5 users by total spend:")
    top_users = sorted(user_totals.items(), key=lambda x: float(x[1]["total_amount"]), reverse=True)[:5]
    for uid, vals in top_users:
        print(f"    {uid}  spend=${float(vals['total_amount']):.2f}  events={int(vals['event_count'])}")

    hr("Kappa Demo Complete")


if __name__ == "__main__":
    main()
