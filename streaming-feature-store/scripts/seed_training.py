"""
Seed script: generates a training dataset and saves it to the offline store.
Also writes global stats to Redis for z-score computation at serving time.

Run: python scripts/seed_training.py
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import logging
import numpy as np
import pandas as pd
from datetime import datetime, timezone

from feature_store.batch_processor import BatchProcessor
from feature_store.offline_store import OfflineStore
from feature_store.online_store import OnlineStore
from simulator.data_generator import generate_batch

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    logger.info("Generating 5000 training events…")
    raw_records = generate_batch(5000, start_seq=0)
    raw_df = pd.DataFrame(raw_records)

    logger.info("Running batch feature pipeline…")
    processor = BatchProcessor()
    features_df = processor.run_full_pipeline(raw_df, label="latest")

    logger.info("Saving features: %d rows, %d columns", len(features_df), len(features_df.columns))

    # Push global stats to Redis for serving-time z-score computation
    stats = processor.compute_global_stats(raw_df)
    online_store = OnlineStore()
    online_store.set_global_stats(stats)
    logger.info("Global stats pushed to Redis: %s", stats)

    logger.info("Training snapshot saved. Features: %s", list(features_df.columns))


if __name__ == "__main__":
    main()
