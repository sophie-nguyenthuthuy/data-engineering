"""
Example: generate a training dataset with point-in-time correct feature joins.

This prevents label leakage: each training row sees only features that existed
BEFORE the label was generated (e.g. before a purchase event).
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
import random

import pandas as pd

from feature_store.offline.parquet_store import OfflineStore
from feature_store.consistency.sync import ConsistencyManager
from feature_store.online.redis_store import OnlineStore
from feature_store.registry.feature_registry import FeatureRegistry


def main(offline_path: str = "./data/offline") -> None:
    offline = OfflineStore(base_path=offline_path, write_batch_size=1000)

    # --- Simulate historical feature writes ---
    print("Writing simulated feature history...")
    base_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    for day in range(30):
        ts = base_ts + timedelta(days=day)
        records = [
            (
                f"user_{i:05d}",
                {
                    "total_purchases": random.randint(0, 100) + day,
                    "churn_risk_score": round(random.random(), 4),
                    "avg_session_duration_sec": round(random.uniform(30, 900), 1),
                },
                ts,
            )
            for i in range(200)
        ]
        offline.write_batch("user_features", records)

    offline.flush()
    print("Feature history written.")

    # --- Simulate label data (purchase events) ---
    label_rows = []
    for i in range(100):
        purchase_ts = base_ts + timedelta(days=random.randint(5, 29), hours=random.randint(0, 23))
        label_rows.append({
            "entity_id": f"user_{i:05d}",
            "label_timestamp": purchase_ts,
            "purchased": 1,
        })
    entity_df = pd.DataFrame(label_rows)

    # --- Point-in-time join ---
    print("\nPerforming point-in-time feature join...")
    training_df = offline.point_in_time_join(
        entity_df,
        group="user_features",
        feature_names=["total_purchases", "churn_risk_score"],
        timestamp_col="label_timestamp",
    )

    print(f"\nTraining dataset shape: {training_df.shape}")
    print(training_df[["entity_id", "label_timestamp", "total_purchases", "churn_risk_score", "purchased"]].head(10).to_string(index=False))
    print("\n✓ Training data is label-leakage free (features predate label timestamps)")

    # --- Save to parquet for model training ---
    out_path = "./data/training_dataset.parquet"
    training_df.to_parquet(out_path, index=False)
    print(f"\nTraining dataset saved to {out_path}")


if __name__ == "__main__":
    main()
