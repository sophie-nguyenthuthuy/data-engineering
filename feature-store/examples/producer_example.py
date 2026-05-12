"""
Example: publish feature events to Kafka.

Run with:
  python examples/producer_example.py

Assumes Kafka is running on localhost:9092 (via docker-compose).
"""
from __future__ import annotations

import random
import time

from feature_store.ingestion.kafka_producer import FeatureProducer


def simulate_user_event(user_id: str) -> dict:
    return {
        "total_purchases": random.randint(0, 500),
        "avg_session_duration_sec": round(random.uniform(30, 900), 1),
        "churn_risk_score": round(random.random(), 4),
        "user_age_days": random.randint(1, 1825),
    }


def simulate_realtime_event(user_id: str) -> dict:
    return {
        "session_page_views": random.randint(1, 50),
        "cart_value_usd": round(random.uniform(0, 500), 2),
        "click_through_rate_1h": round(random.uniform(0, 0.5), 4),
    }


def main() -> None:
    producer = FeatureProducer(bootstrap_servers="localhost:9092")

    print("Publishing feature events... (Ctrl+C to stop)")
    user_ids = [f"user_{i:05d}" for i in range(1000)]
    n = 0
    try:
        while True:
            uid = random.choice(user_ids)
            # User features (slower changing)
            if random.random() < 0.3:
                producer.publish("user_features", uid, simulate_user_event(uid))
            # Realtime features (every event)
            producer.publish("realtime_features", uid, simulate_realtime_event(uid))
            n += 1
            if n % 100 == 0:
                print(f"  Published {n} events")
            time.sleep(0.01)  # 100 events/sec
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"Done. Total events: {n}")


if __name__ == "__main__":
    main()
