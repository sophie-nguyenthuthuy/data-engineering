"""
Synthetic banking transaction producer.
Publishes realistic transaction events to Kafka, with seeded anomaly patterns.
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.getenv("TRANSACTIONS_TOPIC", "transactions")
TPS = float(os.getenv("TPS", "5"))

ACCOUNTS = [f"ACC{i:06d}" for i in range(1, 201)]

MERCHANTS = [
    ("Amazon", "online_retail"),
    ("Walmart", "grocery"),
    ("Shell", "fuel"),
    ("Starbucks", "dining"),
    ("Netflix", "subscription"),
    ("Delta Airlines", "travel"),
    ("Marriott", "lodging"),
    ("CVS Pharmacy", "pharmacy"),
    ("Home Depot", "hardware"),
    ("Best Buy", "electronics"),
    ("Uber", "transport"),
    ("DoorDash", "dining"),
    ("Apple Store", "electronics"),
    ("Costco", "grocery"),
    ("BP Gas", "fuel"),
    ("ATM Withdrawal", "atm"),
    ("Western Union", "wire_transfer"),
    ("Casino Royale", "gambling"),
    ("Coinbase", "crypto"),
    ("Unknown Merchant", "unknown"),
]

CITIES = [
    ("New York", 40.7128, -74.0060),
    ("Los Angeles", 34.0522, -118.2437),
    ("Chicago", 41.8781, -87.6298),
    ("Houston", 29.7604, -95.3698),
    ("Miami", 25.7617, -80.1918),
    ("Seattle", 47.6062, -122.3321),
    ("Boston", 42.3601, -71.0589),
    ("Denver", 39.7392, -104.9903),
    ("Phoenix", 33.4484, -112.0740),
    ("Atlanta", 33.7490, -84.3880),
    # International — used to trigger geo-velocity anomalies
    ("London", 51.5074, -0.1278),
    ("Tokyo", 35.6762, 139.6503),
    ("Sydney", -33.8688, 151.2093),
]

# Accounts that will exhibit anomalous behaviour during this run
ANOMALOUS_ACCOUNTS = random.sample(ACCOUNTS, k=20)


def delivery_report(err, msg):
    if err:
        print(f"[producer] delivery failed: {err}")


def normal_transaction(account_id: str) -> dict:
    merchant, category = random.choice(MERCHANTS[:14])  # mainstream only
    city, lat, lon = random.choice(CITIES[:10])
    hour = datetime.now().hour
    # Slightly higher amounts during business hours
    base = 50 if 9 <= hour <= 20 else 20
    amount = round(random.expovariate(1 / base) + 1, 2)
    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": account_id,
        "amount": amount,
        "currency": "USD",
        "merchant": merchant,
        "merchant_category": category,
        "city": city,
        "latitude": lat + random.uniform(-0.05, 0.05),
        "longitude": lon + random.uniform(-0.05, 0.05),
        "card_present": random.random() > 0.15,
        "transaction_type": "debit",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def anomalous_transaction(account_id: str) -> dict:
    tx = normal_transaction(account_id)
    anomaly = random.choice([
        "high_amount",
        "odd_hours",
        "round_number",
        "card_not_present_high",
        "international",
        "suspicious_merchant",
    ])

    if anomaly == "high_amount":
        tx["amount"] = round(random.uniform(8000, 50000), 2)

    elif anomaly == "odd_hours":
        # Override timestamp to 2–5 AM UTC
        now = datetime.now(timezone.utc)
        odd = now.replace(hour=random.randint(2, 4), minute=random.randint(0, 59))
        tx["timestamp"] = odd.isoformat()
        tx["amount"] = round(random.uniform(500, 5000), 2)

    elif anomaly == "round_number":
        tx["amount"] = float(random.choice([500, 1000, 2000, 5000, 10000]))

    elif anomaly == "card_not_present_high":
        tx["card_present"] = False
        tx["amount"] = round(random.uniform(2000, 20000), 2)

    elif anomaly == "international":
        city, lat, lon = random.choice(CITIES[10:])
        tx["city"] = city
        tx["latitude"] = lat
        tx["longitude"] = lon
        tx["amount"] = round(random.uniform(1000, 8000), 2)

    elif anomaly == "suspicious_merchant":
        merchant, category = random.choice(MERCHANTS[14:])
        tx["merchant"] = merchant
        tx["merchant_category"] = category
        tx["amount"] = round(random.uniform(500, 5000), 2)

    return tx


def main():
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "queue.buffering.max.ms": 50,
    }
    producer = Producer(conf)
    delay = 1.0 / TPS
    print(f"[producer] streaming to {TOPIC} @ {TPS} tx/s (bootstrap={KAFKA_BOOTSTRAP})")

    burst_counter: dict[str, int] = {}

    while True:
        account_id = random.choice(ACCOUNTS)
        is_anomalous = account_id in ANOMALOUS_ACCOUNTS and random.random() < 0.10

        # Velocity burst: occasionally flood same account
        if random.random() < 0.005:
            burst_counter[account_id] = 8

        if burst_counter.get(account_id, 0) > 0:
            tx = normal_transaction(account_id)
            tx["amount"] = round(random.uniform(50, 300), 2)
            burst_counter[account_id] -= 1
        elif is_anomalous:
            tx = anomalous_transaction(account_id)
        else:
            tx = normal_transaction(account_id)

        payload = json.dumps(tx).encode()
        producer.produce(TOPIC, key=account_id.encode(), value=payload, callback=delivery_report)
        producer.poll(0)
        time.sleep(delay + random.uniform(-delay * 0.3, delay * 0.3))


if __name__ == "__main__":
    main()
