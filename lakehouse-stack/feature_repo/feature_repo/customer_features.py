# Lab 16 — Feature Store Fundamentals with Feast
# Lab Step 1: Feature Definition
# Features: customer_order_count, avg_order_value (from Silver/offline lakehouse table)

from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field, FileSource
from feast.types import Float64, Int64

# Entity: the primary key used to look up features
customer = Entity(name="customer", join_keys=["customer_id"])

# Offline source: parquet file representing the Silver-layer aggregated orders table
customer_orders_source = FileSource(
    name="customer_orders_source",
    path="data/customer_orders.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Feature View: registers customer_order_count and avg_order_value
customer_stats_fv = FeatureView(
    name="customer_order_stats",
    entities=[customer],
    ttl=timedelta(days=14),  # Homework: tuned TTL from 7 → 14 days
    schema=[
        Field(name="customer_order_count", dtype=Int64,
              description="Total number of orders placed by a customer"),
        Field(name="avg_order_value", dtype=Float64,
              description="Average monetary value of customer orders"),
        Field(name="total_revenue", dtype=Float64,
              description="Homework: total revenue = order_count × avg_order_value"),
    ],
    online=True,
    source=customer_orders_source,
    tags={"team": "lakehouse", "layer": "silver"},
)

# Feature Service: groups features for model consumption
customer_activity_svc = FeatureService(
    name="customer_activity_v1",
    features=[customer_stats_fv],
)
