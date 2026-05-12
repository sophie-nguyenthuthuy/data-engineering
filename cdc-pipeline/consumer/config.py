import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class Config:
    kafka_bootstrap_servers: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    schema_registry_url: str     = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    warehouse_dsn: str           = os.environ.get("WAREHOUSE_DSN", "postgresql://dw_user:dw_secret@localhost:5433/data_warehouse")
    kafka_group_id: str          = os.environ.get("KAFKA_GROUP_ID", "cdc-consumer-group")
    topics: List[str]            = field(default_factory=lambda: os.environ.get("TOPICS", "cdc.public.users,cdc.public.orders,cdc.public.order_items").split(","))
    lag_tolerance_ms: int        = int(os.environ.get("LAG_TOLERANCE_MS", "30000"))
    max_buffer_size: int         = int(os.environ.get("MAX_BUFFER_SIZE", "10000"))
    log_level: str               = os.environ.get("LOG_LEVEL", "INFO")
    # How often to flush the reorder buffer even without watermark advancement
    flush_interval_ms: int       = int(os.environ.get("FLUSH_INTERVAL_MS", "5000"))
    # Batch size for warehouse writes
    warehouse_batch_size: int    = int(os.environ.get("WAREHOUSE_BATCH_SIZE", "500"))


CONFIG = Config()
