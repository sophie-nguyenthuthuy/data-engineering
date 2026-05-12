from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Postgres
    postgres_dsn: str = "postgresql://pipeline:pipeline@localhost:5432/payments"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_payment_topic: str = "payments.events"
    kafka_dlq_topic: str = "payments.dlq"
    kafka_consumer_group_warehouse: str = "warehouse-consumer"
    kafka_consumer_group_notification: str = "notification-consumer"
    kafka_transactional_id: str = "outbox-relay-1"

    # Redis (notification queue)
    redis_url: str = "redis://localhost:6379/0"

    # Outbox poller
    outbox_poll_interval_ms: int = 500
    outbox_batch_size: int = 50
    outbox_max_retries: int = 5

    # Failure injection (for demo)
    inject_failure_step: str = ""   # e.g. "kafka", "warehouse", "notification"
    inject_failure_rate: float = 0.3


settings = Settings()
