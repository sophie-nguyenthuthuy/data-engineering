from __future__ import annotations
import structlog
from confluent_kafka import Producer

from ..config import settings
from ..models import DriftReport, RetrainingJob

log = structlog.get_logger(__name__)


class MLOpsEventProducer:
    """Publishes drift alerts and retraining commands to Kafka."""

    def __init__(self) -> None:
        self._producer = Producer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "acks": "all",
            "retries": 3,
        })

    async def publish_drift_alert(self, report: DriftReport) -> None:
        self._producer.produce(
            topic=settings.kafka_drift_topic,
            key=report.model_name.encode(),
            value=report.model_dump_json().encode(),
            on_delivery=self._cb,
        )
        self._producer.flush(timeout=5)
        log.info("drift_alert_published", model=report.model_name, status=report.overall_status)

    async def publish_retrain_command(self, job: RetrainingJob) -> None:
        self._producer.produce(
            topic=settings.kafka_retrain_topic,
            key=job.trigger.model_name.encode(),
            value=job.model_dump_json().encode(),
            on_delivery=self._cb,
        )
        self._producer.flush(timeout=5)
        log.info("retrain_command_published", job_id=job.job_id, model=job.trigger.model_name)

    def close(self) -> None:
        self._producer.flush()

    @staticmethod
    def _cb(err, msg):
        if err:
            log.error("kafka_delivery_error", error=str(err))
