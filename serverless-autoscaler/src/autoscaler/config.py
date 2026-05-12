from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PredictorConfig:
    arima_order: tuple[int, int, int] = (2, 1, 2)
    seasonal_order: tuple[int, int, int, int] = (1, 1, 1, 12)
    min_history_points: int = 10
    forecast_horizon_minutes: int = 30
    confidence_interval: float = 0.95
    # Scale up by this factor above predicted p95 to be safe
    safety_factor: float = 1.15


@dataclass
class SchedulerConfig:
    prewarm_lead_time_seconds: int = 300   # 5 min before scheduled start
    poll_interval_seconds: int = 30
    job_registry_path: str = "/etc/autoscaler/jobs.yaml"


@dataclass
class HPAConfig:
    kubeconfig_path: Optional[str] = None   # None = in-cluster
    namespace: str = "default"
    scale_down_cooldown_seconds: int = 300
    scale_up_cooldown_seconds: int = 60
    min_replicas_floor: int = 1
    max_replicas_ceiling: int = 200


@dataclass
class MetricsStoreConfig:
    db_url: str = "sqlite:///autoscaler_metrics.db"
    retention_days: int = 90


@dataclass
class CostConfig:
    cold_start_seconds: float = 120.0        # avg worker cold-start time
    worker_cost_per_hour: float = 0.096      # USD, e.g. m5.xlarge spot
    idle_prewarm_cost_factor: float = 0.25   # fraction of time workers idle during prewarm


@dataclass
class AppConfig:
    predictor: PredictorConfig = field(default_factory=PredictorConfig)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    hpa: HPAConfig = field(default_factory=HPAConfig)
    metrics_store: MetricsStoreConfig = field(default_factory=MetricsStoreConfig)
    cost: CostConfig = field(default_factory=CostConfig)
    prometheus_port: int = 9090
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> AppConfig:
        cfg = cls()
        cfg.hpa.namespace = os.getenv("AUTOSCALER_NAMESPACE", cfg.hpa.namespace)
        cfg.hpa.kubeconfig_path = os.getenv("KUBECONFIG", cfg.hpa.kubeconfig_path)
        cfg.metrics_store.db_url = os.getenv("METRICS_DB_URL", cfg.metrics_store.db_url)
        cfg.scheduler.prewarm_lead_time_seconds = int(
            os.getenv("PREWARM_LEAD_TIME_SECONDS", cfg.scheduler.prewarm_lead_time_seconds)
        )
        cfg.prometheus_port = int(os.getenv("PROMETHEUS_PORT", cfg.prometheus_port))
        cfg.log_level = os.getenv("LOG_LEVEL", cfg.log_level)
        return cfg
