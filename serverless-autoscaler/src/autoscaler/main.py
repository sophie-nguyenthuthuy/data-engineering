from __future__ import annotations

import logging
import sys

from .config import AppConfig
from .cost_tracker import CostTracker
from .hpa_client import HPAClient
from .metrics_store import MetricsStore
from .predictor import ARIMAPredictor
from .scheduler import PredictiveScheduler
from .telemetry import Telemetry


def build_app(cfg: AppConfig) -> PredictiveScheduler:
    store = MetricsStore(cfg.metrics_store)
    hpa = HPAClient(cfg.hpa)
    predictor = ARIMAPredictor(cfg.predictor)
    telemetry = Telemetry(cfg.prometheus_port)
    cost = CostTracker(cfg.cost, store)
    return PredictiveScheduler(
        cfg=cfg.scheduler,
        predictor=predictor,
        hpa=hpa,
        store=store,
        cost=cost,
        telemetry=telemetry,
    )


def main() -> None:
    cfg = AppConfig.from_env()
    logging.basicConfig(
        level=cfg.log_level,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        stream=sys.stdout,
    )
    scheduler = build_app(cfg)
    scheduler.run_forever()


if __name__ == "__main__":
    main()
