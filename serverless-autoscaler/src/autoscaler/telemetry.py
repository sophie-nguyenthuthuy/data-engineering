from __future__ import annotations

import logging
import threading
from typing import Optional

logger = logging.getLogger(__name__)


class Telemetry:
    """
    Prometheus metrics for the autoscaler control plane.
    All counters/gauges/histograms are exposed on /metrics.
    """

    def __init__(self, port: int = 9090) -> None:
        try:
            from prometheus_client import (
                Counter,
                Gauge,
                Histogram,
                start_http_server,
            )

            self.prewarm_total = Counter(
                "autoscaler_prewarm_total",
                "Number of predictive prewarm operations triggered",
                ["job_id"],
            )
            self.scaling_actions = Counter(
                "autoscaler_scaling_actions_total",
                "HPA patches applied",
                ["job_id", "reason"],
            )
            self.active_jobs = Gauge(
                "autoscaler_active_jobs",
                "Currently running jobs being tracked",
                ["job_id"],
            )
            self.predicted_workers = Gauge(
                "autoscaler_predicted_workers",
                "ARIMA-predicted peak workers for next run",
                ["job_id"],
            )
            self.job_duration = Histogram(
                "autoscaler_job_duration_seconds",
                "Observed job wall-clock duration",
                ["job_id"],
                buckets=[60, 300, 600, 1800, 3600, 7200],
            )
            self.net_savings_usd = Gauge(
                "autoscaler_net_savings_usd_total",
                "Cumulative net cold-start cost savings in USD",
            )

            start_http_server(port)
            logger.info("Prometheus metrics server started on :%d", port)

        except ImportError:
            logger.warning("prometheus_client not installed — metrics disabled")
            self._install_stubs()

    def _install_stubs(self) -> None:
        """Replace all metrics with no-op stubs when prometheus_client is absent."""
        class _Stub:
            def labels(self, **_):
                return self
            def inc(self, *_, **__): pass
            def set(self, *_, **__): pass
            def observe(self, *_, **__): pass

        self.prewarm_total = _Stub()
        self.scaling_actions = _Stub()
        self.active_jobs = _Stub()
        self.predicted_workers = _Stub()
        self.job_duration = _Stub()
        self.net_savings_usd = _Stub()
