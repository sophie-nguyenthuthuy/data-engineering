from __future__ import annotations

import logging
import warnings
from datetime import datetime
from typing import Optional
from uuid import uuid4

import numpy as np

from .config import PredictorConfig
from .models import JobRun, ResourceForecast

logger = logging.getLogger(__name__)

# statsmodels raises convergence warnings we handle gracefully
warnings.filterwarnings("ignore", category=UserWarning, module="statsmodels")


class ARIMAPredictor:
    """
    Fits a SARIMA model on historical peak_workers observations for a job
    and forecasts the resource demand for the next scheduled run.

    Falls back to a rolling-percentile estimate when history is too sparse
    or model fitting fails.
    """

    def __init__(self, cfg: PredictorConfig) -> None:
        self._cfg = cfg

    def forecast(
        self,
        job_id: str,
        history: list[JobRun],
        target_start: datetime,
    ) -> Optional[ResourceForecast]:
        completed = [r for r in history if r.peak_workers is not None]
        if len(completed) < self._cfg.min_history_points:
            logger.warning(
                "job=%s only %d history points, need %d — using percentile fallback",
                job_id,
                len(completed),
                self._cfg.min_history_points,
            )
            return self._percentile_forecast(job_id, completed, target_start)

        series = np.array([float(r.peak_workers) for r in completed], dtype=float)
        return self._arima_forecast(job_id, series, completed, target_start)

    # ------------------------------------------------------------------ #
    #  ARIMA path                                                          #
    # ------------------------------------------------------------------ #

    def _arima_forecast(
        self,
        job_id: str,
        series: np.ndarray,
        completed: list[JobRun],
        target_start: datetime,
    ) -> Optional[ResourceForecast]:
        try:
            from statsmodels.tsa.statespace.sarimax import SARIMAX

            order = self._cfg.arima_order
            seasonal = self._cfg.seasonal_order
            # Only apply seasonality if we have enough observations
            if len(series) < seasonal[3] * 2:
                seasonal = (0, 0, 0, 0)

            model = SARIMAX(
                series,
                order=order,
                seasonal_order=seasonal,
                enforce_stationarity=False,
                enforce_invertibility=False,
            )
            fit = model.fit(disp=False, maxiter=200)
            forecast_result = fit.get_forecast(steps=1)
            mean = float(forecast_result.predicted_mean.iloc[0])
            ci = forecast_result.conf_int(alpha=1 - self._cfg.confidence_interval)
            lower = max(1, int(ci.iloc[0, 0]))
            upper = int(ci.iloc[0, 1])

            predicted_workers = max(1, int(mean * self._cfg.safety_factor))
            predicted_cpu, predicted_mem = self._extrapolate_resources(
                completed, predicted_workers
            )

            logger.info(
                "job=%s ARIMA forecast workers=%d [%d–%d] AIC=%.1f",
                job_id, predicted_workers, lower, upper, fit.aic,
            )
            return ResourceForecast(
                job_id=job_id,
                forecast_at=datetime.utcnow(),
                target_start=target_start,
                predicted_peak_workers=predicted_workers,
                predicted_peak_cpu_millicores=predicted_cpu,
                predicted_peak_memory_mib=predicted_mem,
                confidence_lower=lower,
                confidence_upper=upper,
                history_points_used=len(completed),
                model_aic=fit.aic,
            )

        except Exception:
            logger.exception("ARIMA fitting failed for job=%s, falling back", job_id)
            return self._percentile_forecast(job_id, completed, target_start)

    # ------------------------------------------------------------------ #
    #  Percentile fallback                                                 #
    # ------------------------------------------------------------------ #

    def _percentile_forecast(
        self,
        job_id: str,
        completed: list[JobRun],
        target_start: datetime,
    ) -> Optional[ResourceForecast]:
        if not completed:
            logger.warning("job=%s no completed runs at all — cannot forecast", job_id)
            return None

        workers = [r.peak_workers for r in completed if r.peak_workers]
        if not workers:
            return None

        p95 = float(np.percentile(workers, 95))
        predicted = max(1, int(p95 * self._cfg.safety_factor))
        predicted_cpu, predicted_mem = self._extrapolate_resources(completed, predicted)

        logger.info("job=%s percentile-fallback workers=%d", job_id, predicted)
        return ResourceForecast(
            job_id=job_id,
            forecast_at=datetime.utcnow(),
            target_start=target_start,
            predicted_peak_workers=predicted,
            predicted_peak_cpu_millicores=predicted_cpu,
            predicted_peak_memory_mib=predicted_mem,
            confidence_lower=max(1, predicted - 2),
            confidence_upper=predicted + 2,
            history_points_used=len(completed),
        )

    # ------------------------------------------------------------------ #
    #  Resource extrapolation                                              #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _extrapolate_resources(
        completed: list[JobRun], target_workers: int
    ) -> tuple[float, float]:
        """Linear extrapolation of CPU/memory from historical peak_workers ratios."""
        cpu_per_worker = [
            r.peak_cpu_millicores / r.peak_workers
            for r in completed
            if r.peak_cpu_millicores and r.peak_workers
        ]
        mem_per_worker = [
            r.peak_memory_mib / r.peak_workers
            for r in completed
            if r.peak_memory_mib and r.peak_workers
        ]

        avg_cpu = float(np.mean(cpu_per_worker)) if cpu_per_worker else 1000.0
        avg_mem = float(np.mean(mem_per_worker)) if mem_per_worker else 2048.0

        return avg_cpu * target_workers, avg_mem * target_workers
