from __future__ import annotations
import time
import uuid
from datetime import datetime

import pandas as pd
import structlog

from ..config import settings
from ..models import (
    PipelineRun,
    PipelineStatus,
    StepResult,
    TrainingSnapshot,
    FeatureVector,
    BatchMetadata,
)
from .registry import FeatureRegistry
from .store import FeatureStore
from .transforms import TransformPipeline, FillMissing, ClipOutliers, StandardScaler

log = structlog.get_logger(__name__)

# Alias so models.pipeline doesn't need BatchMetadata
from ..models.feature import FeatureVector  # noqa: F811


class FeatureEngineeringPipeline:
    """
    Orchestrates the full feature engineering workflow:

    1. Validate raw input against the feature registry schema
    2. Apply a configurable TransformPipeline (fill → clip → scale → encode)
    3. Materialise FeatureVectors into the online store
    4. Capture a TrainingSnapshot for use as drift reference
    5. Return a PipelineRun record with per-step telemetry
    """

    def __init__(
        self,
        model_name: str,
        registry: FeatureRegistry,
        store: FeatureStore,
        transform_pipeline: TransformPipeline | None = None,
    ) -> None:
        self._model_name = model_name
        self._registry = registry
        self._store = store
        self._pipeline = transform_pipeline or _default_pipeline()

    # ------------------------------------------------------------------
    # Training path — fit + snapshot
    # ------------------------------------------------------------------

    async def run_training(
        self,
        df: pd.DataFrame,
        model_version: str,
        entity_id_col: str = "entity_id",
    ) -> tuple[pd.DataFrame, TrainingSnapshot, PipelineRun]:
        run = PipelineRun(
            pipeline_name="feature_engineering_training",
            model_name=self._model_name,
            triggered_by="training",
            status=PipelineStatus.RUNNING,
            input_rows=len(df),
        )
        run_start = time.perf_counter()
        step_results: list[StepResult] = []

        try:
            # Step 1: schema validation
            df, step = await self._step_validate(df, "schema_validation")
            step_results.append(step)

            # Step 2: fit + transform
            df, step = await self._step_fit_transform(df, "fit_transform")
            step_results.append(step)

            # Step 3: capture training snapshot
            stats = self._store.compute_stats(df)
            snapshot = TrainingSnapshot(
                model_name=self._model_name,
                model_version=model_version,
                row_count=len(df),
                feature_stats=stats,
            )
            await self._store.save_training_snapshot(snapshot)
            step_results.append(StepResult(
                step_name="capture_snapshot",
                status=PipelineStatus.COMPLETED,
                rows_in=len(df),
                rows_out=len(df),
                metrics={"features_captured": len(stats)},
            ))

            # Step 4: materialise vectors
            step = await self._step_materialise(df, entity_id_col)
            step_results.append(step)

            run.status = PipelineStatus.COMPLETED
            run.output_rows = len(df)
            run.artifacts["snapshot_id"] = snapshot.snapshot_id

        except Exception as exc:
            run.status = PipelineStatus.FAILED
            run.error_message = str(exc)
            log.error("feature_pipeline_failed", model=self._model_name, error=str(exc))
            raise

        finally:
            run.finished_at = datetime.utcnow()
            run.duration_ms = (time.perf_counter() - run_start) * 1000
            run.step_results = step_results

        log.info(
            "training_pipeline_complete",
            model=self._model_name,
            rows=run.output_rows,
            duration_ms=f"{run.duration_ms:.0f}",
        )
        return df, snapshot, run

    # ------------------------------------------------------------------
    # Serving path — transform only (no fit, no snapshot)
    # ------------------------------------------------------------------

    async def run_serving(
        self,
        df: pd.DataFrame,
        entity_id_col: str = "entity_id",
    ) -> tuple[pd.DataFrame, PipelineRun]:
        run = PipelineRun(
            pipeline_name="feature_engineering_serving",
            model_name=self._model_name,
            triggered_by="serving",
            status=PipelineStatus.RUNNING,
            input_rows=len(df),
        )
        run_start = time.perf_counter()
        step_results: list[StepResult] = []

        try:
            df, step = await self._step_validate(df, "schema_validation")
            step_results.append(step)

            df, step = await self._step_transform(df, "transform")
            step_results.append(step)

            step = await self._step_materialise(df, entity_id_col)
            step_results.append(step)

            run.status = PipelineStatus.COMPLETED
            run.output_rows = len(df)

        except Exception as exc:
            run.status = PipelineStatus.FAILED
            run.error_message = str(exc)
            raise

        finally:
            run.finished_at = datetime.utcnow()
            run.duration_ms = (time.perf_counter() - run_start) * 1000
            run.step_results = step_results

        return df, run

    # ------------------------------------------------------------------
    # Private steps
    # ------------------------------------------------------------------

    async def _step_validate(
        self, df: pd.DataFrame, name: str
    ) -> tuple[pd.DataFrame, StepResult]:
        t0 = time.perf_counter()
        issues: list[str] = []
        for fd in self._registry:
            if fd.name not in df.columns:
                log.warning("feature_missing_from_input", feature=fd.name)
            else:
                if not fd.nullable and df[fd.name].isna().any():
                    issues.append(f"{fd.name}: unexpected nulls")
        return df, StepResult(
            step_name=name,
            status=PipelineStatus.COMPLETED,
            duration_ms=(time.perf_counter() - t0) * 1000,
            rows_in=len(df),
            rows_out=len(df),
            metrics={"issues": issues},
        )

    async def _step_fit_transform(
        self, df: pd.DataFrame, name: str
    ) -> tuple[pd.DataFrame, StepResult]:
        t0 = time.perf_counter()
        out = self._pipeline.fit_transform(df)
        return out, StepResult(
            step_name=name,
            status=PipelineStatus.COMPLETED,
            duration_ms=(time.perf_counter() - t0) * 1000,
            rows_in=len(df),
            rows_out=len(out),
        )

    async def _step_transform(
        self, df: pd.DataFrame, name: str
    ) -> tuple[pd.DataFrame, StepResult]:
        t0 = time.perf_counter()
        out = self._pipeline.transform(df)
        return out, StepResult(
            step_name=name,
            status=PipelineStatus.COMPLETED,
            duration_ms=(time.perf_counter() - t0) * 1000,
            rows_in=len(df),
            rows_out=len(out),
        )

    async def _step_materialise(
        self, df: pd.DataFrame, entity_id_col: str
    ) -> StepResult:
        t0 = time.perf_counter()
        count = 0
        for _, row in df.iterrows():
            entity_id = str(row.get(entity_id_col, uuid.uuid4()))
            features = {k: v for k, v in row.items() if k != entity_id_col}
            vec = FeatureVector(
                entity_id=entity_id,
                model_name=self._model_name,
                features=features,
            )
            await self._store.write_vector(vec)
            count += 1
        return StepResult(
            step_name="materialise_vectors",
            status=PipelineStatus.COMPLETED,
            duration_ms=(time.perf_counter() - t0) * 1000,
            rows_in=len(df),
            rows_out=count,
            metrics={"vectors_written": count},
        )


def _default_pipeline() -> TransformPipeline:
    return TransformPipeline([
        FillMissing(strategy="auto"),
        ClipOutliers(p_low=1.0, p_high=99.0),
        StandardScaler(),
    ])
