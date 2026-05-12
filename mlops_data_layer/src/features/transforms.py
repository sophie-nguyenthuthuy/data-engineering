from __future__ import annotations
import abc
import time
from typing import Any, Callable
import numpy as np
import pandas as pd
import structlog

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Base step
# ---------------------------------------------------------------------------

class TransformStep(abc.ABC):
    """A single, composable transformation applied to a DataFrame."""

    name: str = "base_step"

    @abc.abstractmethod
    def fit(self, df: pd.DataFrame) -> "TransformStep":
        """Learn parameters from training data. Returns self."""

    @abc.abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply the transformation. Must not mutate *df* in-place."""

    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.fit(df).transform(df)


# ---------------------------------------------------------------------------
# Concrete steps
# ---------------------------------------------------------------------------

class DropNullRows(TransformStep):
    """Drop rows where any of the specified columns is null."""
    name = "drop_null_rows"

    def __init__(self, columns: list[str] | None = None) -> None:
        self.columns = columns

    def fit(self, df: pd.DataFrame) -> "DropNullRows":
        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        cols = self.columns or df.columns.tolist()
        return df.dropna(subset=cols).reset_index(drop=True)


class FillMissing(TransformStep):
    """Fill nulls — numerical with mean, categorical with mode."""
    name = "fill_missing"

    def __init__(self, strategy: str = "auto") -> None:
        self.strategy = strategy  # auto | mean | median | mode | constant
        self._fill_values: dict[str, Any] = {}

    def fit(self, df: pd.DataFrame) -> "FillMissing":
        for col in df.columns:
            if df[col].isna().any():
                if pd.api.types.is_numeric_dtype(df[col]):
                    if self.strategy in ("auto", "mean"):
                        self._fill_values[col] = df[col].mean()
                    elif self.strategy == "median":
                        self._fill_values[col] = df[col].median()
                else:
                    mode = df[col].mode()
                    self._fill_values[col] = mode[0] if len(mode) else "UNKNOWN"
        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.fillna(self._fill_values)


class MinMaxScaler(TransformStep):
    """Scale numerical columns to [0, 1]."""
    name = "min_max_scaler"

    def __init__(self, columns: list[str] | None = None) -> None:
        self.columns = columns
        self._min: dict[str, float] = {}
        self._max: dict[str, float] = {}

    def fit(self, df: pd.DataFrame) -> "MinMaxScaler":
        cols = self.columns or df.select_dtypes(include="number").columns.tolist()
        for col in cols:
            self._min[col] = float(df[col].min())
            self._max[col] = float(df[col].max())
        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for col, mn in self._min.items():
            mx = self._max[col]
            rng = mx - mn
            out[col] = (out[col] - mn) / rng if rng > 0 else 0.0
        return out


class StandardScaler(TransformStep):
    """Z-score normalise numerical columns."""
    name = "standard_scaler"

    def __init__(self, columns: list[str] | None = None) -> None:
        self.columns = columns
        self._mean: dict[str, float] = {}
        self._std: dict[str, float] = {}

    def fit(self, df: pd.DataFrame) -> "StandardScaler":
        cols = self.columns or df.select_dtypes(include="number").columns.tolist()
        for col in cols:
            self._mean[col] = float(df[col].mean())
            self._std[col] = float(df[col].std())
        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for col, mean in self._mean.items():
            std = self._std[col]
            out[col] = (out[col] - mean) / std if std > 0 else 0.0
        return out


class OrdinalEncoder(TransformStep):
    """Encode categorical columns as integer ordinals."""
    name = "ordinal_encoder"

    def __init__(self, columns: list[str] | None = None) -> None:
        self.columns = columns
        self._maps: dict[str, dict[str, int]] = {}

    def fit(self, df: pd.DataFrame) -> "OrdinalEncoder":
        cols = self.columns or df.select_dtypes(include="object").columns.tolist()
        for col in cols:
            unique = sorted(df[col].dropna().astype(str).unique())
            self._maps[col] = {v: i for i, v in enumerate(unique)}
        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for col, mapping in self._maps.items():
            out[col] = out[col].astype(str).map(mapping).fillna(-1).astype(int)
        return out


class ClipOutliers(TransformStep):
    """Clip numerical values to [p_low, p_high] percentiles learned from training."""
    name = "clip_outliers"

    def __init__(
        self,
        columns: list[str] | None = None,
        p_low: float = 1.0,
        p_high: float = 99.0,
    ) -> None:
        self.columns = columns
        self.p_low = p_low
        self.p_high = p_high
        self._lo: dict[str, float] = {}
        self._hi: dict[str, float] = {}

    def fit(self, df: pd.DataFrame) -> "ClipOutliers":
        cols = self.columns or df.select_dtypes(include="number").columns.tolist()
        for col in cols:
            self._lo[col] = float(np.percentile(df[col].dropna(), self.p_low))
            self._hi[col] = float(np.percentile(df[col].dropna(), self.p_high))
        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for col in self._lo:
            out[col] = out[col].clip(lower=self._lo[col], upper=self._hi[col])
        return out


class LambdaStep(TransformStep):
    """Wrap a plain function as a pipeline step."""
    name = "lambda_step"

    def __init__(self, fn: Callable[[pd.DataFrame], pd.DataFrame], step_name: str = "lambda") -> None:
        self._fn = fn
        self.name = step_name

    def fit(self, df: pd.DataFrame) -> "LambdaStep":
        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._fn(df)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

class TransformPipeline:
    """
    Ordered sequence of TransformSteps. Supports fit / transform / fit_transform.
    Records per-step latency and row counts for observability.
    """

    def __init__(self, steps: list[TransformStep]) -> None:
        self.steps = steps
        self._fitted = False

    def fit(self, df: pd.DataFrame) -> "TransformPipeline":
        current = df
        for step in self.steps:
            t0 = time.perf_counter()
            step.fit(current)
            current = step.transform(current)
            log.debug("transform_fit", step=step.name, rows=len(current),
                      ms=f"{(time.perf_counter()-t0)*1000:.1f}")
        self._fitted = True
        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self._fitted:
            raise RuntimeError("Pipeline must be fit before transform")
        current = df.copy()
        for step in self.steps:
            t0 = time.perf_counter()
            rows_before = len(current)
            current = step.transform(current)
            log.debug(
                "transform_applied",
                step=step.name,
                rows_in=rows_before,
                rows_out=len(current),
                ms=f"{(time.perf_counter()-t0)*1000:.1f}",
            )
        return current

    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.fit(df).transform(df)

    def __repr__(self) -> str:
        return f"TransformPipeline([{', '.join(s.name for s in self.steps)}])"
