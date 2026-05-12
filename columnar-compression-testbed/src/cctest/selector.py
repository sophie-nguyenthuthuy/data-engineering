"""Per-column encoding selector.

For each column the selector:
  1. Samples up to ``sample_size`` rows.
  2. Benchmarks every codec that ``supports_dtype`` for that column's dtype.
  3. Commits to the codec with the best compression ratio (ties broken by
     encode speed).
  4. Caches the decision keyed on (column_name, dtype).
  5. On ``schema_changed`` the affected column entries are evicted so the
     next encode triggers a fresh evaluation.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

from .codecs import ALL_CODECS, Codec, BenchmarkResult

logger = logging.getLogger(__name__)


@dataclass
class SelectionRecord:
    column: str
    dtype: str
    codec: Codec
    benchmark: BenchmarkResult
    sample_n: int


@dataclass
class SelectorConfig:
    sample_size: int = 8_192
    min_ratio_improvement: float = 1.05  # new codec must be >= 5% better to unseat cached choice
    benchmark_rounds: int = 3


class EncodingSelector:
    def __init__(self, codecs: Optional[list[Codec]] = None, config: Optional[SelectorConfig] = None) -> None:
        self._codecs = codecs if codecs is not None else list(ALL_CODECS)
        self._cfg = config or SelectorConfig()
        self._cache: dict[tuple[str, str], SelectionRecord] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def select(self, name: str, column: np.ndarray) -> Codec:
        """Return the best codec for *column*, using cache when available."""
        key = (name, str(column.dtype))
        if key in self._cache:
            return self._cache[key].codec
        record = self._evaluate(name, column)
        self._cache[key] = record
        return record.codec

    def force_reevaluate(self, name: str, column: np.ndarray) -> Codec:
        """Discard cached choice and re-run benchmark."""
        key = (name, str(column.dtype))
        self._cache.pop(key, None)
        return self.select(name, column)

    def schema_changed(self, old_columns: dict[str, str], new_columns: dict[str, str]) -> list[str]:
        """
        Compare old vs new {name: dtype} schema maps.  Evict cache for any
        column whose dtype changed or that is new.  Returns affected names.
        """
        affected = []
        for name, new_dtype in new_columns.items():
            old_dtype = old_columns.get(name)
            if old_dtype != new_dtype:
                key = (name, old_dtype) if old_dtype else (name, new_dtype)
                self._cache.pop(key, None)
                self._cache.pop((name, new_dtype), None)
                affected.append(name)
                logger.info("Schema change: column %r %s → %s; cache evicted", name, old_dtype, new_dtype)
        return affected

    def cache_summary(self) -> list[SelectionRecord]:
        return list(self._cache.values())

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _candidates(self, dtype: np.dtype) -> list[Codec]:
        return [c for c in self._codecs if c.supports_dtype(dtype)]

    def _sample(self, column: np.ndarray) -> np.ndarray:
        n = min(self._cfg.sample_size, len(column))
        if n == len(column):
            return column
        rng = np.random.default_rng(0)
        idx = rng.choice(len(column), size=n, replace=False)
        idx.sort()
        return column[idx]

    def _evaluate(self, name: str, column: np.ndarray) -> SelectionRecord:
        candidates = self._candidates(column.dtype)
        if not candidates:
            raise ValueError(f"No codec supports dtype {column.dtype} for column {name!r}")

        sample = self._sample(column)
        results: list[tuple[BenchmarkResult, Codec]] = []

        for codec in candidates:
            try:
                bm = codec.benchmark(sample, rounds=self._cfg.benchmark_rounds)
                if bm.lossless:
                    results.append((bm, codec))
            except Exception as exc:
                logger.debug("Codec %s failed on column %r: %s", codec.name, name, exc)

        if not results:
            raise RuntimeError(f"All codecs failed for column {name!r} (dtype={column.dtype})")

        # Best = highest ratio; ties broken by fastest encode
        best_bm, best_codec = max(results, key=lambda t: (t[0].ratio, -t[0].encode_ms))

        logger.info(
            "Selected %s for column %r (ratio=%.2fx, saving=%.1f%%)",
            best_codec.name, name, best_bm.ratio, best_bm.space_saving * 100,
        )
        return SelectionRecord(
            column=name,
            dtype=str(column.dtype),
            codec=best_codec,
            benchmark=best_bm,
            sample_n=len(sample),
        )
