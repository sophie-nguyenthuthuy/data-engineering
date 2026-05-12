"""Probabilistic watermarks for out-of-order stream processing.

Public API:
    from pwm import PerKeyDelayEstimator, WatermarkAdvancer, CorrectionStream
"""

from __future__ import annotations

__version__ = "0.1.0"

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from pwm.correction.stream import CorrectionStream
    from pwm.sketch.tdigest import TDigest
    from pwm.watermark.advancer import WatermarkAdvancer
    from pwm.watermark.estimator import PerKeyDelayEstimator

_LAZY: dict[str, str] = {
    "TDigest": "pwm.sketch.tdigest",
    "PerKeyDelayEstimator": "pwm.watermark.estimator",
    "WatermarkAdvancer": "pwm.watermark.advancer",
    "CorrectionStream": "pwm.correction.stream",
}


def __getattr__(name: str) -> Any:
    mod = _LAZY.get(name)
    if mod is None:
        raise AttributeError(f"module 'pwm' has no attribute {name!r}")
    import importlib

    return getattr(importlib.import_module(mod), name)


__all__ = ["CorrectionStream", "PerKeyDelayEstimator", "TDigest", "WatermarkAdvancer"]
