from .alp import ALPCodec
from .base import BenchmarkResult, Codec, EncodedColumn
from .fsst import FSSTCodec
from .gorilla import GorillaDeltaCodec, GorillaFloatCodec

__all__ = [
    "Codec",
    "EncodedColumn",
    "BenchmarkResult",
    "FSSTCodec",
    "ALPCodec",
    "GorillaFloatCodec",
    "GorillaDeltaCodec",
]

ALL_CODECS: list[Codec] = [
    FSSTCodec(),
    ALPCodec(),
    GorillaFloatCodec(),
    GorillaDeltaCodec(),
]
