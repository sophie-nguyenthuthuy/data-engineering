from .base import BaseTarget, CompiledArtifact
from .spark_target import SparkTarget
from .flink_target import FlinkTarget
from .dbt_target import DbtTarget
from ..compiler.selector import Target


def get_target(target: Target) -> BaseTarget:
    return {
        Target.SPARK: SparkTarget,
        Target.FLINK: FlinkTarget,
        Target.DBT: DbtTarget,
    }[target]()


__all__ = ["BaseTarget", "CompiledArtifact", "SparkTarget", "FlinkTarget", "DbtTarget", "get_target"]
