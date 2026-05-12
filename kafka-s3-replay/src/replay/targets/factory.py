"""Factory that instantiates the correct target from a ReplayConfig."""

from __future__ import annotations

from replay.models import ReplayConfig, TargetType
from replay.targets.base import BaseTarget


def make_target(config: ReplayConfig) -> BaseTarget:
    """Return the appropriate BaseTarget subclass for the given config."""
    match config.target_type:
        case TargetType.KAFKA:
            from replay.targets.kafka import KafkaTarget
            if not config.kafka_target:
                raise ValueError("kafka_target config is required for KAFKA target type")
            return KafkaTarget(config.kafka_target)

        case TargetType.HTTP:
            from replay.targets.http import HttpTarget
            if not config.http_target:
                raise ValueError("http_target config is required for HTTP target type")
            return HttpTarget(config.http_target)

        case TargetType.FILE:
            from replay.targets.file import FileTarget
            if not config.file_target:
                raise ValueError("file_target config is required for FILE target type")
            return FileTarget(config.file_target)

        case TargetType.STDOUT:
            from replay.targets.stdout import StdoutTarget
            return StdoutTarget()

        case _:
            raise ValueError(f"Unknown target type: {config.target_type}")
