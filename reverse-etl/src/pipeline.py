from pathlib import Path
import yaml
from .models import PipelineConfig, RunResult
from .sources import SOURCE_REGISTRY
from .destinations import DESTINATION_REGISTRY
from .transforms.mapper import FieldMapper
from .logger import get_logger

logger = get_logger(__name__)


def load_pipeline(path: str | Path) -> PipelineConfig:
    with open(path) as f:
        raw = yaml.safe_load(f)
    return PipelineConfig(**raw)


def load_all_pipelines(config_dir: str | Path) -> list[PipelineConfig]:
    config_dir = Path(config_dir)
    pipelines = []
    for f in sorted(config_dir.glob("*.yaml")):
        try:
            pipelines.append(load_pipeline(f))
        except Exception as e:
            logger.error(f"Failed to load pipeline {f.name}: {e}")
    return pipelines


def run_pipeline(config: PipelineConfig) -> RunResult:
    logger.info(f"[{config.name}] starting run")

    try:
        # 1. Extract
        source_cls = SOURCE_REGISTRY.get(config.source.type)
        if not source_cls:
            raise ValueError(f"Unknown source type: {config.source.type!r}")
        source = source_cls(config.source.params)
        records = source.fetch(config.source.query)
        rows_extracted = len(records)
        logger.info(f"[{config.name}] extracted {rows_extracted} rows from {config.source.type}")

        # 2. Transform
        mapper = FieldMapper(config.destination.field_mappings)
        records = mapper.apply(records)

        # 3. Load
        dest_cls = DESTINATION_REGISTRY.get(config.destination.type)
        if not dest_cls:
            raise ValueError(f"Unknown destination type: {config.destination.type!r}")
        dest = dest_cls(config.destination.params)
        rows_synced = dest.send(records)
        logger.info(f"[{config.name}] synced {rows_synced} rows to {config.destination.type}")

        return RunResult(
            pipeline=config.name,
            success=True,
            rows_extracted=rows_extracted,
            rows_synced=rows_synced,
        )

    except Exception as e:
        logger.error(f"[{config.name}] pipeline failed: {e}")
        return RunResult(pipeline=config.name, success=False, error=str(e))
