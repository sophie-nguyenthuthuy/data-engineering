import pytest
from unittest.mock import MagicMock, patch
from src.models import PipelineConfig, RunResult
from src.pipeline import run_pipeline


def _make_config(source_type="postgres", dest_type="slack"):
    return PipelineConfig(
        name="test_pipe",
        source={"type": source_type, "query": "SELECT 1"},
        destination={
            "type": dest_type,
            "params": {"channel": "#test"},
        },
    )


def test_run_pipeline_success():
    config = _make_config()
    mock_source = MagicMock()
    mock_source.fetch.return_value = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    mock_dest = MagicMock()
    mock_dest.send.return_value = 2

    with (
        patch("src.pipeline.SOURCE_REGISTRY", {"postgres": lambda p: mock_source}),
        patch("src.pipeline.DESTINATION_REGISTRY", {"slack": lambda p: mock_dest}),
    ):
        result = run_pipeline(config)

    assert result.success is True
    assert result.rows_extracted == 2
    assert result.rows_synced == 2


def test_run_pipeline_source_error():
    config = _make_config()
    mock_source = MagicMock()
    mock_source.fetch.side_effect = Exception("connection refused")

    with (
        patch("src.pipeline.SOURCE_REGISTRY", {"postgres": lambda p: mock_source}),
        patch("src.pipeline.DESTINATION_REGISTRY", {}),
    ):
        result = run_pipeline(config)

    assert result.success is False
    assert "connection refused" in result.error


def test_run_pipeline_unknown_source():
    config = _make_config(source_type="unknown_db")
    with (
        patch("src.pipeline.SOURCE_REGISTRY", {}),
        patch("src.pipeline.DESTINATION_REGISTRY", {}),
    ):
        result = run_pipeline(config)

    assert result.success is False
    assert "unknown_db" in result.error


def test_run_pipeline_zero_rows():
    config = _make_config()
    mock_source = MagicMock()
    mock_source.fetch.return_value = []
    mock_dest = MagicMock()
    mock_dest.send.return_value = 0

    with (
        patch("src.pipeline.SOURCE_REGISTRY", {"postgres": lambda p: mock_source}),
        patch("src.pipeline.DESTINATION_REGISTRY", {"slack": lambda p: mock_dest}),
    ):
        result = run_pipeline(config)

    assert result.success is True
    assert result.rows_extracted == 0
    assert result.rows_synced == 0
