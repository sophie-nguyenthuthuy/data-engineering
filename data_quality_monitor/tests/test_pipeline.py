"""Integration-style tests for MicroBatchProcessor (all I/O mocked)."""
from __future__ import annotations
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import pandas as pd

from src.models import (
    MicroBatch,
    BatchMetadata,
    ValidationResult,
    ValidationStatus,
    ValidatorBackend,
)
from src.pipeline.micro_batch_processor import MicroBatchProcessor


def _make_result(status: ValidationStatus) -> ValidationResult:
    return ValidationResult(
        result_id="r1",
        batch_id="b1",
        table_name="orders",
        backend=ValidatorBackend.GREAT_EXPECTATIONS,
        suite_name="suite",
        status=status,
        pass_rate=1.0 if status == ValidationStatus.PASSED else 0.4,
        total_checks=5,
        passed_checks=5 if status == ValidationStatus.PASSED else 2,
        failed_checks=0 if status == ValidationStatus.PASSED else 3,
        warning_checks=0,
        row_count=10,
        duration_ms=20.0,
    )


@pytest.fixture
def sample_batch() -> MicroBatch:
    df = pd.DataFrame({
        "order_id": [f"ORD-{i}" for i in range(10)],
        "amount": [float(i + 1) for i in range(10)],
        "status": ["confirmed"] * 10,
    })
    return MicroBatch.from_dataframe(
        df, BatchMetadata(source="test", table_name="orders")
    )


@pytest.fixture
def processor() -> MicroBatchProcessor:
    repo = AsyncMock()
    producer = AsyncMock()
    collector = MagicMock()
    publisher = AsyncMock()
    job_ctrl = AsyncMock()
    job_ctrl.apply_result = AsyncMock(return_value=[])

    proc = MicroBatchProcessor.__new__(MicroBatchProcessor)
    proc._repo = repo
    proc._producer = producer
    proc._collector = collector
    proc._publisher = publisher
    proc._job_controller = job_ctrl
    proc._validators = []
    return proc


class TestMicroBatchProcessor:
    @pytest.mark.asyncio
    async def test_process_passed(self, processor, sample_batch):
        passed_result = _make_result(ValidationStatus.PASSED)
        mock_validator = AsyncMock()
        mock_validator.validate = AsyncMock(return_value=passed_result)
        processor._validators = [mock_validator]

        result = await processor.process(sample_batch)

        assert result.status == ValidationStatus.PASSED
        processor._repo.save_result.assert_called_once()
        processor._producer.publish_result.assert_called_once()
        processor._publisher.publish_result.assert_called_once()
        processor._collector.record.assert_called_once()
        processor._job_controller.apply_result.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_failed_triggers_block(self, processor, sample_batch):
        failed_result = _make_result(ValidationStatus.FAILED)
        mock_validator = AsyncMock()
        mock_validator.validate = AsyncMock(return_value=failed_result)
        processor._validators = [mock_validator]
        processor._job_controller.apply_result = AsyncMock(
            return_value=["etl_transform", "ml_feature_pipeline"]
        )

        result = await processor.process(sample_batch)

        assert result.status == ValidationStatus.FAILED
        processor._job_controller.apply_result.assert_called_once_with(failed_result)

    @pytest.mark.asyncio
    async def test_merge_worst_status_wins(self, processor, sample_batch):
        r_pass = _make_result(ValidationStatus.PASSED)
        r_fail = _make_result(ValidationStatus.FAILED)

        v1 = AsyncMock()
        v1.validate = AsyncMock(return_value=r_pass)
        v2 = AsyncMock()
        v2.validate = AsyncMock(return_value=r_fail)
        processor._validators = [v1, v2]

        result = await processor.process(sample_batch)
        assert result.status == ValidationStatus.FAILED

    @pytest.mark.asyncio
    async def test_all_validators_fail_returns_error(self, processor, sample_batch):
        v = AsyncMock()
        v.validate = AsyncMock(side_effect=RuntimeError("validator down"))
        processor._validators = [v]

        result = await processor.process(sample_batch)
        assert result.status == ValidationStatus.ERROR
        assert result.error_message is not None

    def test_merge_single_result_passthrough(self, processor, sample_batch):
        r = _make_result(ValidationStatus.PASSED)
        merged = processor._merge_results(sample_batch, [r])
        assert merged is r

    def test_merge_empty_returns_error(self, processor, sample_batch):
        merged = processor._merge_results(sample_batch, [RuntimeError("oops")])
        assert merged.status == ValidationStatus.ERROR
