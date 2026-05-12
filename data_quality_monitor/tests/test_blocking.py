"""Tests for the JobController downstream gate."""
from __future__ import annotations
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from src.models import ValidationResult, ValidationStatus, ValidatorBackend
from src.blocking.job_controller import JobController


def _make_result(status: ValidationStatus, table: str = "orders") -> ValidationResult:
    return ValidationResult(
        result_id="res-1",
        batch_id="batch-1",
        table_name=table,
        backend=ValidatorBackend.GREAT_EXPECTATIONS,
        suite_name="orders_default_suite",
        status=status,
        pass_rate=1.0 if status == ValidationStatus.PASSED else 0.5,
        total_checks=10,
        passed_checks=10 if status == ValidationStatus.PASSED else 5,
        failed_checks=0 if status == ValidationStatus.PASSED else 5,
        warning_checks=0,
        row_count=100,
        duration_ms=42.0,
    )


@pytest.fixture
def mock_redis():
    redis = AsyncMock()
    redis.exists = AsyncMock(return_value=0)
    redis.hset = AsyncMock()
    redis.expire = AsyncMock()
    redis.delete = AsyncMock(return_value=1)
    redis.keys = AsyncMock(return_value=[])
    redis.hgetall = AsyncMock(return_value={})
    return redis


@pytest.fixture
def controller(mock_redis) -> JobController:
    return JobController(mock_redis)


class TestJobController:
    @pytest.mark.asyncio
    async def test_blocks_on_failure(self, controller, mock_redis):
        result = _make_result(ValidationStatus.FAILED)
        blocked = await controller.apply_result(result)
        # Should have attempted to block all configured downstream jobs
        assert mock_redis.hset.called
        assert mock_redis.expire.called

    @pytest.mark.asyncio
    async def test_no_block_on_pass(self, controller, mock_redis):
        mock_redis.keys = AsyncMock(return_value=[])
        result = _make_result(ValidationStatus.PASSED)
        blocked = await controller.apply_result(result)
        assert blocked == []
        mock_redis.hset.assert_not_called()

    @pytest.mark.asyncio
    async def test_is_blocked_true(self, controller, mock_redis):
        mock_redis.exists = AsyncMock(return_value=1)
        assert await controller.is_blocked("etl_transform") is True

    @pytest.mark.asyncio
    async def test_is_blocked_false(self, controller, mock_redis):
        mock_redis.exists = AsyncMock(return_value=0)
        assert await controller.is_blocked("etl_transform") is False

    @pytest.mark.asyncio
    async def test_force_unblock_existing(self, controller, mock_redis):
        mock_redis.delete = AsyncMock(return_value=1)
        result = await controller.force_unblock("etl_transform")
        assert result is True

    @pytest.mark.asyncio
    async def test_force_unblock_nonexistent(self, controller, mock_redis):
        mock_redis.delete = AsyncMock(return_value=0)
        result = await controller.force_unblock("ghost_job")
        assert result is False

    @pytest.mark.asyncio
    async def test_list_active_blocks_empty(self, controller, mock_redis):
        mock_redis.keys = AsyncMock(return_value=[])
        blocks = await controller.list_active_blocks()
        assert blocks == []

    @pytest.mark.asyncio
    async def test_list_active_blocks_with_data(self, controller, mock_redis):
        key = b"dq:block:etl_transform"
        mock_redis.keys = AsyncMock(return_value=[key])
        mock_redis.hgetall = AsyncMock(return_value={
            b"job_name": b"etl_transform",
            b"table_name": b"orders",
            b"batch_id": b"batch-1",
            b"status": b"failed",
            b"pass_rate": b"0.5",
            b"failed_checks": b"5",
            b"blocked_at": b"2026-05-03T00:00:00",
            b"result_id": b"res-1",
        })
        blocks = await controller.list_active_blocks()
        assert len(blocks) == 1
        assert blocks[0]["job_name"] == "etl_transform"
        assert blocks[0]["table_name"] == "orders"
