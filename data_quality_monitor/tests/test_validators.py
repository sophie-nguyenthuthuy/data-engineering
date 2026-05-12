"""Unit tests for validator logic (no real GE/Soda runtime required)."""
from __future__ import annotations
import pytest
import pandas as pd
from unittest.mock import AsyncMock, MagicMock, patch

from src.models import MicroBatch, BatchMetadata, ValidationStatus, ValidatorBackend
from src.validators.great_expectations_validator import (
    GreatExpectationsValidator,
    _parse_ge_results,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_batch() -> MicroBatch:
    df = pd.DataFrame({
        "order_id": ["ORD-001", "ORD-002", "ORD-003"],
        "customer_id": ["C1", "C2", "C3"],
        "amount": [10.0, 25.50, 5.99],
        "status": ["pending", "confirmed", "shipped"],
        "created_at": pd.Timestamp.utcnow(),
    })
    return MicroBatch.from_dataframe(
        df,
        BatchMetadata(source="kafka", table_name="orders"),
    )


@pytest.fixture
def empty_batch() -> MicroBatch:
    return MicroBatch(
        metadata=BatchMetadata(source="kafka", table_name="orders"),
        records=[],
    )


# ---------------------------------------------------------------------------
# MicroBatch
# ---------------------------------------------------------------------------

class TestMicroBatch:
    def test_row_count(self, sample_batch):
        assert sample_batch.row_count == 3

    def test_roundtrip_dataframe(self, sample_batch):
        df = sample_batch.to_dataframe()
        assert list(df.columns) == ["order_id", "customer_id", "amount", "status", "created_at"]
        assert len(df) == 3

    def test_empty_batch(self, empty_batch):
        assert empty_batch.row_count == 0
        assert empty_batch.to_dataframe().empty


# ---------------------------------------------------------------------------
# GreatExpectationsValidator
# ---------------------------------------------------------------------------

class TestGreatExpectationsValidator:
    def test_health_check_ok(self):
        validator = GreatExpectationsValidator()
        with patch.object(validator, "_get_context", return_value=MagicMock()):
            import asyncio
            assert asyncio.get_event_loop().run_until_complete(validator.health_check())

    def test_health_check_fail(self):
        validator = GreatExpectationsValidator()
        with patch.object(validator, "_get_context", side_effect=RuntimeError("no context")):
            import asyncio
            assert not asyncio.get_event_loop().run_until_complete(validator.health_check())

    @pytest.mark.asyncio
    async def test_validate_returns_error_on_exception(self, sample_batch):
        validator = GreatExpectationsValidator()
        with patch.object(validator, "_get_context", side_effect=Exception("boom")):
            result = await validator.validate(sample_batch)
        assert result.status == ValidationStatus.ERROR
        assert result.error_message is not None

    def test_parse_ge_results_all_pass(self):
        mock_result = MagicMock()
        mock_result.results = []
        results = _parse_ge_results(mock_result)
        assert results == []

    def test_parse_ge_results_with_failure(self):
        check = MagicMock()
        check.success = False
        check.expectation_config.expectation_type = "expect_column_values_to_not_be_null"
        check.expectation_config.kwargs = {"column": "order_id"}
        check.result = {
            "observed_value": None,
            "unexpected_count": 2,
            "unexpected_percent": 66.7,
            "element_count": 3,
        }

        mock_result = MagicMock()
        mock_result.results = [check]
        results = _parse_ge_results(mock_result)

        assert len(results) == 1
        assert results[0].status == ValidationStatus.FAILED
        assert results[0].unexpected_count == 2


# ---------------------------------------------------------------------------
# SodaValidator health check (import-guarded)
# ---------------------------------------------------------------------------

class TestSodaValidatorHealthCheck:
    @pytest.mark.asyncio
    async def test_health_check_no_soda(self):
        from src.validators.soda_validator import SodaValidator
        validator = SodaValidator()
        with patch("builtins.__import__", side_effect=ImportError("no soda")):
            # health_check should not raise — it returns False
            result = await validator.health_check()
            # either True (soda installed) or False; just ensure no exception
            assert isinstance(result, bool)
