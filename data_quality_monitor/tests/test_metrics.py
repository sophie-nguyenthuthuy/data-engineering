"""Tests for the MetricsCollector rolling window."""
from __future__ import annotations
import time
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch

from src.models import ValidationResult, ValidationStatus, ValidatorBackend
from src.metrics.collector import MetricsCollector


def _result(status: ValidationStatus, table: str = "orders", pass_rate: float | None = None) -> ValidationResult:
    if pass_rate is None:
        pass_rate = 1.0 if status == ValidationStatus.PASSED else 0.5
    return ValidationResult(
        result_id="r1",
        batch_id="b1",
        table_name=table,
        backend=ValidatorBackend.GREAT_EXPECTATIONS,
        suite_name="suite",
        status=status,
        pass_rate=pass_rate,
        total_checks=10,
        passed_checks=int(pass_rate * 10),
        failed_checks=10 - int(pass_rate * 10),
        warning_checks=0,
        row_count=50,
        duration_ms=30.0,
    )


class TestMetricsCollector:
    def test_empty_summary(self):
        c = MetricsCollector()
        s = c.summary()
        assert s["total"] == 0
        assert s["overall_pass_rate"] == 1.0

    def test_single_pass(self):
        c = MetricsCollector()
        c.record(_result(ValidationStatus.PASSED))
        s = c.summary()
        assert s["total"] == 1
        assert s["failed"] == 0
        assert s["overall_pass_rate"] == 1.0

    def test_single_failure(self):
        c = MetricsCollector()
        c.record(_result(ValidationStatus.FAILED))
        s = c.summary()
        assert s["total"] == 1
        assert s["failed"] == 1
        assert s["overall_pass_rate"] == 0.0

    def test_mixed(self):
        c = MetricsCollector()
        c.record(_result(ValidationStatus.PASSED))
        c.record(_result(ValidationStatus.PASSED))
        c.record(_result(ValidationStatus.FAILED))
        s = c.summary()
        assert s["total"] == 3
        assert s["failed"] == 1
        assert abs(s["overall_pass_rate"] - 2 / 3) < 1e-6

    def test_multiple_tables(self):
        c = MetricsCollector()
        c.record(_result(ValidationStatus.PASSED, table="orders"))
        c.record(_result(ValidationStatus.FAILED, table="customers"))
        s = c.summary()
        assert "orders" in s["per_table"]
        assert "customers" in s["per_table"]
        assert s["per_table"]["orders"]["failed"] == 0
        assert s["per_table"]["customers"]["failed"] == 1

    def test_eviction_of_old_entries(self):
        c = MetricsCollector()
        old_time = datetime.utcnow() - timedelta(hours=2)
        # Manually insert an old entry
        from src.models import ValidationResult
        r = _result(ValidationStatus.FAILED)
        c._window["orders"].append((old_time, r))
        # Record a fresh pass
        c.record(_result(ValidationStatus.PASSED))
        s = c.summary()
        # Old failure should have been evicted
        assert s["failed"] == 0
