"""Tests for the workload classifier."""

import pytest

from cow_mor_bench.workload.classifier import classify_custom
from cow_mor_bench.workload.patterns import WorkloadClass


def test_olap_classification():
    result = classify_custom(
        write_ratio=0.05,
        update_fraction_of_table=0.01,
        avg_batch_rows=5000,
        full_scan_ratio=0.8,
        point_read_ratio=0.1,
    )
    assert result.predicted_class == WorkloadClass.OLAP_HEAVY


def test_oltp_classification():
    result = classify_custom(
        write_ratio=0.75,
        update_fraction_of_table=0.05,
        avg_batch_rows=50,
        full_scan_ratio=0.05,
        point_read_ratio=0.8,
    )
    assert result.predicted_class == WorkloadClass.OLTP_HEAVY


def test_streaming_classification():
    result = classify_custom(
        write_ratio=0.90,
        update_fraction_of_table=0.01,
        avg_batch_rows=200,
        full_scan_ratio=0.1,
        point_read_ratio=0.05,
    )
    assert result.predicted_class == WorkloadClass.STREAMING_INGEST


def test_batch_update_classification():
    result = classify_custom(
        write_ratio=0.70,
        update_fraction_of_table=0.30,
        avg_batch_rows=50_000,
        full_scan_ratio=0.2,
        point_read_ratio=0.05,
    )
    assert result.predicted_class == WorkloadClass.BATCH_UPDATE


def test_confidence_in_range():
    result = classify_custom(0.5, 0.1, 1000, 0.3, 0.3)
    assert 0.0 < result.confidence <= 1.0


def test_reasoning_non_empty():
    result = classify_custom(0.3, 0.1, 1000, 0.4, 0.3)
    assert len(result.reasoning) > 0
