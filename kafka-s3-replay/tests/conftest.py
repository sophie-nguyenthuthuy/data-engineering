"""Shared pytest fixtures."""

from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import boto3
import pytest
from moto import mock_aws

from replay.models import (
    ArchiveFormat,
    Event,
    S3ArchiveConfig,
    TimeWindow,
)

BUCKET = "test-archive-bucket"
REGION = "us-east-1"

SAMPLE_EVENTS = [
    {
        "topic": "orders",
        "partition": 0,
        "offset": i,
        "timestamp": f"2024-03-14T10:{i:02d}:00Z",
        "key": f"order-{i:03d}",
        "payload": {"order_id": f"order-{i:03d}", "amount": i * 10.0},
    }
    for i in range(1, 6)
]


@pytest.fixture
def window() -> TimeWindow:
    return TimeWindow(
        start=datetime(2024, 3, 14, 0, 0, tzinfo=timezone.utc),
        end=datetime(2024, 3, 15, 0, 0, tzinfo=timezone.utc),
    )


@pytest.fixture
def sample_events() -> list[Event]:
    events = []
    for raw in SAMPLE_EVENTS:
        events.append(
            Event(
                topic=raw["topic"],
                partition=raw["partition"],
                offset=raw["offset"],
                key=raw["key"].encode(),
                value=json.dumps(raw["payload"]).encode(),
                timestamp=datetime.fromisoformat(raw["timestamp"].replace("Z", "+00:00")),
            )
        )
    return events


@pytest.fixture
def s3_archive_config() -> S3ArchiveConfig:
    return S3ArchiveConfig(bucket=BUCKET, prefix="topics", region=REGION, format=ArchiveFormat.JSONL)


@pytest.fixture
def mock_s3(s3_archive_config) -> Generator:
    """Start moto S3 mock, create bucket, and upload sample JSONL."""
    with mock_aws():
        client = boto3.client("s3", region_name=REGION)
        client.create_bucket(Bucket=BUCKET)

        # Upload a sample JSONL file
        body = "\n".join(json.dumps(e) for e in SAMPLE_EVENTS).encode()
        key = "topics/orders/0000/orders+0000+00000000000000000001.json"
        client.put_object(Bucket=BUCKET, Key=key, Body=body)

        # Also upload a gzipped version
        gz_key = "topics/orders/0001/orders+0001+00000000000000000001.json.gz"
        client.put_object(Bucket=BUCKET, Key=gz_key, Body=gzip.compress(body))

        yield client


@pytest.fixture
def fixture_dir() -> Path:
    return Path(__file__).parent / "fixtures"
