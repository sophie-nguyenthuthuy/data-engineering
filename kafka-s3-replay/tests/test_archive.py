"""Tests for S3ArchiveReader."""

from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone

import pytest

from replay.archive.s3 import S3ArchiveReader
from replay.models import ArchiveFormat, S3ArchiveConfig, TimeWindow


@pytest.mark.asyncio
class TestS3ArchiveReader:
    async def test_list_files(self, mock_s3, s3_archive_config, window):
        reader = S3ArchiveReader(s3_archive_config)
        keys = await reader.list_files("orders", window)
        assert len(keys) >= 1
        assert all("orders" in k for k in keys)

    async def test_list_files_unknown_topic_returns_empty(self, mock_s3, s3_archive_config, window):
        reader = S3ArchiveReader(s3_archive_config)
        keys = await reader.list_files("nonexistent-topic", window)
        assert keys == []

    async def test_read_events_returns_correct_count(self, mock_s3, s3_archive_config, window):
        reader = S3ArchiveReader(s3_archive_config)
        keys = await reader.list_files("orders", window)
        assert keys, "Need at least one file to test read_events"

        events = []
        async for event in reader.read_events(keys[0], window):
            events.append(event)

        # Sample has 5 events, 4 of which fall within 2024-03-14 window
        assert len(events) == 4

    async def test_read_events_all_within_window(self, mock_s3, s3_archive_config, window):
        reader = S3ArchiveReader(s3_archive_config)
        keys = await reader.list_files("orders", window)
        async for event in reader.read_events(keys[0], window):
            assert window.contains(event.timestamp), f"Event {event.offset} outside window"

    async def test_read_gzipped_file(self, mock_s3, s3_archive_config, window):
        reader = S3ArchiveReader(s3_archive_config)
        # The gz key is at partition 0001
        gz_key = "topics/orders/0001/orders+0001+00000000000000000001.json.gz"
        events = []
        async for event in reader.read_events(gz_key, window):
            events.append(event)
        assert len(events) == 4  # same sample data

    async def test_event_has_required_fields(self, mock_s3, s3_archive_config, window):
        reader = S3ArchiveReader(s3_archive_config)
        keys = await reader.list_files("orders", window)
        async for event in reader.read_events(keys[0], window):
            assert event.topic == "orders"
            assert event.timestamp.tzinfo is not None
            assert isinstance(event.value, bytes)
            break  # just check first event


class TestExtractTimestamp:
    def test_epoch_millis(self):
        from replay.archive.s3 import S3ArchiveReader
        ts = S3ArchiveReader._extract_timestamp({"timestamp": 1710410400000})
        assert ts is not None
        assert ts.year == 2024

    def test_iso_string(self):
        from replay.archive.s3 import S3ArchiveReader
        ts = S3ArchiveReader._extract_timestamp({"timestamp": "2024-03-14T10:00:00Z"})
        assert ts is not None
        assert ts.month == 3

    def test_fallback_fields(self):
        from replay.archive.s3 import S3ArchiveReader
        ts = S3ArchiveReader._extract_timestamp({"event_time": "2024-03-14T10:00:00Z"})
        assert ts is not None

    def test_none_when_missing(self):
        from replay.archive.s3 import S3ArchiveReader
        ts = S3ArchiveReader._extract_timestamp({"data": "no timestamp here"})
        assert ts is None
