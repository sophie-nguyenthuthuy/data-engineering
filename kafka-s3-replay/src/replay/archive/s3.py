"""S3 archive reader — supports JSONL and Avro formats written by Kafka Connect."""

from __future__ import annotations

import gzip
import io
import json
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import anyio
import boto3
import botocore.exceptions
import fastavro

from replay.models import ArchiveFormat, Event, S3ArchiveConfig, TimeWindow

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class S3ArchiveReader:
    """
    Reads events from S3 archives created by Kafka Connect S3 Sink.

    Expected S3 key layouts (both are supported):
      Kafka Connect default: {prefix}/{topic}/{partition:04d}/{topic}+{partition:04d}+{offset:020d}.json(.gz)
      Date-partitioned:      {prefix}/{topic}/year={yyyy}/month={mm}/day={dd}/...
    """

    def __init__(self, config: S3ArchiveConfig) -> None:
        self.config = config
        kwargs: dict = {"region_name": config.region}
        if config.endpoint_url:
            kwargs["endpoint_url"] = config.endpoint_url
        self._s3 = boto3.client("s3", **kwargs)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def list_files(self, topic: str, window: TimeWindow) -> list[str]:
        """Return S3 keys that may contain events in *window* for *topic*."""
        prefix = f"{self.config.prefix}/{topic}/" if self.config.prefix else f"{topic}/"
        keys: list[str] = []

        paginator = self._s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.config.bucket, Prefix=prefix)

        for page in pages:
            for obj in page.get("Contents", []):
                key: str = obj["Key"]
                last_modified: datetime = obj["LastModified"]
                if last_modified.tzinfo is None:
                    last_modified = last_modified.replace(tzinfo=timezone.utc)
                # Pre-filter: skip objects whose last-modified date is before window start
                # (files modified before the window can't contain events inside it)
                if last_modified < window.start:
                    continue
                if self._key_matches_format(key):
                    keys.append(key)

        logger.info("topic=%s found %d candidate files in S3", topic, len(keys))
        return sorted(keys)

    async def read_events(self, key: str, window: TimeWindow) -> AsyncIterator[Event]:
        """Stream events from a single S3 object, filtered to *window*."""
        body = await anyio.to_thread.run_sync(lambda: self._fetch_object(key))
        topic = self._topic_from_key(key)
        partition, offset_start = self._partition_offset_from_key(key)

        async for event in self._parse_body(body, key, topic, partition, offset_start, window):
            yield event

    async def count_events(self, topics: list[str], window: TimeWindow) -> int:
        """Estimate total event count across all topics in the window."""
        total = 0
        for topic in topics:
            keys = await self.list_files(topic, window)
            for key in keys:
                total += await anyio.to_thread.run_sync(lambda k=key: self._count_in_file(k, window))
        return total

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_object(self, key: str) -> bytes:
        try:
            resp = self._s3.get_object(Bucket=self.config.bucket, Key=key)
            return resp["Body"].read()
        except botocore.exceptions.ClientError as exc:
            raise RuntimeError(f"Failed to fetch s3://{self.config.bucket}/{key}: {exc}") from exc

    def _count_in_file(self, key: str, window: TimeWindow) -> int:
        body = self._fetch_object(key)
        topic = self._topic_from_key(key)
        partition, offset_start = self._partition_offset_from_key(key)
        count = 0
        for _ in self._parse_body_sync(body, key, topic, partition, offset_start, window):
            count += 1
        return count

    async def _parse_body(
        self,
        body: bytes,
        key: str,
        topic: str,
        partition: int,
        offset_start: int,
        window: TimeWindow,
    ) -> AsyncIterator[Event]:
        for event in self._parse_body_sync(body, key, topic, partition, offset_start, window):
            yield event

    def _parse_body_sync(
        self,
        body: bytes,
        key: str,
        topic: str,
        partition: int,
        offset_start: int,
        window: TimeWindow,
    ):
        if key.endswith(".gz"):
            body = gzip.decompress(body)

        fmt = self.config.format
        if fmt == ArchiveFormat.JSONL or key.endswith(".json") or key.endswith(".jsonl"):
            yield from self._parse_jsonl(body, key, topic, partition, offset_start, window)
        elif fmt == ArchiveFormat.AVRO or key.endswith(".avro"):
            yield from self._parse_avro(body, key, topic, partition, offset_start, window)
        else:
            logger.warning("Unsupported format for key %s, attempting JSONL parse", key)
            yield from self._parse_jsonl(body, key, topic, partition, offset_start, window)

    def _parse_jsonl(
        self,
        body: bytes,
        key: str,
        topic: str,
        partition: int,
        offset_start: int,
        window: TimeWindow,
    ):
        """Parse Kafka Connect JSON/JSONL format."""
        offset = offset_start
        for line in body.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                logger.debug("Skipping malformed JSON line in %s", key)
                continue

            ts = self._extract_timestamp(record)
            if ts is None or not window.contains(ts):
                offset += 1
                continue

            value_raw = record.get("payload") or record.get("value") or record
            key_raw = record.get("key")

            yield Event(
                topic=topic,
                partition=partition,
                offset=record.get("offset", offset),
                key=key_raw.encode() if isinstance(key_raw, str) else (key_raw or None),
                value=json.dumps(value_raw).encode(),
                timestamp=ts,
                source_path=key,
                headers={
                    k: str(v).encode()
                    for k, v in record.get("headers", {}).items()
                },
            )
            offset += 1

    def _parse_avro(
        self,
        body: bytes,
        key: str,
        topic: str,
        partition: int,
        offset_start: int,
        window: TimeWindow,
    ):
        """Parse Confluent/Kafka Connect Avro files."""
        reader = fastavro.reader(io.BytesIO(body))
        offset = offset_start
        for record in reader:
            ts = self._extract_timestamp(record)
            if ts is None or not window.contains(ts):
                offset += 1
                continue

            yield Event(
                topic=topic,
                partition=partition,
                offset=record.get("offset", offset),
                key=str(record.get("key", "")).encode() or None,
                value=json.dumps(record.get("payload", record)).encode(),
                timestamp=ts,
                source_path=key,
            )
            offset += 1

    @staticmethod
    def _extract_timestamp(record: dict) -> datetime | None:
        for field in ("timestamp", "ts", "event_time", "created_at", "time"):
            raw = record.get(field)
            if raw is None:
                continue
            try:
                if isinstance(raw, (int, float)):
                    # epoch millis
                    if raw > 1e12:
                        raw = raw / 1000
                    return datetime.fromtimestamp(raw, tz=timezone.utc)
                if isinstance(raw, str):
                    from dateutil.parser import parse
                    dt = parse(raw)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                if isinstance(raw, datetime):
                    if raw.tzinfo is None:
                        return raw.replace(tzinfo=timezone.utc)
                    return raw
            except Exception:
                continue
        return None

    def _key_matches_format(self, key: str) -> bool:
        exts = (".json", ".jsonl", ".avro", ".json.gz", ".jsonl.gz", ".avro.gz")
        return any(key.endswith(ext) for ext in exts)

    def _topic_from_key(self, key: str) -> str:
        # s3://bucket/{prefix}/{topic}/{partition}/...
        parts = key.lstrip("/").split("/")
        offset = 1 if self.config.prefix else 0
        return parts[offset] if len(parts) > offset else "unknown"

    def _partition_offset_from_key(self, key: str) -> tuple[int, int]:
        """
        Extract partition and starting offset from Kafka Connect file name.
        Format: {topic}+{partition:04d}+{offset:020d}.ext
        """
        filename = key.split("/")[-1].split(".")[0]
        parts = filename.rsplit("+", 2)
        if len(parts) == 3:
            try:
                return int(parts[1]), int(parts[2])
            except ValueError:
                pass
        return 0, 0
