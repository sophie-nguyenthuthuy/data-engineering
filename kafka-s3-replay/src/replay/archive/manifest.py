"""Build a replay manifest — an ordered list of S3 files covering the requested window."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

from replay.models import S3ArchiveConfig, TimeWindow
from replay.archive.s3 import S3ArchiveReader

logger = logging.getLogger(__name__)


class ManifestEntry:
    __slots__ = ("topic", "key", "estimated_events")

    def __init__(self, topic: str, key: str, estimated_events: int = 0) -> None:
        self.topic = topic
        self.key = key
        self.estimated_events = estimated_events

    def to_dict(self) -> dict:
        return {"topic": self.topic, "key": self.key, "estimated_events": self.estimated_events}


async def build_manifest(
    topics: list[str],
    window: TimeWindow,
    archive_config: S3ArchiveConfig,
    output_path: str | None = None,
) -> list[ManifestEntry]:
    """
    Scan S3 for all files in the window and write a manifest JSON.
    Returns the list of ManifestEntry objects.
    """
    reader = S3ArchiveReader(archive_config)
    entries: list[ManifestEntry] = []

    for topic in topics:
        keys = await reader.list_files(topic, window)
        for key in keys:
            entries.append(ManifestEntry(topic=topic, key=key))

    logger.info("Manifest: %d files across %d topics", len(entries), len(topics))

    if output_path:
        manifest_doc = {
            "generated_at": datetime.utcnow().isoformat(),
            "window": {"start": window.start.isoformat(), "end": window.end.isoformat()},
            "topics": topics,
            "files": [e.to_dict() for e in entries],
        }
        Path(output_path).write_text(json.dumps(manifest_doc, indent=2))
        logger.info("Manifest written to %s", output_path)

    return entries
