"""Persistent checkpoint store — allows interrupted replays to resume."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


class CheckpointStore:
    """
    File-based checkpoint store.

    Each job writes a JSON file under {checkpoint_dir}/{job_id}.json.
    The checkpoint records which S3 keys have been fully processed
    and the last successfully replayed event timestamp.
    """

    def __init__(self, checkpoint_dir: str, job_id: str) -> None:
        self.path = Path(checkpoint_dir) / f"{job_id}.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._state: dict = self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_key_done(self, key: str) -> bool:
        return key in self._state.get("completed_keys", [])

    def mark_key_done(self, key: str) -> None:
        self._state.setdefault("completed_keys", [])
        if key not in self._state["completed_keys"]:
            self._state["completed_keys"].append(key)
        self._state["last_updated"] = datetime.now(tz=timezone.utc).isoformat()
        self._save()

    def record_progress(self, replayed: int, failed: int) -> None:
        self._state["replayed_events"] = replayed
        self._state["failed_events"] = failed
        self._state["last_updated"] = datetime.now(tz=timezone.utc).isoformat()
        self._save()

    def get_replayed_count(self) -> int:
        return self._state.get("replayed_events", 0)

    def get_failed_count(self) -> int:
        return self._state.get("failed_events", 0)

    def reset(self) -> None:
        self._state = {}
        self._save()

    def delete(self) -> None:
        if self.path.exists():
            self.path.unlink()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load(self) -> dict:
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text())
                logger.info("Resuming from checkpoint %s (%d keys done)",
                            self.path, len(data.get("completed_keys", [])))
                return data
            except Exception:
                logger.warning("Corrupt checkpoint at %s — starting fresh", self.path)
        return {}

    def _save(self) -> None:
        self.path.write_text(json.dumps(self._state, indent=2))
